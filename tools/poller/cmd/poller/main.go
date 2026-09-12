// Command poller polls AWS S3 CloudTrail logs and, in Local Mode, writes
// them to ./data/raw.jsonl and ./data/tier3-avro/events.avro. Cloud Mode
// (--mode=pubsub, publishing to GCP Pub/Sub) is designed in PLAN.md but not
// yet implemented -- this build is scoped to Local Mode only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hamba/avro/v2"

	"github.com/mdfranz/crosslake/tools/poller/internal/avroenc"
	"github.com/mdfranz/crosslake/tools/poller/internal/cursor"
	"github.com/mdfranz/crosslake/tools/poller/internal/disksink"
	"github.com/mdfranz/crosslake/tools/poller/internal/s3source"
	"github.com/mdfranz/crosslake/tools/poller/internal/telemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to poller config YAML")
	schemaPath := flag.String("schema", "schema/cloudtrail.avsc", "path to the Avro schema")
	mode := flag.String("mode", "", "sink mode override: local|pubsub (default: from config)")
	once := flag.Bool("once", false, "run a single poll pass and exit")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := telemetry.Init(ctx, "crosslake-poller")
	if err != nil {
		log.Fatalf("telemetry init: %v", err)
	}
	defer func() {
		// Use a fresh context: ctx may already be canceled (Ctrl+C) by the
		// time we get here, and Shutdown needs to actually flush.
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(sctx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if *mode != "" {
		cfg.Mode = *mode
	}
	if cfg.Mode == "" {
		cfg.Mode = "local"
	}
	if cfg.Mode != "local" {
		log.Fatalf("mode %q not implemented yet -- this build is Local Mode only (see PLAN.md)", cfg.Mode)
	}

	schemaBytes, err := os.ReadFile(*schemaPath)
	if err != nil {
		log.Fatalf("reading schema %s: %v", *schemaPath, err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		log.Fatalf("parsing schema %s: %v", *schemaPath, err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.AWS.Region))
	if err != nil {
		log.Fatalf("loading AWS config: %v", err)
	}
	src := s3source.New(s3.NewFromConfig(awsCfg), cfg.AWS.S3Bucket, cfg.AWS.S3Prefix)

	if *once {
		n, err := runOnce(ctx, src, schema, cfg)
		if err != nil {
			log.Fatalf("poll failed: %v", err)
		}
		log.Printf("done: %d record(s) written to %s", n, cfg.Local.DataDir)
		return
	}

	log.Printf("polling every %ds (Ctrl+C to stop)", cfg.PollIntervalSeconds)
	ticker := time.NewTicker(time.Duration(cfg.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		n, err := runOnce(ctx, src, schema, cfg)
		if err != nil {
			log.Printf("poll failed: %v", err)
		} else if n > 0 {
			log.Printf("wrote %d record(s)", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runOnce is the poll->fetch->parse->write->cursor-update pass described in
// PLAN.md. It becomes the seam for a future Cloud Run Job/Lambda handler.
func runOnce(ctx context.Context, src *s3source.Source, schema avro.Schema, cfg Config) (int, error) {
	tracer := otel.Tracer("crosslake-poller")
	ctx, span := tracer.Start(ctx, "RunOnce")
	defer span.End()

	cur, err := cursor.Load(cfg.CursorFile)
	if err != nil {
		return 0, err
	}

	keys, err := src.ListSince(ctx, cur.LastKey)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	span.SetAttributes(attribute.Int("s3.objects_found", len(keys)))
	if len(keys) == 0 {
		return 0, nil
	}

	sink, err := disksink.New(cfg.Local.DataDir, schema)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := sink.Close(); err != nil {
			log.Printf("closing sink: %v", err)
		}
	}()

	total := 0
	for _, key := range keys {
		objCtx, objSpan := tracer.Start(ctx, "fetch_object", trace.WithAttributes(attribute.String("s3.key", key)))
		body, err := src.FetchAndGunzip(objCtx, key)
		if err != nil {
			objSpan.SetStatus(codes.Error, err.Error())
			objSpan.End()
			return total, err
		}
		objSpan.End()

		var blob struct {
			Records []json.RawMessage `json:"Records"`
		}
		if err := json.Unmarshal(body, &blob); err != nil {
			return total, fmt.Errorf("unmarshal %s: %w", key, err)
		}

		for _, raw := range blob.Records {
			_, recSpan := tracer.Start(ctx, "write_record", trace.WithAttributes(
				attribute.String("mode", "local"),
				attribute.String("sink", "disk"),
			))
			rec, err := avroenc.FromJSON(raw)
			if err != nil {
				recSpan.SetStatus(codes.Error, err.Error())
				recSpan.End()
				return total, fmt.Errorf("parsing record from %s: %w", key, err)
			}
			if err := sink.WriteRecord(raw, rec); err != nil {
				recSpan.SetStatus(codes.Error, err.Error())
				recSpan.End()
				return total, err
			}
			recSpan.End()
			total++
		}

		cur.LastKey = key
		if err := cursor.Save(cfg.CursorFile, cur); err != nil {
			return total, err
		}
	}

	span.SetAttributes(attribute.Int("records.written", total))
	return total, nil
}
