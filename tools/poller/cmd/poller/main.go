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
	"sync"
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
	s3Prefix := flag.String("s3-prefix", "", "S3 prefix override (default: from config) -- for backfilling an earlier date range without touching the live cursor")
	cursorFile := flag.String("cursor-file", "", "cursor file override (default: from config) -- pair with -s3-prefix so a backfill run doesn't reuse (or clobber) the live tailing cursor")
	fetchConcurrency := flag.Int("fetch-concurrency", 0, "concurrent S3 fetch override (default: from config, normally 16) -- set 1 to restore fully-sequential fetching")
	allowUnsafePolling := flag.Bool(
		"allow-unsafe-last-key-polling",
		false,
		"allow loop mode despite the known out-of-order CloudTrail delivery gap",
	)
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
	if err := applySourceOverrides(&cfg, *s3Prefix, *cursorFile); err != nil {
		log.Fatal(err)
	}
	if *fetchConcurrency > 0 {
		cfg.FetchConcurrency = *fetchConcurrency
	}
	if !*once && !*allowUnsafePolling {
		log.Fatal(
			"loop mode disabled: the last-key cursor can omit out-of-order CloudTrail deliveries; " +
				"use --once with a closed date prefix or explicitly acknowledge the risk with " +
				"--allow-unsafe-last-key-polling",
		)
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

func applySourceOverrides(cfg *Config, s3Prefix, cursorFile string) error {
	if s3Prefix != "" && cursorFile == "" {
		return fmt.Errorf("--s3-prefix requires --cursor-file so a backfill cannot reuse the configured live cursor")
	}
	if s3Prefix != "" {
		cfg.AWS.S3Prefix = s3Prefix
	}
	if cursorFile != "" {
		cfg.CursorFile = cursorFile
	}
	return nil
}

// runOnce is the poll->fetch->parse->write->cursor-update pass described in
// PLAN.md. It becomes the seam for a future Cloud Run Job/Lambda handler.
func runOnce(ctx context.Context, src *s3source.Source, schema avro.Schema, cfg Config) (int, error) {
	tracer := otel.Tracer("crosslake-poller")
	ctx, span := tracer.Start(ctx, "poll")
	defer span.End()
	started := time.Now()
	defer func() {
		span.SetAttributes(attribute.Int64("poll.duration_ms", time.Since(started).Milliseconds()))
	}()

	cur, err := cursor.Load(cfg.CursorFile)
	if err != nil {
		return 0, err
	}

	keys, err := src.ListSince(ctx, cur.LastKey)
	if err != nil {
		span.SetStatus(codes.Error, "list_failed")
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

	// Chunk by CheckpointEveryObjects: within each chunk, fetch+gunzip runs
	// concurrently (bounded by FetchConcurrency) since it's I/O-bound and
	// dominates wall-clock time by ~1000x over local parse+write (see
	// LEARNINGS.md); parsing and writing that chunk's results then runs
	// strictly sequentially, in listing order, before the chunk's single
	// flush+checkpoint -- disksink.Sink isn't safe for concurrent writes,
	// and the cursor's meaning depends on listing order regardless of
	// which fetch happened to finish first over the network.
	total := 0
	for chunkStart := 0; chunkStart < len(keys); chunkStart += cfg.CheckpointEveryObjects {
		chunkEnd := chunkStart + cfg.CheckpointEveryObjects
		if chunkEnd > len(keys) {
			chunkEnd = len(keys)
		}
		chunk := keys[chunkStart:chunkEnd]

		bodies, fetchErrs := fetchChunkConcurrently(ctx, src, tracer, chunk, chunkStart, cfg.FetchConcurrency)
		for i, err := range fetchErrs {
			if err != nil {
				return total, fmt.Errorf("fetching object %d (%s): %w", chunkStart+i, chunk[i], err)
			}
		}

		var lastKeyInChunk string
		for i, key := range chunk {
			objectIndex := chunkStart + i
			_, objSpan := tracer.Start(ctx, "process_object")
			objSpan.SetAttributes(attribute.Int("object.index", objectIndex))
			objectStarted := time.Now()

			var blob struct {
				Records []json.RawMessage `json:"Records"`
			}
			if err := json.Unmarshal(bodies[i], &blob); err != nil {
				objSpan.SetStatus(codes.Error, "decode_failed")
				objSpan.End()
				return total, fmt.Errorf("unmarshal %s: %w", key, err)
			}

			for _, raw := range blob.Records {
				rec, err := avroenc.FromJSON(raw)
				if err != nil {
					objSpan.SetStatus(codes.Error, "record_parse_failed")
					objSpan.End()
					return total, fmt.Errorf("parsing record from %s: %w", key, err)
				}
				if err := sink.WriteRecord(raw, rec); err != nil {
					objSpan.SetStatus(codes.Error, "record_write_failed")
					objSpan.End()
					return total, err
				}
				total++
			}

			objSpan.SetAttributes(
				attribute.Int("object.records_written", len(blob.Records)),
				attribute.Int64("object.duration_ms", time.Since(objectStarted).Milliseconds()),
			)
			objSpan.End()
			lastKeyInChunk = key
		}

		// OCF writes are buffered, and Flush forces a new compression
		// block. Flushing/checkpointing after every single object was
		// measured to produce one ~2.4-record block per object and
		// roughly double the Avro file size on a real batch (see
		// LEARNINGS.md). Batching every CheckpointEveryObjects objects
		// (default 100) restores healthy block sizes while keeping the
		// same durability property: the cursor still never advances past
		// data that hasn't been flushed. A crash mid-batch just means the
		// next run re-fetches and re-appends that batch's objects -- the
		// same at-least-once/idempotent-replay model as before, just over
		// a larger, tunable window instead of a hidden per-object one.
		if err := sink.Flush(); err != nil {
			return total, fmt.Errorf("flush after chunk ending %s: %w", lastKeyInChunk, err)
		}
		cur.LastKey = lastKeyInChunk
		if err := cursor.Save(cfg.CursorFile, cur); err != nil {
			return total, fmt.Errorf("checkpoint after chunk ending %s: %w", lastKeyInChunk, err)
		}
	}

	span.SetAttributes(attribute.Int("records.written", total))
	return total, nil
}

// fetchChunkConcurrently fetches and gunzips a chunk of S3 objects with up
// to concurrency requests in flight at once. Results are returned in the
// same order as keys (bodies[i] / errs[i] correspond to keys[i]) regardless
// of completion order, since the caller must process and checkpoint them in
// listing order.
func fetchChunkConcurrently(
	ctx context.Context, src *s3source.Source, tracer trace.Tracer, keys []string, startIndex, concurrency int,
) (bodies [][]byte, errs []error) {
	bodies = make([][]byte, len(keys))
	errs = make([]error, len(keys))

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, key string) {
			defer wg.Done()
			defer func() { <-sem }()

			objCtx, objSpan := tracer.Start(ctx, "fetch_object")
			objSpan.SetAttributes(attribute.Int("object.index", startIndex+i))
			body, err := src.FetchAndGunzip(objCtx, key)
			if err != nil {
				objSpan.SetStatus(codes.Error, "fetch_failed")
				errs[i] = err
			} else {
				objSpan.SetAttributes(attribute.Int("object.uncompressed_bytes", len(body)))
				bodies[i] = body
			}
			objSpan.End()
		}(i, key)
	}
	wg.Wait()
	return bodies, errs
}
