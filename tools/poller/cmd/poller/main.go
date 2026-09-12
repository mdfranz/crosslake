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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hamba/avro/v2"

	"github.com/mdfranz/crosslake/tools/poller/internal/avroenc"
	"github.com/mdfranz/crosslake/tools/poller/internal/disksink"
	"github.com/mdfranz/crosslake/tools/poller/internal/ledger"
	"github.com/mdfranz/crosslake/tools/poller/internal/manifest"
	"github.com/mdfranz/crosslake/tools/poller/internal/s3source"
	"github.com/mdfranz/crosslake/tools/poller/internal/telemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// main only ever calls os.Exit, and only with run's result -- run itself
// must never call log.Fatal/os.Exit. Exiting from inside run (as an earlier
// version of this function did, via log.Fatal after telemetry.Init) skips
// every deferred function on that stack, including the shutdown() that
// flushes buffered OTEL spans -- confirmed for real while testing --
// reconcile's error path: the span for a deliberately-triggered "objects
// missing" failure never reached Logfire at all, while the success-path
// span for the same command landed fine. See LEARNINGS.md.
func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.yaml", "path to poller config YAML")
	schemaPath := flag.String("schema", "schema/cloudtrail.avsc", "path to the Avro schema")
	mode := flag.String("mode", "", "sink mode override: local|pubsub (default: from config)")
	once := flag.Bool("once", false, "run a single poll pass and exit")
	s3Prefix := flag.String("s3-prefix", "", "S3 prefix override (default: from config) -- for backfilling an earlier date range without touching the live cursor/ledger")
	cursorFile := flag.String("cursor-file", "", "cursor file override (default: from config), used only by --allow-unsafe-last-key-polling loop mode")
	ledgerFile := flag.String("ledger-file", "", "ledger file override (default: from config) -- pair with -s3-prefix so a backfill doesn't reuse (or grow) the live ledger")
	fetchConcurrency := flag.Int("fetch-concurrency", 0, "concurrent S3 fetch override (default: from config, normally 16) -- set 1 to restore fully-sequential fetching")
	allowUnsafePolling := flag.Bool(
		"allow-unsafe-last-key-polling",
		false,
		"allow loop mode despite the known out-of-order CloudTrail delivery gap (see internal/cursor) -- --once uses the ledger instead and doesn't need this",
	)
	reconcile := flag.Bool(
		"reconcile",
		false,
		"read-only: re-list the configured prefix and report any objects missing from the ledger, without processing them; exits non-zero if any are found",
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// fail logs and returns the exit code run() should propagate to main's
	// os.Exit -- never call log.Fatal*/os.Exit directly below this point,
	// or every defer registered so far (most importantly shutdown, just
	// below) gets skipped. See run's doc comment.
	fail := func(format string, args ...any) int {
		log.Printf(format, args...)
		return 1
	}

	shutdown, err := telemetry.Init(ctx, "crosslake-poller")
	if err != nil {
		// Nothing to flush yet -- telemetry itself failed to initialize --
		// so this one case is safe as a direct return without fail's
		// logging wrapper adding anything.
		log.Printf("telemetry init: %v", err)
		return 1
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
		return fail("%v", err)
	}
	if *mode != "" {
		cfg.Mode = *mode
	}
	if cfg.Mode == "" {
		cfg.Mode = "local"
	}
	if cfg.Mode != "local" {
		return fail("mode %q not implemented yet -- this build is Local Mode only (see PLAN.md)", cfg.Mode)
	}
	// The cursor is only ever consulted by loop mode (--once and
	// --reconcile are both ledger-only), so a backfill combined with
	// either of those doesn't need to override it too.
	needsCursor := !*once && !*reconcile
	if err := applySourceOverrides(&cfg, *s3Prefix, *cursorFile, *ledgerFile, needsCursor); err != nil {
		return fail("%v", err)
	}
	if *fetchConcurrency > 0 {
		cfg.FetchConcurrency = *fetchConcurrency
	}
	if !*once && !*reconcile && !*allowUnsafePolling {
		return fail(
			"loop mode disabled: the last-key cursor can omit out-of-order CloudTrail deliveries; " +
				"use --once (backed by the ledger, safe for a closed prefix) or explicitly acknowledge " +
				"the risk with --allow-unsafe-last-key-polling",
		)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.AWS.Region))
	if err != nil {
		return fail("loading AWS config: %v", err)
	}
	src := s3source.New(s3.NewFromConfig(awsCfg), cfg.AWS.S3Bucket, cfg.AWS.S3Prefix)

	if *reconcile {
		if err := runReconcile(ctx, src, cfg); err != nil {
			return fail("%v", err)
		}
		return 0
	}

	schemaBytes, err := os.ReadFile(*schemaPath)
	if err != nil {
		return fail("reading schema %s: %v", *schemaPath, err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		return fail("parsing schema %s: %v", *schemaPath, err)
	}

	if *once {
		cp, err := newLedgerCheckpointer(cfg.LedgerFile, cfg.AWS.S3Bucket, src)
		if err != nil {
			return fail("loading ledger %s: %v", cfg.LedgerFile, err)
		}
		started := time.Now()
		n, err := runOnce(ctx, src, cp, schema, cfg)
		if err != nil {
			return fail("poll failed: %v", err)
		}
		log.Printf("done: %d record(s) written to %s", n, cfg.Local.DataDir)

		// Write the durable ingest-time manifest tools/compare's
		// manifest.py cross-checks its live query results against -- see
		// internal/manifest's package doc. Built from the ledger's full
		// current state (not just this run's delta), so it reflects the
		// cohort as of right now even when --once was a no-op rerun.
		doc := manifest.Build(cp.Ledger(), cfg.AWS.S3Bucket, cfg.AWS.S3Prefix, schemaBytes, started)
		mPath := manifestPath(cfg.LedgerFile)
		if err := manifest.Save(doc, mPath); err != nil {
			return fail("writing manifest %s: %v", mPath, err)
		}
		log.Printf("manifest: %s (cohort=%s, objects=%d, records=%d)", mPath, doc.CohortID, doc.ObjectCount, doc.TotalRecords)
		return 0
	}

	log.Printf("polling every %ds (Ctrl+C to stop)", cfg.PollIntervalSeconds)
	ticker := time.NewTicker(time.Duration(cfg.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		// A fresh cursorCheckpointer per pass is deliberate and cheap (it's
		// just cursor.Load): the ticker loop's whole reason to exist is
		// picking up where the last pass's Commit left off, and Commit
		// already persisted that via cursor.Save.
		cp, err := newCursorCheckpointer(cfg.CursorFile, src)
		if err != nil {
			return fail("loading cursor %s: %v", cfg.CursorFile, err)
		}
		n, err := runOnce(ctx, src, cp, schema, cfg)
		if err != nil {
			log.Printf("poll failed: %v", err)
		} else if n > 0 {
			log.Printf("wrote %d record(s)", n)
		}
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
		}
	}
}

// manifestPath derives the manifest file's path from the ledger file's, so
// no separate config key is needed: ledger.json -> ledger.manifest.json,
// ledger-2026-09-10.json -> ledger-2026-09-10.manifest.json. Falls back to
// appending the suffix outright if ledgerFile doesn't end in ".json".
func manifestPath(ledgerFile string) string {
	if strings.HasSuffix(ledgerFile, ".json") {
		return strings.TrimSuffix(ledgerFile, ".json") + ".manifest.json"
	}
	return ledgerFile + ".manifest.json"
}

// applySourceOverrides applies a backfill's --s3-prefix override, requiring
// a dedicated --ledger-file every time (both --once and --reconcile are
// ledger-only) and a dedicated --cursor-file only when needsCursor is true
// (loop mode) -- otherwise a backfill would silently reuse, and permanently
// grow, the configured live cursor/ledger.
func applySourceOverrides(cfg *Config, s3Prefix, cursorFile, ledgerFile string, needsCursor bool) error {
	if s3Prefix != "" && ledgerFile == "" {
		return fmt.Errorf("--s3-prefix requires --ledger-file so a backfill cannot reuse (or grow) the configured live ledger")
	}
	if s3Prefix != "" && needsCursor && cursorFile == "" {
		return fmt.Errorf("--s3-prefix with loop mode requires --cursor-file so a backfill cannot reuse (or move) the configured live cursor")
	}
	if s3Prefix != "" {
		cfg.AWS.S3Prefix = s3Prefix
	}
	if cursorFile != "" {
		cfg.CursorFile = cursorFile
	}
	if ledgerFile != "" {
		cfg.LedgerFile = ledgerFile
	}
	return nil
}

// runReconcile re-lists the configured prefix and reports any object S3 has
// that the ledger doesn't -- see internal/ledger.Missing's doc comment for
// what that gap means (unprocessed work, or a late delivery that arrived
// after an earlier run already advanced past it). Read-only: it never
// fetches object bodies or touches the sink or the ledger file.
//
// Named return so the deferred status-setting below covers every return
// path, matching runOnce's "poll" span -- see its doc comment for why a
// categorized status string, not err.Error(), is exported (wrapped errors
// here can carry an S3 key).
func runReconcile(ctx context.Context, src *s3source.Source, cfg Config) (err error) {
	tracer := otel.Tracer("crosslake-poller")
	ctx, span := tracer.Start(ctx, "reconcile")
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "reconcile_failed")
		}
	}()
	started := time.Now()
	defer func() {
		span.SetAttributes(attribute.Int64("reconcile.duration_ms", time.Since(started).Milliseconds()))
	}()

	l, err := ledger.Load(cfg.LedgerFile)
	if err != nil {
		span.SetStatus(codes.Error, "ledger_load_failed")
		return fmt.Errorf("reconcile: loading ledger %s: %w", cfg.LedgerFile, err)
	}
	objects, err := src.List(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "list_failed")
		return fmt.Errorf("reconcile: listing: %w", err)
	}
	span.SetAttributes(attribute.Int("s3.objects_found", len(objects)))

	keyETags := make([]ledger.KeyETag, len(objects))
	for i, o := range objects {
		keyETags[i] = ledger.KeyETag{Key: o.Key, ETag: o.ETag}
	}
	missing := l.Missing(cfg.AWS.S3Bucket, keyETags)
	span.SetAttributes(attribute.Int("reconcile.missing_count", len(missing)))

	if len(missing) == 0 {
		log.Printf("reconcile: %d object(s) in S3, all present in ledger %s", len(objects), cfg.LedgerFile)
		return nil
	}
	log.Printf("reconcile: %d of %d object(s) in S3 are missing from ledger %s", len(missing), len(objects), cfg.LedgerFile)
	for _, m := range missing {
		log.Printf("  missing: %s (etag=%s)", m.Key, m.ETag)
	}
	span.SetStatus(codes.Error, "objects_missing")
	return fmt.Errorf("reconcile: %d object(s) missing from the ledger -- rerun --once to process them", len(missing))
}

// objectFetcher is the subset of *s3source.Source runOnce needs to fetch
// object bodies, so tests can supply a fake without a real S3 client.
type objectFetcher interface {
	FetchAndGunzip(ctx context.Context, key string) ([]byte, error)
}

// runOnce is the poll->fetch->parse->write->checkpoint pass described in
// PLAN.md. It becomes the seam for a future Cloud Run Job/Lambda handler.
// It doesn't know or care whether cp is ledger- or cursor-backed -- see
// checkpoint.go's checkpointer doc comment.
//
// Named returns so the deferred status-setting below covers every return
// path automatically: previously only the ListSince failure marked the
// outer "poll" span as an error (an explicit SetStatus at that one call
// site) -- every other failure (fetch, decode, parse, write, flush,
// checkpoint) returned straight past it, leaving "poll" showing as OK in
// Logfire even when the run failed. A generic "poll_failed" status (not
// err.Error()) is deliberate: wrapped errors can carry an S3 key, which
// contains the real account ID -- see docs/observability.md's data
// minimization rule. The specific failure reason is still on whichever
// child span (fetch_object/process_object) set its own categorized status.
func runOnce(ctx context.Context, src objectFetcher, cp checkpointer, schema avro.Schema, cfg Config) (total int, err error) {
	tracer := otel.Tracer("crosslake-poller")
	ctx, span := tracer.Start(ctx, "poll")
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "poll_failed")
		}
	}()
	started := time.Now()
	defer func() {
		span.SetAttributes(attribute.Int64("poll.duration_ms", time.Since(started).Milliseconds()))
	}()

	pending, err := cp.Pending(ctx)
	if err != nil {
		span.SetStatus(codes.Error, "list_failed")
		return 0, err
	}
	span.SetAttributes(attribute.Int("s3.objects_found", len(pending)))
	if len(pending) == 0 {
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
	// and the ledger/cursor checkpoint's meaning depends on knowing exactly
	// which objects that flush covered.
	total = 0 // total is a named return now, so this reassigns rather than shadows
	for chunkStart := 0; chunkStart < len(pending); chunkStart += cfg.CheckpointEveryObjects {
		chunkEnd := chunkStart + cfg.CheckpointEveryObjects
		if chunkEnd > len(pending) {
			chunkEnd = len(pending)
		}
		chunk := pending[chunkStart:chunkEnd]

		bodies, fetchErrs := fetchChunkConcurrently(ctx, src, tracer, chunk, chunkStart, cfg.FetchConcurrency)
		for i, err := range fetchErrs {
			if err != nil {
				return total, fmt.Errorf("fetching object %d (%s): %w", chunkStart+i, chunk[i].Key, err)
			}
		}

		committed := make([]committedObject, len(chunk))
		for i, obj := range chunk {
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
				return total, fmt.Errorf("unmarshal %s: %w", obj.Key, err)
			}

			for _, raw := range blob.Records {
				rec, err := avroenc.FromJSON(raw)
				if err != nil {
					objSpan.SetStatus(codes.Error, "record_parse_failed")
					objSpan.End()
					return total, fmt.Errorf("parsing record from %s: %w", obj.Key, err)
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
			committed[i] = committedObject{pendingObject: obj, RecordsWritten: len(blob.Records)}
		}

		// OCF writes are buffered, and Flush forces a new compression
		// block. Flushing/checkpointing after every single object was
		// measured to produce one ~2.4-record block per object and
		// roughly double the Avro file size on a real batch (see
		// LEARNINGS.md). Batching every CheckpointEveryObjects objects
		// (default 100) restores healthy block sizes while keeping the
		// same durability property: the checkpoint still never advances
		// past data that hasn't been flushed. A crash mid-batch just means
		// the next run re-fetches and re-processes that batch's objects --
		// the same at-least-once/idempotent-replay model as before, just
		// over a larger, tunable window instead of a hidden per-object one.
		if err := sink.Flush(); err != nil {
			return total, fmt.Errorf("flush after chunk ending %s: %w", chunk[len(chunk)-1].Key, err)
		}
		if err := cp.Commit(committed); err != nil {
			return total, fmt.Errorf("checkpoint after chunk ending %s: %w", chunk[len(chunk)-1].Key, err)
		}
	}

	span.SetAttributes(attribute.Int("records.written", total))
	return total, nil
}

// fetchChunkConcurrently fetches and gunzips a chunk of S3 objects with up
// to concurrency requests in flight at once. Results are returned in the
// same order as chunk (bodies[i] / errs[i] correspond to chunk[i])
// regardless of completion order, since the caller must process and
// checkpoint them in listing order.
func fetchChunkConcurrently(
	ctx context.Context, src objectFetcher, tracer trace.Tracer, chunk []pendingObject, startIndex, concurrency int,
) (bodies [][]byte, errs []error) {
	bodies = make([][]byte, len(chunk))
	errs = make([]error, len(chunk))

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, obj := range chunk {
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
		}(i, obj.Key)
	}
	wg.Wait()
	return bodies, errs
}
