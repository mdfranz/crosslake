package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/mdfranz/crosslake/tools/poller/internal/ledger"
	"github.com/mdfranz/crosslake/tools/poller/internal/s3source"
)

func TestApplySourceOverridesAlwaysRequiresDedicatedBackfillLedger(t *testing.T) {
	for _, needsCursor := range []bool{true, false} {
		var cfg Config
		cfg.AWS.S3Prefix = "configured-prefix"
		cfg.LedgerFile = "configured-ledger.json"

		if err := applySourceOverrides(&cfg, "closed-day-prefix", "cursor-backfill.json", "", needsCursor); err == nil {
			t.Fatalf("needsCursor=%v: prefix override without a ledger override was accepted", needsCursor)
		}
		if cfg.AWS.S3Prefix != "configured-prefix" || cfg.LedgerFile != "configured-ledger.json" {
			t.Fatalf("needsCursor=%v: invalid override mutated config: %#v", needsCursor, cfg)
		}
	}
}

func TestApplySourceOverridesRequiresCursorOnlyWhenNeeded(t *testing.T) {
	var cfg Config
	cfg.AWS.S3Prefix = "configured-prefix"
	cfg.CursorFile = "configured-cursor.json"

	if err := applySourceOverrides(&cfg, "closed-day-prefix", "", "ledger-backfill.json", true); err == nil {
		t.Fatal("needsCursor=true: prefix override without a cursor override was accepted")
	}
	if cfg.CursorFile != "configured-cursor.json" {
		t.Fatalf("invalid override mutated config: %#v", cfg)
	}

	// needsCursor=false (the --once / --reconcile paths, both ledger-only)
	// must NOT require a --cursor-file override, since neither ever reads
	// the cursor.
	if err := applySourceOverrides(&cfg, "closed-day-prefix", "", "ledger-backfill.json", false); err != nil {
		t.Fatalf("needsCursor=false: applySourceOverrides: %v", err)
	}
	if cfg.CursorFile != "configured-cursor.json" {
		t.Fatalf("cursor should be left untouched when not overridden: %#v", cfg)
	}
}

func TestApplySourceOverridesAppliesBackfillTriple(t *testing.T) {
	var cfg Config
	if err := applySourceOverrides(&cfg, "closed-day-prefix", "cursor-backfill.json", "ledger-backfill.json", true); err != nil {
		t.Fatalf("applySourceOverrides: %v", err)
	}
	if cfg.AWS.S3Prefix != "closed-day-prefix" {
		t.Fatalf("prefix = %q", cfg.AWS.S3Prefix)
	}
	if cfg.CursorFile != "cursor-backfill.json" {
		t.Fatalf("cursor = %q", cfg.CursorFile)
	}
	if cfg.LedgerFile != "ledger-backfill.json" {
		t.Fatalf("ledger = %q", cfg.LedgerFile)
	}
}

// fakeLister is a canned s3source.Source.List, so tests don't need a real
// S3 client to exercise runOnce's ledger path.
type fakeLister struct {
	objects []s3source.Object
}

func (f *fakeLister) List(context.Context) ([]s3source.Object, error) {
	return f.objects, nil
}

// fakeFetcher serves canned object bodies and can be told to fail specific
// keys, simulating a crash/error partway through a chunk.
type fakeFetcher struct {
	bodies map[string][]byte
	fail   map[string]bool
}

func (f *fakeFetcher) FetchAndGunzip(_ context.Context, key string) ([]byte, error) {
	if f.fail[key] {
		return nil, fmt.Errorf("simulated fetch failure for %s", key)
	}
	b, ok := f.bodies[key]
	if !ok {
		return nil, fmt.Errorf("fakeFetcher: no body configured for %s", key)
	}
	return b, nil
}

func recordBody(eventID string) []byte {
	return []byte(fmt.Sprintf(
		`{"Records":[{"eventVersion":"1.11","eventTime":"2026-09-12T00:00:00Z","eventID":%q}]}`,
		eventID,
	))
}

func testSchema(t *testing.T) avro.Schema {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	schemaBytes, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "schema", "cloudtrail.avsc"))
	if err != nil {
		t.Fatalf("ReadFile(schema): %v", err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		t.Fatalf("Parse(schema): %v", err)
	}
	return schema
}

// TestRunOnceLedgerCrashRetryReprocessesOnlyUnflushedChunk is the crash/retry
// test docs/review-telemetry-plan.md's Phase 0 exit criteria calls for:
// "rerunning or crashing at every checkpoint produces no missing records."
// It fails one object mid-second-chunk on the first run (simulating a crash
// after the first chunk committed but before the second could), then
// re-runs with the failure fixed, and checks the union of both runs' work
// covers every object exactly once -- no gap from the failed chunk, no
// duplicate from the chunk that had already committed.
func TestRunOnceLedgerCrashRetryReprocessesOnlyUnflushedChunk(t *testing.T) {
	const bucket = "test-bucket"
	objects := []s3source.Object{
		{Key: "obj1", ETag: "etag1", Size: 10},
		{Key: "obj2", ETag: "etag2", Size: 10},
		{Key: "obj3", ETag: "etag3", Size: 10},
		{Key: "obj4", ETag: "etag4", Size: 10},
	}
	bodies := map[string][]byte{
		"obj1": recordBody("00000000-0000-4000-8000-000000000001"),
		"obj2": recordBody("00000000-0000-4000-8000-000000000002"),
		"obj3": recordBody("00000000-0000-4000-8000-000000000003"),
		"obj4": recordBody("00000000-0000-4000-8000-000000000004"),
	}

	schema := testSchema(t)
	cfg := Config{
		CheckpointEveryObjects: 2, // chunks: [obj1,obj2], [obj3,obj4]
		FetchConcurrency:       2,
	}
	cfg.Local.DataDir = t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.json")

	// Run 1: obj3 fails to fetch, so the second chunk never flushes/commits.
	lister := &fakeLister{objects: objects}
	fetcher := &fakeFetcher{bodies: bodies, fail: map[string]bool{"obj3": true}}
	cp, err := newLedgerCheckpointer(ledgerPath, bucket, lister)
	if err != nil {
		t.Fatalf("newLedgerCheckpointer: %v", err)
	}
	n1, err := runOnce(context.Background(), fetcher, cp, schema, cfg)
	if err == nil {
		t.Fatal("run 1: expected the simulated fetch failure to surface as an error")
	}
	// obj1 and obj2 (one record each) made up the first chunk and committed
	// before the second chunk's fetch failure; obj3/obj4 wrote nothing.
	if n1 != 2 {
		t.Fatalf("run 1: wrote %d record(s), want 2 (obj1+obj2's committed chunk)", n1)
	}

	reloaded, err := ledger.Load(ledgerPath)
	if err != nil {
		t.Fatalf("ledger.Load after run 1: %v", err)
	}
	if !reloaded.Seen(bucket, "obj1", "etag1") || !reloaded.Seen(bucket, "obj2", "etag2") {
		t.Fatal("run 1: committed chunk (obj1, obj2) missing from the persisted ledger")
	}
	if reloaded.Seen(bucket, "obj3", "etag3") || reloaded.Seen(bucket, "obj4", "etag4") {
		t.Fatal("run 1: uncommitted chunk (obj3, obj4) was recorded despite the fetch failure")
	}

	// Run 2: same prefix, failure fixed. A fresh checkpointer reloads the
	// ledger from disk, exactly like a real restarted process would.
	fetcher2 := &fakeFetcher{bodies: bodies} // no keys fail this time
	cp2, err := newLedgerCheckpointer(ledgerPath, bucket, lister)
	if err != nil {
		t.Fatalf("newLedgerCheckpointer (run 2): %v", err)
	}
	pending, err := cp2.Pending(context.Background())
	if err != nil {
		t.Fatalf("Pending (run 2): %v", err)
	}
	if len(pending) != 2 || pending[0].Key != "obj3" || pending[1].Key != "obj4" {
		t.Fatalf("Pending (run 2) = %v, want exactly [obj3, obj4] -- the already-committed chunk must not be reprocessed", pending)
	}

	n2, err := runOnce(context.Background(), fetcher2, cp2, schema, cfg)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if n2 != 2 {
		t.Fatalf("run 2: wrote %d record(s), want 2 (obj3+obj4)", n2)
	}

	final, err := ledger.Load(ledgerPath)
	if err != nil {
		t.Fatalf("ledger.Load after run 2: %v", err)
	}
	if final.Len() != 4 {
		t.Fatalf("final ledger has %d entries, want 4 (no gap, no duplicate)", final.Len())
	}

	// The real completeness check: every object landed in raw.jsonl exactly
	// once across both runs combined.
	raw, err := os.ReadFile(filepath.Join(cfg.Local.DataDir, "raw.jsonl"))
	if err != nil {
		t.Fatalf("reading raw.jsonl: %v", err)
	}
	if got, want := countLines(raw), 4; got != want {
		t.Fatalf("raw.jsonl has %d line(s) across both runs, want %d (one per object, no gap or duplicate)", got, want)
	}
}

func countLines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
