package disksink

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/mdfranz/crosslake/tools/poller/internal/avroenc"
)

func TestFlushPersistsPrivateOutputs(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	schemaBytes, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "schema", "cloudtrail.avsc"))
	if err != nil {
		t.Fatalf("ReadFile(schema): %v", err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		t.Fatalf("Parse(schema): %v", err)
	}

	raw := []byte(`{"eventVersion":"1.11","eventTime":"2026-09-12T00:00:00Z","eventID":"00000000-0000-4000-8000-abcdefabcdef"}`)
	record, err := avroenc.FromJSON(raw)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}

	dataDir := t.TempDir()
	avroDir := filepath.Join(dataDir, "tier3-avro")
	if err := os.MkdirAll(avroDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Verify New also repairs permissions on outputs from earlier versions.
	for _, path := range []string{
		filepath.Join(dataDir, "raw.jsonl"),
		filepath.Join(avroDir, "events.avro"),
	} {
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
	sink, err := New(dataDir, schema)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			sink.Close()
		}
	})

	if err := sink.WriteRecord(raw, record); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for _, path := range []string{
		filepath.Join(dataDir, "raw.jsonl"),
		filepath.Join(dataDir, "tier3-avro", "events.avro"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty after Flush", path)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s permissions = %o, want 600", path, got)
		}
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
}
