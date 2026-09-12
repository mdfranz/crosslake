package manifest

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mdfranz/crosslake/tools/poller/internal/ledger"
)

func TestCohortIDStableForSameBucketPrefix(t *testing.T) {
	a := CohortID("bucket", "prefix/2026/09/10/")
	b := CohortID("bucket", "prefix/2026/09/10/")
	if a != b {
		t.Fatalf("CohortID not stable: %q vs %q", a, b)
	}
}

func TestCohortIDDiffersAcrossPrefix(t *testing.T) {
	a := CohortID("bucket", "prefix/2026/09/10/")
	b := CohortID("bucket", "prefix/2026/09/11/")
	if a == b {
		t.Fatal("CohortID identical for two different prefixes")
	}
}

func TestBuildAggregatesLedgerEntries(t *testing.T) {
	l := ledger.New()
	l.Record("b", "obj1", "etag1", 100, 5)
	l.Record("b", "obj2", "etag2", 200, 7)
	l.Record("other-bucket", "obj3", "etag3", 999, 999) // must not leak into b's totals

	started := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	doc := Build(l, "b", "prefix/", []byte(`{"fake":"schema"}`), started)

	if doc.ObjectCount != 2 {
		t.Fatalf("ObjectCount = %d, want 2", doc.ObjectCount)
	}
	if doc.TotalBytes != 300 {
		t.Fatalf("TotalBytes = %d, want 300", doc.TotalBytes)
	}
	if doc.TotalRecords != 12 {
		t.Fatalf("TotalRecords = %d, want 12", doc.TotalRecords)
	}
	if doc.CohortID != CohortID("b", "prefix/") {
		t.Fatalf("CohortID = %q, want %q", doc.CohortID, CohortID("b", "prefix/"))
	}
	if doc.SchemaVersion == "" {
		t.Fatal("SchemaVersion is empty")
	}
	if doc.ObjectsFingerprint == "" {
		t.Fatal("ObjectsFingerprint is empty")
	}
	if !doc.StartedAt.Equal(started) {
		t.Fatalf("StartedAt = %v, want %v", doc.StartedAt, started)
	}
	if doc.EndedAt.Before(doc.StartedAt) {
		t.Fatalf("EndedAt %v is before StartedAt %v", doc.EndedAt, doc.StartedAt)
	}
}

func TestObjectsFingerprintIsOrderIndependent(t *testing.T) {
	started := time.Now()
	schema := []byte(`{"fake":"schema"}`)

	l1 := ledger.New()
	l1.Record("b", "obj1", "etag1", 1, 1)
	l1.Record("b", "obj2", "etag2", 1, 1)

	l2 := ledger.New()
	l2.Record("b", "obj2", "etag2", 1, 1) // recorded in the opposite order
	l2.Record("b", "obj1", "etag1", 1, 1)

	doc1 := Build(l1, "b", "prefix/", schema, started)
	doc2 := Build(l2, "b", "prefix/", schema, started)

	if doc1.ObjectsFingerprint != doc2.ObjectsFingerprint {
		t.Fatalf("fingerprint depends on commit order: %q vs %q", doc1.ObjectsFingerprint, doc2.ObjectsFingerprint)
	}
}

func TestObjectsFingerprintChangesWithObjectSet(t *testing.T) {
	started := time.Now()
	schema := []byte(`{"fake":"schema"}`)

	l1 := ledger.New()
	l1.Record("b", "obj1", "etag1", 1, 1)

	l2 := ledger.New()
	l2.Record("b", "obj1", "etag1", 1, 1)
	l2.Record("b", "obj2", "etag2", 1, 1)

	doc1 := Build(l1, "b", "prefix/", schema, started)
	doc2 := Build(l2, "b", "prefix/", schema, started)

	if doc1.ObjectsFingerprint == doc2.ObjectsFingerprint {
		t.Fatal("fingerprint identical for two different object sets")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	l := ledger.New()
	l.Record("b", "obj1", "etag1", 100, 5)
	doc := Build(l, "b", "prefix/", []byte(`{"fake":"schema"}`), time.Now())

	path := filepath.Join(t.TempDir(), "ledger.manifest.json")
	if err := Save(doc, path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if reloaded.RunID != doc.RunID || reloaded.CohortID != doc.CohortID || reloaded.ObjectsFingerprint != doc.ObjectsFingerprint {
		t.Fatalf("Load round-trip mismatch: got %+v, want %+v", reloaded, doc)
	}
}

func TestLoadMissingFileIsAnError(t *testing.T) {
	// Unlike ledger.Load/cursor.Load, a missing manifest is an error: a
	// caller (tools/compare) has no meaningful zero value to fall back to.
	if _, err := Load(filepath.Join(t.TempDir(), "missing.manifest.json")); err == nil {
		t.Fatal("Load accepted a missing manifest file")
	}
}
