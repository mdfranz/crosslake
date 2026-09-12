package ledger

import (
	"path/filepath"
	"testing"
)

func TestRecordThenSeen(t *testing.T) {
	l := New()
	if l.Seen("b", "k1", "e1") {
		t.Fatal("Seen true before Record")
	}
	l.Record("b", "k1", "e1", 100, 5)
	if !l.Seen("b", "k1", "e1") {
		t.Fatal("Seen false after Record")
	}
	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1", l.Len())
	}
}

func TestOverwrittenObjectIsUnseenUnderNewETag(t *testing.T) {
	// A key being reused (the S3 object was overwritten) must not be
	// mistaken for "already processed" -- identity is (bucket, key, etag),
	// not key alone.
	l := New()
	l.Record("b", "k1", "etag-v1", 100, 5)
	if l.Seen("b", "k1", "etag-v2") {
		t.Fatal("Seen true for a different etag on the same key")
	}
}

func TestDifferentBucketSameKeyIsUnseen(t *testing.T) {
	l := New()
	l.Record("bucket-a", "k1", "e1", 100, 5)
	if l.Seen("bucket-b", "k1", "e1") {
		t.Fatal("Seen true across buckets")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.json")

	l := New()
	l.Record("b", "2026/09/10/obj1.json.gz", "etag1", 111, 3)
	l.Record("b", "2026/09/10/obj2.json.gz", "etag2", 222, 7)
	if err := l.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if reloaded.Len() != 2 {
		t.Fatalf("Len after reload = %d, want 2", reloaded.Len())
	}
	if !reloaded.Seen("b", "2026/09/10/obj1.json.gz", "etag1") {
		t.Fatal("obj1 not seen after reload")
	}
	if !reloaded.Seen("b", "2026/09/10/obj2.json.gz", "etag2") {
		t.Fatal("obj2 not seen after reload")
	}
}

func TestLoadMissingFileIsEmpty(t *testing.T) {
	l, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if l.Len() != 0 {
		t.Fatalf("Len = %d, want 0 for a missing ledger file", l.Len())
	}
}

func TestRecordIsIdempotent(t *testing.T) {
	l := New()
	l.Record("b", "k1", "e1", 100, 5)
	l.Record("b", "k1", "e1", 100, 5) // simulates a re-run committing the same object again
	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after re-recording the same object", l.Len())
	}
}

func TestMissingFindsNewAndSkipsCommitted(t *testing.T) {
	l := New()
	l.Record("b", "k1", "e1", 1, 1)

	got := l.Missing("b", []KeyETag{
		{Key: "k1", ETag: "e1"},             // already committed
		{Key: "k2", ETag: "e2"},             // new
		{Key: "k1", ETag: "e1-overwritten"}, // same key, different etag: not the same object
	})

	want := map[KeyETag]bool{
		{Key: "k2", ETag: "e2"}:             true,
		{Key: "k1", ETag: "e1-overwritten"}: true,
	}
	if len(got) != len(want) {
		t.Fatalf("Missing = %v, want 2 entries matching %v", got, want)
	}
	for _, g := range got {
		if !want[g] {
			t.Fatalf("unexpected entry in Missing: %v", g)
		}
	}
}

func TestMissingIsBucketScoped(t *testing.T) {
	l := New()
	l.Record("bucket-a", "k1", "e1", 1, 1)

	got := l.Missing("bucket-b", []KeyETag{{Key: "k1", ETag: "e1"}})
	if len(got) != 1 {
		t.Fatalf("Missing across buckets = %v, want the object reported missing for bucket-b", got)
	}
}

func TestEntriesUnderScopesToPrefix(t *testing.T) {
	l := New()
	l.Record("b", "CloudTrail/us-east-1/2026/09/10/a.json.gz", "e1", 1, 1)
	l.Record("b", "CloudTrail/us-east-1/2026/09/11/b.json.gz", "e2", 2, 2)
	l.Record("other-bucket", "CloudTrail/us-east-1/2026/09/10/c.json.gz", "e3", 3, 3)

	got := l.EntriesUnder("b", "CloudTrail/us-east-1/2026/09/10/")
	if len(got) != 1 || got[0].Key != "CloudTrail/us-east-1/2026/09/10/a.json.gz" {
		t.Fatalf("EntriesUnder returned %v, want only the matching bucket/prefix entry", got)
	}
}
