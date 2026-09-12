// Package ledger tracks which S3 objects a poller run has durably committed
// to the local sink, keyed by object identity rather than by a single
// lexicographic boundary.
//
// docs/review-telemetry-plan.md's P0 finding measured why a `last_key`
// cursor (see internal/cursor) is unsafe: a read-only replay of one real
// CloudTrail day (1,002 objects, four regions) found 419/877 and 16/66
// same-minute deliveries lexicographically inverted in the two busiest
// regions, and a 60s StartAfter poll would have silently omitted ~39.3% and
// ~13.6% of those regions' objects respectively -- permanently, since a
// cursor that has already advanced past a key never revisits it. No polling
// interval fixes this; the ordering assumption itself is false.
//
// The ledger instead records a set of objects already committed, identified
// by (bucket, key, etag) -- not by their position relative to any other
// object. Combined with s3source.Source.List's full (non-StartAfter) prefix
// listing, a caller can re-list a closed prefix on every run, diff against
// the ledger, and process only what's new: correct regardless of delivery
// order, at the cost of re-listing (not re-fetching or re-processing)
// objects already known. See cmd/poller's ledgerCheckpointer.
//
// An entry is written only after the caller's sink has flushed the
// corresponding records (see Record's doc comment) -- so a crash before
// that point simply leaves the object absent from the ledger, and the next
// run reprocesses it. That is the same at-least-once/idempotent-replay
// model docs/runbook.md and LEARNINGS.md already document for the
// checkpoint-batched disksink, just keyed by object identity instead of a
// batch boundary.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mdfranz/crosslake/tools/poller/internal/atomicfile"
)

// KeyETag identifies one S3 object independent of any particular bucket
// (the bucket is supplied separately to Seen/Record/Diff) -- kept minimal
// and decoupled from s3source.Object so this package has no dependency on
// the AWS SDK and stays trivially testable.
type KeyETag struct {
	Key  string
	ETag string
}

// Entry is one committed object's record, as persisted to disk.
type Entry struct {
	Bucket         string    `json:"bucket"`
	Key            string    `json:"key"`
	ETag           string    `json:"etag"`
	Size           int64     `json:"size"`
	RecordsWritten int       `json:"records_written"`
	CommittedAt    time.Time `json:"committed_at"`
}

// document is the on-disk shape: a slice (not a map) so JSON output is
// stable and diff-able, with a version field so a future incompatible
// change can detect and migrate old ledgers instead of silently
// misreading them.
type document struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

const currentVersion = 1

// Ledger is an in-memory seen-object set, loaded from and saved to one JSON
// file. It is not safe for concurrent use -- same constraint as
// internal/cursor, and for the same reason (single local poller instance).
type Ledger struct {
	byID map[string]Entry
}

// New returns an empty ledger, for tests and for the case where no ledger
// file exists yet.
func New() *Ledger {
	return &Ledger{byID: make(map[string]Entry)}
}

// Load reads the ledger file, returning an empty Ledger (nothing committed
// yet) if the file doesn't exist.
func Load(path string) (*Ledger, error) {
	var doc document
	found, err := atomicfile.ReadJSON(path, &doc)
	if err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	l := New()
	if !found {
		return l, nil
	}
	for _, e := range doc.Entries {
		l.byID[id(e.Bucket, e.Key, e.ETag)] = e
	}
	return l, nil
}

// Save replaces the ledger file atomically. Like the cursor, the ledger can
// reveal source topology (bucket, keys), so it's written owner-only.
func (l *Ledger) Save(path string) error {
	doc := document{Version: currentVersion, Entries: make([]Entry, 0, len(l.byID))}
	for _, e := range l.byID {
		doc.Entries = append(doc.Entries, e)
	}
	sort.Slice(doc.Entries, func(i, j int) bool {
		if doc.Entries[i].Bucket != doc.Entries[j].Bucket {
			return doc.Entries[i].Bucket < doc.Entries[j].Bucket
		}
		return doc.Entries[i].Key < doc.Entries[j].Key
	})
	if err := atomicfile.WriteJSON(path, doc, 0o600); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	return nil
}

// Seen reports whether (bucket, key, etag) has already been committed.
// A different etag for the same key (the object was overwritten) is
// correctly treated as unseen -- key alone is not a stable identity.
func (l *Ledger) Seen(bucket, key, etag string) bool {
	_, ok := l.byID[id(bucket, key, etag)]
	return ok
}

// Record commits one object to the ledger. Callers must only call Record
// after every record from this object has been durably flushed to the sink
// (see disksink.Sink.Flush) -- Record is the checkpoint itself, not a
// pre-commit marker. Calling Record twice for the same (bucket, key, etag)
// overwrites the entry (idempotent).
func (l *Ledger) Record(bucket, key, etag string, size int64, recordsWritten int) {
	l.byID[id(bucket, key, etag)] = Entry{
		Bucket:         bucket,
		Key:            key,
		ETag:           etag,
		Size:           size,
		RecordsWritten: recordsWritten,
		CommittedAt:    time.Now().UTC(),
	}
}

// Len returns the number of committed entries, for logging/telemetry.
func (l *Ledger) Len() int { return len(l.byID) }

// Entries returns every committed entry for bucket, sorted by key -- the
// basis for internal/manifest's aggregate counts and object-identity
// fingerprint. Sorted (rather than map iteration order) so the fingerprint
// is deterministic regardless of how entries were loaded or recorded.
func (l *Ledger) Entries(bucket string) []Entry {
	var entries []Entry
	for _, e := range l.byID {
		if e.Bucket == bucket {
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

// EntriesUnder returns committed entries in bucket whose keys are under
// prefix, sorted by key. A ledger can safely be reused for disjoint prefixes:
// identity is still the complete bucket/key/etag tuple, but a manifest for
// one prefix must not aggregate entries committed while a different prefix
// was configured. An empty prefix intentionally matches every key.
func (l *Ledger) EntriesUnder(bucket, prefix string) []Entry {
	var entries []Entry
	for _, e := range l.byID {
		if e.Bucket == bucket && strings.HasPrefix(e.Key, prefix) {
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

// Missing returns the subset of objects not yet committed for bucket, in
// the same order they were given. Callers pass a fresh S3 listing (see
// s3source.Source.List) to find new work (the normal --once path) or, run
// later against the same closed prefix, to reconcile: any object S3 has
// that the ledger doesn't is either work this run hasn't reached yet or a
// late delivery that arrived after an earlier run already checkpointed
// past where it would have sorted -- exactly the gap a last_key cursor
// hides. See cmd/poller's --reconcile mode.
func (l *Ledger) Missing(bucket string, objects []KeyETag) []KeyETag {
	var missing []KeyETag
	for _, o := range objects {
		if !l.Seen(bucket, o.Key, o.ETag) {
			missing = append(missing, o)
		}
	}
	return missing
}

// id computes a stable identity for (bucket, key, etag). Hashing (rather
// than using the tuple directly as a map key) keeps the in-memory index
// bounded-width regardless of key length and mirrors
// docs/review-telemetry-plan.md's "(bucket-scope hash, key, version/etag)"
// identity recommendation; the raw fields are still stored in Entry itself
// for inspection and reconciliation reporting, since this file is local and
// gitignored like cursor.json already is.
func id(bucket, key, etag string) string {
	h := sha256.Sum256([]byte(bucket + "\x00" + key + "\x00" + etag))
	return hex.EncodeToString(h[:])
}
