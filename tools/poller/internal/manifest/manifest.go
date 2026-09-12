// Package manifest writes the durable, ingest-time record of what a
// poller --once run actually committed: object count, bytes, and an
// order-independent (key, etag) fingerprint, plus enough provenance
// (git commit, schema version, tool versions) to reproduce the run.
//
// This is deliberately not the same thing as tools/compare's
// cohort_signature query (see queries.sql): cohort_signature proves the
// three comparison views (S3 baseline, Tier 2 Parquet, Tier 3 Avro) agree
// with EACH OTHER at query time. It cannot prove that agreement matches
// what was actually ingested -- if, say, the Beam pipeline were re-run
// after S3 drifted, all three views could agree with each other on a
// cohort that isn't the one the poller originally committed, and
// cohort_signature would report a clean reconciliation on the wrong data.
// The manifest is the independent, durable ground truth tools/compare
// checks its live numbers against (see compare/manifest.py) -- exactly the
// gap docs/review-telemetry-plan.md's "the comparison cohort is
// uncontrolled" finding described.
package manifest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/mdfranz/crosslake/tools/poller/internal/atomicfile"
	"github.com/mdfranz/crosslake/tools/poller/internal/ledger"
)

// Document is the on-disk manifest shape. Field names mirror
// docs/review-telemetry-plan.md's manifest requirement list.
type Document struct {
	Version   int       `json:"version"`
	RunID     string    `json:"run_id"`
	CohortID  string    `json:"cohort_id"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`

	GitCommit     string `json:"git_commit,omitempty"`
	SchemaVersion string `json:"schema_version"`
	GoVersion     string `json:"go_version"`

	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`

	ObjectCount        int    `json:"object_count"`
	TotalBytes         int64  `json:"total_bytes"`
	TotalRecords       int    `json:"total_records"`
	ObjectsFingerprint string `json:"objects_fingerprint"`
}

const currentVersion = 1

// CohortID derives a stable identity for (bucket, prefix) -- the same
// bucket/prefix pair always produces the same cohort_id, independent of
// which objects have been committed so far, so a manifest written after a
// partial ingest and one written after a complete one for the same prefix
// are recognizably the same cohort. Short (16 hex chars) since it's a
// human-facing correlation ID, not a security boundary.
func CohortID(bucket, prefix string) string {
	h := sha256.Sum256([]byte(bucket + "\x00" + prefix))
	return hex.EncodeToString(h[:8])
}

// newRunID returns a fresh, sortable, human-readable per-invocation ID. Not
// a UUID (no dependency worth adding for this) -- a UTC timestamp plus 4
// random bytes is unique enough for a local single-poller-instance tool
// (see internal/cursor and internal/ledger's "not safe for concurrent
// poller instances" constraint, which applies here too).
func newRunID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read on the standard reader never errors
	return fmt.Sprintf("run-%s-%s", now.UTC().Format("20060102T150405Z"), hex.EncodeToString(b[:]))
}

// gitCommit reads the revision embedded by the Go toolchain's VCS stamping
// (available since Go 1.18) when present, falling back to shelling out to
// `git rev-parse HEAD`. The fallback is required, not just belt-and-braces:
// confirmed directly that `go run` -- the command every doc example in this
// repo uses (README.md, docs/runbook.md) -- never embeds VCS settings at
// all, only `go build` does. Without this fallback, git_commit would be
// empty for the documented workflow, not just as a rare edge case.
// tools/compare/compare/report.py's _git_metadata() already shells out to
// git the same way, so this isn't a new class of dependency for the repo.
// Degrades to "" (not an error) if neither source works, since git
// provenance is informational, not load-bearing for the cohort checks that
// matter (object_count/total_records/objects_fingerprint).
func gitCommit() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// schemaVersion hashes the schema file's bytes rather than requiring an
// explicit version field in cloudtrail.avsc (which doesn't have one) --
// any field-list change changes this value, which is exactly what a
// consumer needs to know before trusting a manifest against a possibly
// different schema.
func schemaVersion(schemaBytes []byte) string {
	h := sha256.Sum256(schemaBytes)
	return hex.EncodeToString(h[:8])
}

// objectsFingerprint hashes the sorted (key, etag) pairs so the result is
// independent of ledger iteration/commit order -- two runs that committed
// the same object set in a different order (e.g. concurrent chunks, a
// resumed backfill) produce an identical fingerprint. Distinct from
// tools/compare's cohort_signature, which fingerprints record *content*
// (eventID set); this fingerprints *source object identity*.
func objectsFingerprint(entries []ledger.Entry) string {
	h := sha256.New()
	for _, e := range entries { // entries is already sorted by key -- see Ledger.Entries
		h.Write([]byte(e.Key))
		h.Write([]byte{0})
		h.Write([]byte(e.ETag))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Build computes a Document from the ledger's current state for bucket, as
// of a run that started at startedAt. schemaBytes is the raw
// cloudtrail.avsc content (Build hashes it; it doesn't parse it).
func Build(l *ledger.Ledger, bucket, prefix string, schemaBytes []byte, startedAt time.Time) Document {
	entries := l.Entries(bucket)
	var totalBytes int64
	var totalRecords int
	for _, e := range entries {
		totalBytes += e.Size
		totalRecords += e.RecordsWritten
	}
	now := time.Now().UTC()
	return Document{
		Version:            currentVersion,
		RunID:              newRunID(now),
		CohortID:           CohortID(bucket, prefix),
		StartedAt:          startedAt.UTC(),
		EndedAt:            now,
		GitCommit:          gitCommit(),
		SchemaVersion:      schemaVersion(schemaBytes),
		GoVersion:          runtime.Version(),
		Bucket:             bucket,
		Prefix:             prefix,
		ObjectCount:        len(entries),
		TotalBytes:         totalBytes,
		TotalRecords:       totalRecords,
		ObjectsFingerprint: objectsFingerprint(entries),
	}
}

// Save writes doc to path atomically. Like the ledger, this can reveal
// source topology (bucket, prefix), so it's owner-only.
func Save(doc Document, path string) error {
	if err := atomicfile.WriteJSON(path, doc, 0o600); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	return nil
}

// Load reads a manifest file. Unlike ledger.Load/cursor.Load, a missing
// manifest is an error here rather than an empty zero value: a caller
// asking to Load one (typically tools/compare, cross-checking against it)
// has nothing meaningful to fall back to.
func Load(path string) (Document, error) {
	var doc Document
	found, err := atomicfile.ReadJSON(path, &doc)
	if err != nil {
		return Document{}, fmt.Errorf("manifest: %w", err)
	}
	if !found {
		return Document{}, fmt.Errorf("manifest: %s not found -- run the poller with --once first", path)
	}
	return doc, nil
}
