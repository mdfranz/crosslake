# Open issues

Concrete, actionable gaps not yet built -- as opposed to `LEARNINGS.md`
(narrative of what happened and bugs already fixed) or `PLAN.md` (design).
See `docs/review-telemetry-plan.md` for the review these mostly trace back
to.

## Per-stage rejected/duplicate record counts are incomplete and not durable

**What exists today:**

- **Beam (Tier 2, `pipelines/parquet-writer/parquet_writer/transforms.py`)**
  already has real per-stage counters: `parsed_records`,
  `rejected_invalid_json`, `rejected_parse_error` (Beam `Metrics.counter`),
  plus a `_rejects` text sink and a Logfire summary
  (`pipeline.py`'s `rejected_total`, `sample_reject_types`). This is
  solid and doesn't need rework.
- **The ledger (`internal/ledger`)** prevents re-processing an
  already-committed object, which is a form of duplicate prevention -- but
  it's silent: `ledgerCheckpointer.Pending` just returns fewer objects, with
  no counter or log line saying how many were filtered as already-seen vs.
  genuinely new.

**What's missing:**

1. **The Go poller has no "rejected" concept at all.** A parse failure in
   `runOnce` (`cmd/poller/main.go`, `avroenc.FromJSON` erroring) aborts the
   *entire run* (`return total, fmt.Errorf(...)`) rather than being counted
   and skipped like Beam already does for its own parse failures. One
   malformed record currently means zero records are written for that whole
   chunk, not "every other record still lands, this one is flagged."
2. **No stage exposes an explicit duplicate counter.** The ledger's dedup is
   real but invisible -- there's no way to see "N objects were skipped
   because already committed" without diffing ledger sizes by hand.
3. **None of this reaches the durable manifest.** Even where Beam's
   accepted/rejected counters already exist, they're ephemeral (Logfire +
   a per-run text file), not part of `internal/manifest`'s `Document` --
   so `docs/review-telemetry-plan.md`'s literal ask ("input, accepted,
   rejected, duplicate, and output record counts per stage" as a durable,
   checkable artifact) isn't met even for the one stage that already
   counts rejects at runtime.

**Why it matters:** without per-stage counts, "N records ingested" can hide
a run that silently accepted fewer records than it saw (poller) or that
can't be distinguished from "N records ingested, M of them were rejected
and M were logged" after the fact once the ephemeral counters/telemetry
have aged out (Beam). The manifest currently only proves *what got
committed*, not *what was seen but didn't make it, and why*.

**Suggested shape**, consistent with the existing `checkpointer` interface
and `internal/manifest`'s `Document`:

- Poller: change the parse-failure path in `runOnce` to count-and-skip
  (with a categorized reason, mirroring Beam's `rejected_invalid_json`/
  `rejected_parse_error` split) rather than aborting the chunk; write
  rejected raw lines somewhere inspectable, mirroring Beam's `_rejects`
  sink rather than just dropping them.
- Ledger: have `Pending` (or a wrapper) return or log a duplicate count
  (objects seen in the listing but already committed) alongside the new
  list.
- `internal/manifest.Document`: add `InputRecords`, `AcceptedRecords`,
  `RejectedRecords` (poller-side; already countable once the above lands)
  and `DuplicateObjects` fields; extend `compare/manifest.py`'s checks to
  cover them.
- Beam: thread its existing counters into a small JSON summary file next
  to its output (not just Logfire) so `compare/manifest.py` can read Tier
  2's accepted/rejected counts the same way it reads the poller's.

Not started as of the `ledger-and-manifest` branch (ledger + run/cohort
manifest landed; see `LEARNINGS.md` #20-22).

## Manifest object fingerprints are recorded but not yet verified live

`internal/manifest` correctly stores an order-independent fingerprint of the
source `(key, etag)` set. However, `tools/compare/compare/manifest.py` only
compares manifest object count, compressed bytes, and record counts. Its
DuckDB `read_blob` inventory exposes object names and sizes but not S3 ETags,
so `objects_fingerprint` is currently provenance for a human to inspect, not
a fail-closed reconciliation check.

This leaves a narrow false-green case: source objects can be replaced while
preserving the same count, compressed-byte total, and record count. If the
derived tiers are then regenerated, the current checks can all pass even
though the object cohort differs from the recorded ingest cohort.

Add an authenticated S3 inventory path that obtains key + ETag (for example,
via a small AWS SDK-backed helper or a versioned S3 Inventory export), compute
the same sorted fingerprint, and add it to `compare.manifest.check` as a
required check. The implementation must keep raw keys, ETags, bucket names,
and account identifiers local: emit only the aggregate fingerprint and a
categorized pass/fail result to reports and telemetry.
