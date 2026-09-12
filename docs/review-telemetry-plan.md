# Architecture and telemetry review (2026-09-12)

This review treats `PLAN.md` as a hypothesis, not as evidence that the
implementation is correct. The current Local Mode is useful, but its output is
not yet a trustworthy basis for format or architecture conclusions.

## Executive assessment

The architecture is strongest as a learning harness: one real source, two
typed representations, common SQL, and a local-first path. It is not yet a
sound ingestion architecture or a controlled benchmark. The immediate goal
should be **reproducible and reconcilable experiments**, not GCP expansion.

The three highest-risk issues are:

1. **The cursor can lose data.** `ListObjectsV2(StartAfter=last_key)` assumes
   CloudTrail delivery order matches lexicographic key order. It does not.
2. **The tiers are not demonstrably the same cohort.** Remote S3 inventory is
   compared with cumulative local files without a captured object manifest or
   record-level reconciliation.
3. **The benchmark confounds format with topology and runtime.** Hundreds of
   tiny remote gzip objects are compared with a few local files, while Avro is
   produced in Go and Parquet through Beam/Python.

Until these are fixed, results are observations about this particular path,
not evidence that Parquet or Avro is generally smaller or faster.

## Evidence from the source and AWS inventory

### P0: `StartAfter` is not a durable CloudTrail checkpoint

The poller persists one `last_key` and passes it to `ListObjectsV2.StartAfter`.
CloudTrail keys contain a timestamp and random suffix, while object delivery is
asynchronous. A trail-wide prefix also places region before date, so a single
global lexicographic cursor cannot represent progress across regions.

Read-only inventory inspection of one active day found 1,002 objects across
four regions. In the two busiest regions, 419/877 and 16/66 adjacent deliveries
within the same minute were lexicographically inverted. Replaying that inventory
by `LastModified` showed a 60-second `StartAfter` poll would omit about 39.3% and
13.6% of those regions respectively; even a ten-minute interval still omitted
objects. This is a correctness failure, not a tuning issue.

Recommended design:

- Inventory every bounded date/region prefix and persist a **seen-object
  ledger** keyed by `(bucket-scope hash, key, version/etag)`.
- Treat notifications as hints for low latency, never as the sole completeness
  mechanism. Periodically reconcile against S3 listing or S3 Inventory.
- Claim work idempotently and checkpoint only after the canonical sink commits.
- Make `--once` over a closed, day-scoped prefix the supported first milestone.
  Keep continuous polling explicitly experimental until the ledger exists.

An overlap window can reduce misses but cannot prove completeness and is not a
substitute for the ledger.

### P0: output durability and ownership were ambiguous

The OCF encoder buffers writes, but the cursor was saved after `WriteRecord`
and before `Close`. A crash could therefore acknowledge an object whose Avro
blocks were never flushed. This branch adds an explicit sink flush/fsync before
each checkpoint and makes cursor replacement atomic. It also creates raw data,
Avro, and cursor files with owner-only permissions.

That fix narrows the failure window but does not make two append-only files
transactional. The better architecture is to make immutable raw objects the
canonical landing zone, then derive Avro and Parquet from a shared manifest.
Each derived artifact should record its input cohort ID and reconcile counts.

### P0: the comparison cohort is uncontrolled

`sizes.py` lists the current S3 prefix but measures cumulative local outputs.
`query_bench.py` does not assert equal row counts or content, and the poller can
append to existing files across runs. Therefore the phrase "same data" is not
currently enforced.

Every experiment should first write a gitignored JSON manifest containing:

- `run_id`, `cohort_id`, UTC start/end, git commit, schema version;
- sanitized source scope hash, object count, compressed bytes, and an
  order-independent hash of `(key, etag)` pairs;
- input, accepted, rejected, duplicate, and output record counts per stage;
- output file count, bytes, codec, row count, and content fingerprint;
- tool/runtime versions and benchmark parameters.

The report must fail closed when tier fingerprints or row counts differ.
Telemetry is a searchable projection of this artifact, not the source of truth.

### P1: the current benchmark answers a topology question

The inspected S3 data consisted of hundreds of small objects (for example,
roughly 701 KiB across 592 objects in one partial regional day). Remote S3
listing, requests, TLS, decompression, and JSON parsing dominate a comparison
against a few local Parquet/Avro files. The observed seconds-versus-milliseconds
gap is valuable evidence for compaction and localization, but not an isolated
file-format result.

Split the experiments:

1. **Format microbenchmark:** same typed in-memory rows, same process, isolated
   encode/decode, fixed codec levels, warmups, repeated randomized order, and
   median/p95 plus bytes.
2. **Storage-layout benchmark:** same engine and storage locality, vary format,
   file size, partitioning, projection, selectivity, and cold/warm cache.
3. **Pipeline benchmark:** measure end-to-end cost, lag, throughput, retries,
   rejects, and resource consumption for the real Go/Beam paths.

The branch upgrades the query and encode tools to warmups plus repeated,
deterministically randomized trials with median/p95 reporting. Query result
digests and a cohort signature now fail the report closed on mismatched inputs.
Cold-cache and larger-scale trials remain future experiments.

### P1: dual-publish is a poor correctness boundary

The target cloud plan publishes each record independently to raw and typed
topics. A partial failure can advance one tier without the other, and there is
no transaction spanning S3, two Pub/Sub publishes, and the checkpoint. That
creates comparison drift and complex retry semantics.

Prefer one durable raw ingress with a stable `event_id`/`cohort_id`, then fan out
to format-specific consumers. At-least-once delivery plus idempotent sinks is a
clearer model than attempting exactly-once behavior across clouds. Preserve the
source object identity only in the private manifest; do not export raw keys.

### P1: schema conclusions are premature

The schema is hand-mirrored in Go, Avro JSON, Python, and DuckDB views. The live
sample already demonstrated identity variants and object-or-null nested fields.
Hand synchronization will drift, and the selected typed subset means a smaller
file can partly reflect dropped information rather than superior encoding.

Generate language bindings and the Arrow schema from one versioned schema (or
add contract tests that compare all field names/types). Report retained-field
coverage and null rates. Add synthetic, sanitized fixtures for every observed
identity/event variant and for RFC3339 offsets/fractional seconds.

## Telemetry that produces useful learnings

The previous design emitted one remote span per record and forced every Beam
worker invocation under one driver-created `traceparent`. That creates high
volume and implies causal structure that does not exist in distributed Beam
execution. This branch uses Beam's native counters/distributions for element
work, which are runner-scoped and designed for processed/error counts. See the
[Apache Beam metrics guide](https://beam.apache.org/documentation/programming-guide/#metrics).

Use this signal model:

| Signal | Granularity | Purpose |
|---|---|---|
| durable run artifact | one per experiment | reproducibility and reconciliation |
| trace | run → stage → object/bundle/query iteration | latency decomposition and failures |
| counter | accepted/rejected/retried/duplicate per stage | correctness and reliability |
| distribution | object bytes, records/object, parse/write/query duration | shape and tails |
| log/event | state transition or categorized failure | diagnosis without payload leakage |

Required common dimensions are `run_id`, `cohort_id`, `service.name`,
`stage`, `mode`, `format`, `codec`, `schema_version`, and `result`. Keep the
dimension set bounded. `run_id` and `cohort_id` are correlation fields, not
metric labels.

Never export CloudTrail payloads, bucket names, object keys, account IDs, ARNs,
principal/access-key IDs, source IPs, user agents, request/response bodies, or
raw exception strings. Use categorized error codes such as `decode_failed` and
keep sensitive diagnostic detail local. The prior Go spans included `s3.key`
and raw error text; this branch removes both from exported spans.

Questions the telemetry should answer directly:

- Did every discovered object reach the canonical sink, and did every accepted
  record appear exactly once in each derived tier?
- Where is end-to-end lag spent: discovery, fetch, decompress, parse, encode,
  flush, or queueing?
- How do object/file size distributions affect throughput and query p95?
- What fraction is rejected by reason and schema version?
- How much of a measured difference remains after matching cohort, engine,
  locality, compression, cache state, and query result?

## Revised phasing and exit criteria

### Phase 0 — make Local Mode trustworthy

- [x] Add the object ledger and closed-prefix safety checks. Landed on
      `ledger-and-manifest`: `internal/ledger`, `s3source.Source.List`
      (full listing, no `StartAfter`), `--once`'s `ledgerCheckpointer`,
      and read-only `--reconcile`. See `LEARNINGS.md` #20.
- [x] Land immutable raw inputs and a run/cohort manifest. Landed:
      `internal/manifest` writes `run_id`/`cohort_id`/`git_commit`/
      `schema_version`/`object_count`/`total_bytes`/`total_records`/
      `objects_fingerprint` after every `--once`; `compare/manifest.py`
      reads it (`manifest_files:` config key for a multi-prefix cohort,
      mirroring `aws.s3_prefixes`). "Immutable raw inputs" is still
      `raw.jsonl` being append-only plus the ledger's dedup, not a
      separate immutable-storage mechanism. See `LEARNINGS.md` #22.
- [x] Reconcile object and record counts before reporting. `--reconcile`
      covers object-level gaps for one prefix (read-only, pre-ingest).
      `compare/manifest.py` (wired into `report.py`'s fail-closed gate)
      covers record-level counts across `s3_baseline`/`tier2_parquet`/
      `tier3_avro` against the durable manifest, at report time -- and
      caught a real duplicate-record bug doing it (`LEARNINGS.md` #22).
- [x]/[ ] Add synthetic contract tests and crash/retry tests. Crash/retry:
      done for the ledger (`TestRunOnceLedgerCrashRetryReprocessesOnlyUnflushedChunk`).
      Synthetic schema-variant contract tests (the P1 "schema conclusions
      are premature" finding): not started.

Exit: rerunning or crashing at every checkpoint produces no missing records
(true for the ledger's own bookkeeping now; not yet proven end-to-end
against the raw/Avro pair together, which item 4 in `LEARNINGS.md` notes
still isn't transactional); all tier fingerprints match for a fixed cohort
-- true as of `LEARNINGS.md` #22's clean 2-day cohort (manifest totals and
every tier's live count agree), verified against real S3/local data,
including a real mismatch the check correctly caught and failed closed on
before that clean run.

### Phase 1 — build the experiment harness

- Add repeatable query/encode/decode benchmarks with warmups and randomized
  order; capture median, p95, and machine metadata.
- Separate remote-layout, local-format, and pipeline experiments.
- Emit one run summary to Logfire and retain the complete JSON artifact locally.

Exit: another machine can replay a manifest and obtain comparable results with
validated query outputs.

### Phase 2 — harden telemetry and schema evolution

- Add bundle/stage metrics, lag histograms, categorized errors, sampling, and
  budget/cardinality checks.
- Generate or contract-test schemas and run additive/breaking evolution cases.

Exit: dashboards answer the questions above without inspecting raw CloudTrail
data or depending on per-record spans.

### Phase 3 — cloud fan-out smoke test

- Provision minimal GCP resources only after Local Mode exit criteria pass.
- Use one canonical ingress and idempotent fan-out; validate replay and partial
  failure before measuring cost/performance.

Exit: bounded cloud cohorts reconcile exactly and teardown is automated.

### Phase 4 — scale and operational comparison

- Run representative volumes/file sizes, cold and warm query trials, and cost
  accounting. Compare operational complexity and schema evolution alongside
  throughput and storage.

Exit: conclusions include confidence/variance and clearly state which factor
(format, layout, runtime, network, or runner) each experiment isolates.
