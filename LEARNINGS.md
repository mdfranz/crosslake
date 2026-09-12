# Learnings

Living notes from building and running the Avro-vs-Parquet CloudTrail
pipeline. See `PLAN.md` for the design and `docs/` for supporting detail.
Scope so far: **Local Mode only** (no GCP) -- real AWS S3 CloudTrail source,
local disk sinks, DuckDB comparisons.

## Real bugs found while building this (not hypothetical)

1. **OCF truncation across runs.** The Avro disk sink originally opened
   `events.avro` with `O_TRUNC` on every poller run, on the theory that
   "each run is one batch." Once the cursor advances between runs, this
   silently discards all previously-written records -- caught because
   `tier3_avro` had 3 rows while `raw.jsonl` and `tier2_parquet` had 1456.
   Fixed by opening `O_RDWR` (no truncate) and letting
   `ocf.NewEncoderWithSchema` detect and append to the existing container
   (it reads the header back and seeks to the end automatically). See
   `tools/poller/internal/disksink/disksink.go`.

2. **Beam breaks span parenting across the DoFn worker-thread boundary.**
   Every `parse_record` span came out as its own orphaned root trace
   (`parent_span_id: null` for all ~1456 of them) instead of nesting under
   `run_pipeline`, because Python's ambient OTEL context doesn't cross
   Beam's thread boundary. Fixed by capturing a W3C `traceparent` before
   building the pipeline graph and re-attaching it inside every
   `DoFn.process()` call. That made the trace tree look tidy but created one
   remote span per record and implied false driver-to-worker causality. The
   optimized design uses runner-native Beam counters/distributions and exports
   one run summary instead. See `docs/observability.md`.

3. **A lexicographic S3 cursor loses late CloudTrail deliveries.** Read-only
   inventory replay found same-minute key inversions and substantial simulated
   omissions with `StartAfter`, including at long polling intervals. A single
   cursor is even less valid across a trail-wide prefix because region precedes
   date in the key. Continuous polling remains unsafe until an object ledger
   and completeness reconciliation replace `last_key`; see
   `docs/review-telemetry-plan.md`.

4. **A checkpoint could outrun buffered output.** The poller saved its cursor
   after encoding records but before the Avro OCF encoder was closed/flushed.
   The optimized branch flushes and fsyncs both local representations before
   atomically replacing the cursor. Two files still are not a transaction, so a
   canonical raw landing plus derived outputs remains the target design.

4a. **The per-object checkpoint fix (item 4) had a real, measured cost:
   flush-per-object fragmented Avro into one ~2.4-record compression block
   per S3 object, roughly doubling file size on a real batch (344KB -> 666KB
   for ~1,490 records; Parquet, written in a separate Beam pass decoupled
   from the poller's flush loop, was unaffected).** Confirmed by counting
   actual OCF blocks with `fastavro.read.block_reader` (625 blocks, avg 2.38
   records/block) and cross-checking against Logfire's own
   `object.records_written` distribution -- they matched exactly. Two
   separable causes, both fixed:
   - `cmd/poller`'s checkpoint (flush + cursor save) ran after every single
     object. Now batches every `checkpoint_every_objects` objects (default
     100, configurable), always flushing on the last object of a poll too.
     This widens the at-least-once replay window on crash from one object to
     up to N objects -- not a new failure mode, the same
     idempotent-replay/possible-duplicate model as before, just at a larger,
     tunable grain.
   - Independently, `hamba/avro/v2/ocf`'s `Encoder` has its own default
     `BlockLength: 100` (records) that caps block size regardless of how
     often `Flush()` is called -- raising the checkpoint interval alone
     didn't fully fix it, since the encoder was still auto-closing a block
     every 100 records. Fixed with `ocf.WithBlockLength(1000)` +
     `ocf.WithBlockSize(4 MiB)` (a memory safety cap) in `disksink.go`.
   Verified fix: 7 blocks, avg 215 records/block, 344KB for 1,507 records --
   back at parity with the pre-regression baseline.

5. **DuckDB's avro extension can't read hamba/avro's `zstandard`-codec OCF
   files** ("File header contains an unknown codec"), even though the file
   is spec-valid Avro. Switched both sides to `deflate`/`gzip` (same zlib
   family) for a codec DuckDB, hamba/avro, and pyarrow all agree on.

6. **`LOGFIRE_API_TOKEN` (set in this environment) is not a valid OTLP
   write token** -- it's scoped for the Logfire Claude Code plugin's own MCP
   calls, not ingestion, and gets a `401 Unauthorized` against
   `logfire-us.pydantic.dev/v1/traces`. Needed a real project write token
   via `uvx logfire --region=us auth` + `projects use`. See
   `docs/observability.md`.

7. **Uncompressed defaults make any "vs. gzip" comparison meaningless.**
   Both `WriteToParquet`'s default codec and `ocf`'s default codec are
   "none." Before fixing this, both tiers looked *worse* than the original
   gzipped CloudTrail JSON, which would have been a misleading headline
   result. Fixed by explicitly setting a real codec on both sides.

8. **`top_event_names`'s `ORDER BY n DESC LIMIT 10` had no tiebreaker**, so
   `query_bench.py`'s new result-hash check correctly flagged
   `views_match=False` even when the underlying data agreed -- CloudTrail
   has many `eventName`s tied at low counts, so which ones land in the top
   10 was non-deterministic across repeats. Fixed with `ORDER BY n DESC,
   eventName ASC`. Left as a live example of the safeguard doing its job:
   caught a real query bug that plain single-shot timing (the original
   `query_bench.py`) would never have surfaced.

9. **Testing against "today" as the S3 prefix reconfirmed the cohort-drift
   risk `docs/review-telemetry-plan.md` predicted, live.** After fixing
   item 8, *every* query still showed `views_match=False` -- not a
   tiebreaker issue this time: `s3_baseline` had grown to 1,508 rows while
   the local tiers (captured minutes earlier) were frozen at 1,507, because
   CloudTrail kept delivering new events to "today"'s prefix during
   testing. Any comparison against an open/live day will drift like this;
   it isn't fixable by tuning a query. Practical workaround short of full
   Phase 0 (manifest + reconciliation): point `s3_prefix` at a closed prior
   day for any run where result stability matters.

## Query diversity: the original four queries didn't exercise Avro vs Parquet at all

All four original queries (`top_event_names`, `count_by_source`, `time_range`,
`cohort_signature`) were structurally identical: full-table scan, one narrow
aggregate column, no `WHERE`, no nesting. That's exactly the shape neither
format's real advantages or disadvantages show up in.
`docs/review-telemetry-plan.md`'s own storage-layout-benchmark guidance calls
out "projection, selectivity" as required dimensions -- neither was varied
at all. Added six queries, each isolating one dimension, plus a `-- views:
v1,v2` directive in `queries.sql` (parsed by `query_bench.py`) for the ones
that need a column `s3_baseline` doesn't expose:

- `count_only` -- zero-column projection floor (does a columnar reader answer
  from file metadata alone?).
- `filtered_low_selectivity` / `filtered_high_selectivity` -- `WHERE
  eventName = ...` at ~64% and ~0.3% match rates, to see whether Parquet's
  row-group statistics let it skip work that Avro (no comparable indexing)
  can't.
- `nested_identity_breakdown` (`tier2_parquet`,`tier3_avro` only) --
  `userIdentity.type`, testing nested-struct-field pruning.
- `boolean_breakdown` (`tier2_parquet`,`tier3_avro` only) -- a low-cardinality
  boolean column, the case Parquet's dictionary/RLE encoding specifically
  targets.
- `full_row_materialize` (`tier2_parquet`,`tier3_avro` only) -- the opposite
  extreme from `count_only`: touches every column, forcing full-row
  reconstruction, where a row-oriented format should be most competitive.

10. **Adding the filtered queries surfaced a real, unrelated bug: a DuckDB
    query-planning pathology in our own `s3_baseline` view, not an
    Avro/Parquet property.** `filtered_low_selectivity`/
    `filtered_high_selectivity` against `s3_baseline` took **83-92 seconds**
    median -- 9-10x slower than every unfiltered query against the same
    view (~7-11s), while the identical filters against `tier2_parquet`/
    `tier3_avro` were as fast as or faster than unfiltered. `EXPLAIN` showed
    DuckDB planning the filtered query as a `LEFT_DELIM_JOIN` (a
    correlated/lateral-join strategy), apparently re-executing `READ_JSON`'s
    remote S3 scan repeatedly instead of once. Root cause: `compare/db.py`'s
    `s3_baseline` view did the `unnest(Records)` and the
    `json_extract_string(...)` projection in one `SELECT ... FROM
    read_json(...), unnest(Records) AS t(r)`; a `WHERE` filter on the
    extracted column made DuckDB treat the unnest as correlated with the
    filter. Fixed by moving the unnest into its own CTE so nothing
    downstream can be planned as correlated with it. Verified with
    `EXPLAIN` (flat `READ_JSON -> UNNEST -> PROJECTION -> FILTER`, no
    delim-join) and by timing (83-92s -> ~7-9s, in line with the unfiltered
    baseline).

11. **`full_row_materialize`'s character-count check differed between tiers
    (1,842,492 vs 1,787,809 for the same 1,507 rows) even though every
    other check (count, `cohort_signature`'s event-ID fingerprint) agreed.**
    Not cohort drift -- the Go poller preserved the original
    `requestParameters`/etc. JSON bytes verbatim (`json.RawMessage` as-is)
    while the Python Beam path re-serialized the same logical JSON via
    plain `json.dumps()`, which differs from Go's default map-key
    ordering/escaping for byte length even when the parsed content is
    identical. **Fixed**: both sides now canonicalize before storing --
    sorted keys, compact separators, no HTML-escaping, real UTF-8 instead
    of `\uXXXX` escapes. Go: `canonicalJSONPtr` in
    `tools/poller/internal/avroenc/record.go` (unmarshal to `any`, re-marshal
    via an `Encoder` with `SetEscapeHTML(false)` -- plain `json.Marshal`
    always HTML-escapes `<>&` regardless of any other setting). Python:
    `json.dumps(value, sort_keys=True, separators=(",", ":"),
    ensure_ascii=False)` in `parquet_writer/transforms.py`. Verified: a
    fresh run now produces byte-identical strings (confirmed on a sample
    `requestParametersJson` value) and `full_row_materialize`'s
    `total_chars` matches exactly (1,795,587 both sides) with the same
    result hash. Regression-tested in
    `tools/poller/internal/avroenc/record_test.go` (out-of-order keys,
    `&`, and a non-ASCII character all round-trip to the expected
    canonical form). No equivalent Python test added -- this project has
    no pytest setup yet, and adding one is a bigger scope decision than
    this fix; live verification above stands in for it.

## Optimization review (logfire-query skill)

Reviewed real Logfire telemetry across all three services (aggregate
duration by `span_name`) alongside the Go and Python source to find
concrete optimization targets, not hypothetical ones.

12. **`compare/db.py`'s `connect()` cost 8-9 seconds on every single
    invocation of every `compare.*` tool, regardless of whether the query
    touched S3 at all.** Isolated by timing each statement in `connect()`
    separately: `INSTALL`/`LOAD` for `httpfs`/`avro` were already cached
    (21ms, 11ms) -- the entire cost was `CREATE SECRET (TYPE s3, PROVIDER
    credential_chain)`, at 8,053ms alone. DuckDB's default
    `credential_chain` search order includes EC2 instance metadata (IMDS),
    which times out (~1s/attempt, this isn't EC2) before falling through to
    the environment variables that were the actual, already-valid
    credential source the whole time. Fixed with `CHAIN 'env'`, restricting
    the search to environment variables only: **8,053ms -> 26ms** for
    secret creation, **9,395ms -> 1,425ms** for the full `connect()`
    (extension `LOAD` accounts for the rest). Verified S3 access still
    works (real `glob()`/query results unchanged) and `schema_inspect.py`
    -- a tool that never touches S3 -- dropped from ~9.4s+ to under 2s
    end-to-end. This was paid on every single `compare.*` run this entire
    session (dozens of invocations); by far the highest-impact single fix
    in this review. If auth here ever moves off static env-var credentials,
    broaden to `CHAIN 'env;config;sts;sso'` -- still excluding `instance`.

13. **The Go poller fetches S3 objects strictly sequentially** (a plain
    `for objectIndex, key := range keys { FetchAndGunzip(...) }` loop, no
    concurrency at all). Telemetry confirms this is the dominant remaining
    cost on any real poll: `fetch_object`/`process_object` spans average
    72-88ms each (network RTT-bound, not CPU-bound -- `write_record`'s own
    per-record cost is ~64 microseconds, 1000x smaller), and a poll
    against ~1,000+ objects took up to 141s wall-clock, purely from that
    latency multiplying sequentially. S3 comfortably supports concurrent
    `GetObject` calls; a bounded worker pool (e.g. 16-32 concurrent
    fetches) would plausibly cut wall-clock poll time by an order of
    magnitude for large/bursty batches. **Not implemented** -- this changes
    core fetch-loop behavior (ordering guarantees for cursor advancement,
    error handling under partial-batch failure) enough that it deserves a
    deliberate decision, not a drive-by change during an optimization
    pass. Flagged as the clear next target if poll latency matters more
    than it currently does for this learning prototype's scale.

14. Looked for but did not find a real optimization case in Go's
    `canonicalJSONPtr` (allocates a new `bytes.Buffer` + `json.Encoder` per
    escape-hatch field per record, up to 6 per record) or in Python's Beam
    `DoFn` per-element overhead (inherent to `DirectRunner`, and the whole
    point of this project is measuring Beam, not avoiding it). Both are
    real allocation/overhead patterns, but S3 fetch dominates total wall
    time by ~1000x at current scale (item 13's numbers), so neither would
    move any real metric today. Worth revisiting only if item 13's fix
    ever makes fetch latency small enough for these to become visible.

## Comparison results

Real numbers from a batch of CloudTrail records pulled from live delivery
(one day, `us-east-1`) via Local Mode. Re-run with `make compare-report`;
numbers will vary with whatever's currently polled into `./data/`.

<!-- BEGIN AUTOGENERATED: make compare-report -->
## Comparison run: 2026-09-12T16:53:42+00:00

### Sizes (vs. original gzipped CloudTrail JSON in S3)

| source | bytes | vs S3 gzip |
|---|---:|---:|
| s3_gzip_original | 747,688 | 1.00x |
| raw_jsonl_uncompressed | 2,396,139 | 3.20x |
| tier2_parquet | 321,430 | 0.43x |
| tier3_avro | 344,405 | 0.46x |

### Schema differences (Tier 2 Parquet vs Tier 3 Avro)

- `eventTime`: parquet=`TIMESTAMP WITH TIME ZONE` vs avro=`TIMESTAMP`

### Query benchmark (identical SQL across all three sources)

| query | view | elapsed_ms |
|---|---|---:|
| top_event_names | s3_baseline | 15674.5 |
| top_event_names | tier2_parquet | 5.1 |
| top_event_names | tier3_avro | 12.2 |
| count_by_source | s3_baseline | 9826.3 |
| count_by_source | tier2_parquet | 4.1 |
| count_by_source | tier3_avro | 9.1 |
| time_range | s3_baseline | 11213.5 |
| time_range | tier2_parquet | 8.6 |
| time_range | tier3_avro | 8.9 |

### Encode benchmark (same process, same rows, deflate/gzip both sides)

`1459` records.

| format | ms | bytes | records/sec |
|---|---:|---:|---:|
| parquet | 38.1 | 322,436 | 38307 |
| avro | 44.1 | 421,858 | 33050 |
<!-- END AUTOGENERATED -->

### Reading these numbers

- **The apparent size ratios are provisional.** The report did not prove that
  the current S3 listing and cumulative local outputs represented exactly the
  same cohort, and the typed tiers omit source fields. Treat the numbers as a
  smoke result until a cohort manifest and retained-field accounting exist.
- **The seconds-versus-milliseconds result primarily supports compaction and
  localization.** The S3 side read hundreds of tiny remote gzip objects while
  the typed sides read a few local files. It does not isolate JSON versus Avro
  or Parquet and should not be described as a format speedup.
- **Parquet edges out Avro on both size and encode speed** in this run
  (~7% smaller, ~15% faster to encode) -- but this is one small batch
  (~1450 records) encoded once in one process; not a claim that generalizes
  without more runs at larger scale. The two are close enough that schema
  ergonomics (nested struct/record support, tooling, downstream consumers)
  is probably the bigger real-world decision driver than raw size/speed.
- The one schema-level difference DuckDB reports (`eventTime`'s timezone
  tag) is a genuine Avro-vs-Parquet typing difference, not a bug -- see
  `docs/schema-design-notes.md`.

## Open questions / next experiments

- Re-run at a larger batch size (multiple days) to see whether the
  Parquet/Avro size and encode-speed gap holds, narrows, or flips.
- The recommended stretch experiment from `PLAN.md`: republish one record
  with an added field and observe Avro's writer/reader schema resolution
  vs. Parquet's per-file footer schema needing explicit
  `unify_schemas`/`union_by_name` on read.
- GCP Cloud Mode (Tier 1 generic envelope via GCS subscription, Tier 2 on
  real Dataflow) is designed in `PLAN.md` but deliberately not built yet --
  this round of work stayed Local Mode only.
