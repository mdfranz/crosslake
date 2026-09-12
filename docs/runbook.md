# Runbook (Local Mode)

Verification steps actually run and confirmed working while building this.
All commands assume the repo root unless noted, and all `make` targets
already source `scripts/logfire-env.sh` for you.

## One-time setup

```sh
uvx logfire --region=us auth
uvx logfire --region=us projects use --org <your-org> <your-project>

cp tools/poller/config.example.yaml tools/poller/config.yaml
$EDITOR tools/poller/config.yaml   # set aws.s3_bucket / aws.s3_prefix -- never commit this file
```

## 1. Poll real CloudTrail into local disk

```sh
make poll-once
```

Confirms: S3 `ListObjectsV2` + gunzip works, `./data/raw.jsonl` grows,
`./data/tier3-avro/events.avro` grows (appends across runs -- see
`LEARNINGS.md` bug #1), the ledger (`tools/poller/ledger.json`, gitignored)
records every object committed this run. `--once` re-lists the whole
configured prefix every run and diffs against the ledger (keyed by
`bucket, key, etag`) rather than a lexicographic boundary -- see
`internal/ledger`'s package doc for the real inventory data
(`docs/review-telemetry-plan.md`) showing why a `last_key` cursor can
permanently skip out-of-order CloudTrail deliveries. This makes `--once`
safe to rerun against the same closed prefix (already-committed objects are
skipped, nothing is reprocessed or lost), but it should still be a closed,
day-scoped prefix -- re-listing a still-growing prefix can't skip objects,
but can't prove completeness at a given instant either.

Loop mode is blocked by default and unchanged by the ledger: it still uses
the legacy last-key cursor (`tools/poller/cursor.json`), since making
continuous polling of a *growing* prefix both safe and cheap needs S3 event
notifications/SQS plus a reconciliation cadence, not just the ledger (see
PLAN.md "Explicit future work"). `make poll-loop` explicitly acknowledges the
known unsafe cursor and exists only for controlled experiments; do not use it
for completeness-sensitive ingestion.

To check a closed prefix for gaps without processing anything (e.g. to catch
a late delivery that arrived after an earlier `--once` run already committed
past where it would have sorted), run in read-only reconcile mode:

```sh
cd tools/poller
go run ./cmd/poller --mode=local --reconcile \
  --s3-prefix 'AWSLogs/<account-id>/CloudTrail/us-east-1/YYYY/MM/DD/' \
  --ledger-file './ledger-YYYY-MM-DD.json'
```

It exits non-zero and lists each missing key if the ledger doesn't yet cover
every object S3 has; rerun `--once` (below) to pick those up.

**Never delete a ledger file for a prefix that's already been ingested into
a `data_dir` you're keeping.** The ledger only prevents *its own* re-ingest;
`raw.jsonl`/`tier3-avro/events.avro` are append-only and have no memory of
their own. A missing ledger makes every object look new again, so `--once`
silently re-appends records already durably written -- hit for real while
building this (see `LEARNINGS.md`), caught only because
`compare.manifest`'s record-count check flagged `tier3_avro`'s row count
disagreeing with the manifest and every other tier. If a ledger is lost,
either restore it from backup or wipe and re-ingest that prefix's `data_dir`
output cleanly rather than re-running `--once` against the old one.

For a closed-prefix backfill, override the prefix and ledger together so the
experiment cannot touch the configured live ledger (`--once`/`--reconcile`
are ledger-only, so `--cursor-file` isn't needed here -- it's only required
alongside `--s3-prefix` for loop mode):

```sh
cd tools/poller
go run ./cmd/poller --mode=local --once \
  --s3-prefix 'AWSLogs/<account-id>/CloudTrail/us-east-1/YYYY/MM/DD/' \
  --ledger-file './ledger-YYYY-MM-DD.json'
```

This also writes a durable manifest next to the ledger
(`ledger-YYYY-MM-DD.manifest.json` -- see `internal/manifest`), recording
the object count, compressed bytes, and an order-independent
`(key, etag)` fingerprint for everything committed so far. `tools/compare`
cross-checks its live numbers against this file (`compare/manifest.py`) --
list every manifest for a multi-prefix cohort under `manifest_files:` in
`config.yaml` (see `config.example.yaml`). Check it standalone with:

```sh
make compare-manifest
```

Sanity-check Tier 3 directly:

```sh
cd tools/compare && uv run python3 -c "
import duckdb
con = duckdb.connect()
print(con.execute(\"SELECT eventName, eventTime, userIdentity FROM read_avro('../../data/tier3-avro/events.avro') LIMIT 3\").fetchall())
"
```

`eventTime` should print as a real `datetime`, not a string -- confirms
Tier 3 isn't the generic-envelope shape.

## 2. Beam DirectRunner -> Tier 2 Parquet

```sh
make beam-local
```

Confirms: `./data/tier2-parquet/events-*.parquet` is written, rejects file
(`events_rejects-*.txt`) is empty for well-formed CloudTrail data.

## 3. Compare

```sh
make compare-sizes    # unreconciled remote/local size inventory
make compare-schema   # side-by-side Tier 2 vs Tier 3 column types
make compare-bench    # warmups + 5 randomized trials; validates result hashes
make compare-encode   # repeated/randomized same-process encode benchmark
make compare-manifest # live numbers vs the poller's durable manifest(s); fails closed on mismatch
make compare-report   # Markdown + gitignored JSON evidence under data/reports/ -- includes the manifest check, fails closed if it doesn't pass
```

## 4. Confirm telemetry

Each component run should show up in Logfire under its service name:
`crosslake-poller`, `crosslake-parquet-writer`, `crosslake-compare`. See
`docs/observability.md`. Beam element counts and duration distributions are
runner-native metrics; Logfire intentionally receives only a run span and
summary rather than one span per CloudTrail record.

## Resetting to a clean run

```sh
rm -f tools/poller/cursor.json tools/poller/ledger*.json tools/poller/*.manifest.json
rm -rf data/
```

Then repeat from step 1. `s3_prefix` in `config.yaml` bounds how much S3
history a fresh run pulls -- narrow it to a single day for a fast smoke
test.
