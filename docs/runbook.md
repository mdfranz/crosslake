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
`LEARNINGS.md` bug #1), cursor advances (`tools/poller/cursor.json`,
gitignored). Restrict this command to a closed, day-scoped prefix. Continuous
or repeated polling with the current `last_key` cursor can miss late CloudTrail
deliveries; see `docs/review-telemetry-plan.md`.

Loop mode is blocked by default. `make poll-loop` explicitly acknowledges the
known unsafe cursor and exists only for controlled experiments; do not use it
for completeness-sensitive ingestion.

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
make compare-report   # Markdown + gitignored JSON evidence under data/reports/
```

## 4. Confirm telemetry

Each component run should show up in Logfire under its service name:
`crosslake-poller`, `crosslake-parquet-writer`, `crosslake-compare`. See
`docs/observability.md`. Beam element counts and duration distributions are
runner-native metrics; Logfire intentionally receives only a run span and
summary rather than one span per CloudTrail record.

## Resetting to a clean run

```sh
rm -f tools/poller/cursor.json
rm -rf data/
```

Then repeat from step 1. `s3_prefix` in `config.yaml` bounds how much S3
history a fresh run pulls -- narrow it to a single day for a fast smoke
test.
