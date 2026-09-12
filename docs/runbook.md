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
gitignored). Re-running with no new S3 objects should report `0 record(s)`.

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
make compare-sizes    # file sizes vs. the original gzipped S3 JSON
make compare-schema   # side-by-side Tier 2 vs Tier 3 column types
make compare-bench    # identical SQL timed across S3 JSON / Parquet / Avro
make compare-encode   # same-process Avro (fastavro) vs Parquet (pyarrow) encode time
make compare-report   # all of the above, rendered as Markdown for LEARNINGS.md
```

## 4. Confirm telemetry

Each of the above should show up in Logfire under its service name:
`crosslake-poller`, `crosslake-parquet-writer`, `crosslake-compare`. See
`docs/observability.md` for the query used to verify span parenting works
correctly (a real bug was caught and fixed this way -- see `LEARNINGS.md`).

## Resetting to a clean run

```sh
rm -f tools/poller/cursor.json
rm -rf data/
```

Then repeat from step 1. `s3_prefix` in `config.yaml` bounds how much S3
history a fresh run pulls -- narrow it to a single day for a fast smoke
test.
