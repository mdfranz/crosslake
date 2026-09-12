# crosslake

A hands-on comparison of Avro vs. Parquet for streaming CloudTrail logs
through a real pipeline: AWS S3 -> (GCP Pub/Sub ->) GCS/local disk. See
`PLAN.md` for the full design and `LEARNINGS.md` for results.

**Current scope: Local Mode only.** No GCP infrastructure is provisioned or
required. The Go poller reads real CloudTrail logs from S3 and writes to
local disk; the Beam pipeline runs on `DirectRunner`; the compare tool uses
DuckDB against local files and S3 directly. Remote S3 versus compacted local
files is a storage-layout observation, not an isolated format benchmark; see
the [architecture and telemetry review](docs/review-telemetry-plan.md).
Cloud Mode (GCP Pub/Sub + Dataflow, the full 3-tier design in `PLAN.md`) is
designed but not built yet.

## Quickstart

Prerequisites: Go 1.26.1+, [uv](https://docs.astral.sh/uv/) (pins Python 3.14
itself, nothing to install separately), AWS credentials with read access to
a CloudTrail S3 bucket.

```sh
# One-time: point telemetry at your own Logfire project (see docs/observability.md)
uvx logfire --region=us auth
uvx logfire --region=us projects use --org <your-org> <your-project>

# One-time: your bucket/prefix (never commit this file -- see AGENTS.md)
cp tools/poller/config.example.yaml tools/poller/config.yaml
$EDITOR tools/poller/config.yaml

make poll-once        # use a closed day prefix; see the cursor warning below
make beam-local       # ./data/raw.jsonl -> ./data/tier2-parquet/ (Parquet)
make compare-report   # Markdown + JSON evidence under ./data/reports/
```

`./data/` is gitignored; nothing it contains is ever committed.
The current last-key cursor is unsafe for continuous CloudTrail delivery, so
loop mode requires an explicit opt-in. It is retained only for controlled
experiments while a seen-object ledger is designed.

## Layout

```
tools/poller/            Go: polls S3, writes raw.jsonl + typed Avro OCF
pipelines/parquet-writer/ Python/Beam: raw.jsonl -> typed Parquet
tools/compare/            Python/DuckDB: sizes, schema diff, query + encode benchmarks
docs/                     architecture, schema design notes, observability, runbook
data/                     local outputs (gitignored)
```

## Observability

Every component ships OpenTelemetry traces to one Logfire project --
Python via the `logfire` SDK, Go via stock `go.opentelemetry.io/otel`. See
`docs/observability.md`.

## Security

This repo is public. Read `AGENTS.md` before committing anything derived
from real AWS account IDs, ARNs, or bucket names.
