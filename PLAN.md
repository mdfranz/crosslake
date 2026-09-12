# CloudTrail (S3) → Pub/Sub → GCS: Avro vs Parquet Learning Pipeline

> **2026-09-12 critical review:** the Local Mode prototype is useful but its
> current cursor and comparison methodology cannot yet support completeness or
> format-performance claims. See [`docs/review-telemetry-plan.md`](docs/review-telemetry-plan.md)
> for evidence, code-level risks, telemetry design, and revised exit criteria.

## Status (as of first Local Mode build)

**Built and verified end-to-end: Local Mode only.** Real AWS S3 CloudTrail
records flow through `tools/poller` (Go) → `./data/raw.jsonl` +
`./data/tier3-avro/events.avro` → `pipelines/parquet-writer` (Beam,
DirectRunner) → `./data/tier2-parquet/*.parquet` → `tools/compare` (DuckDB).
All three components ship OpenTelemetry traces to one Logfire project. See
`LEARNINGS.md` for real results and bugs found/fixed along the way, and
`docs/runbook.md` for the exact commands.

**Not built yet:** everything GCP (Pulumi stacks, Pub/Sub topics/schema,
GCS subscriptions, Dataflow) -- Cloud Mode below is still the target design
for that half, just deferred. A few specifics changed from the original
design once real data and real tools were involved; where this doc still
describes the original intent, `LEARNINGS.md` and `docs/` note what
actually happened:

- **Python**: `>=3.14`, managed with `uv` (not raw `pip`/`venv`) -- picked
  when the environment turned out to already have `uv` and no system `pip`.
  Apache Beam 2.76 and DirectRunner both work fine on 3.14, confirmed before
  committing to it.
- **Observability token**: this environment's `LOGFIRE_API_TOKEN` is scoped
  for the Logfire Claude Code plugin's own API calls, not OTLP ingestion --
  a real project write token (`uvx logfire --region=us auth` +
  `projects use`) is required instead. See `docs/observability.md`.
- **Avro/Parquet codec**: both `hamba/avro/v2/ocf` and Beam's
  `WriteToParquet` default to *no* compression, which silently makes any
  "vs. gzip" size comparison look wrong. Both are explicitly set to
  deflate/gzip. DuckDB's `avro` extension additionally can't read
  `zstandard`-codec OCF files, ruling that codec out for Tier 3 as long as
  DuckDB is a consumer.
- **Beam telemetry**: per-record remote spans were removed. Worker-side counts
  and distributions use Beam metrics; Logfire receives one driver span and a
  compact run summary. See `docs/observability.md`.

## Context

The user wants hands-on understanding of stream processing across object storage,
specifically the practical difference between Avro and Parquet, using a real
pipeline: existing AWS CloudTrail logs in S3 → Google Cloud Pub/Sub → GCS.

The user already has direct experience with GCP's Cloud Logging → Pub/Sub export
(feeding a Dataflow "Pub/Sub to Avro Files on Cloud Storage" template) on a
different project. Research during planning confirmed that pattern produces only a
**generic envelope** Avro (`message: bytes`, `attributes: map`, `timestamp: long`) —
no Pub/Sub Schema, no parsing of the JSON payload, so the "Avro-ness" is shallow.
That became the key design driver: the plan below treats that familiar pattern as
a baseline/control tier, and builds two additional tiers that produce genuinely
structured, typed output — one in Avro, one in Parquet — so the user gets a real
apples-to-apples comparison rather than just re-deriving what they already know.

This repository began as a from-scratch scaffold. Scope is an explicit
**learning prototype**, but ingestion completeness, cohort reconciliation, and
repeatable measurement are prerequisites for valid learnings rather than
optional production hardening.

## Locked-in design decisions (from conversation)

- **IaC**: Pulumi, TypeScript, both AWS and GCP.
- **Bridge**: a custom poller/pusher tool (not Storage Transfer Service, not an
  AWS-side Lambda push) — runs locally first, written so it can move to GCP
  (Cloud Run Job) or AWS (Lambda/ECS) later with minimal change.
- **Dual-publish**: every CloudTrail record is published to two Pub/Sub topics:
  - `cloudtrail-raw` — no schema, raw JSON bytes (mirrors the user's Cloud
    Logging pattern).
  - `cloudtrail-avro` — a Pub/Sub Schema (Avro) attached; poller Avro-encodes
    each record client-side before publishing.
- **Dual operating modes (Cloud Mode & Local Mode)**:
  - **Source is always AWS S3**: In both modes, the Go poller connects to real
    AWS S3 CloudTrail logs via `s3source`, maintaining cursor state and
    decompressing gzipped JSON records.
  - **Cloud Mode**: Poller sinks to GCP Pub/Sub topics (`cloudtrail-raw`,
    `cloudtrail-avro`); GCS subscriptions write Tier 1 (envelope) and Tier 3
    (structured Avro); Dataflow runs the Beam pipeline to write Tier 2
    (structured Parquet) to GCS.
  - **Local Mode (Pub/Sub & Dataflow bypass)**: Poller sinks to local disk
    (`internal/disksink`):
    - Appends raw JSON lines to `./data/raw.jsonl` (mirrors `cloudtrail-raw`).
    - Writes typed Avro container files to `./data/tier3-avro/events.avro`
      using `hamba/avro/v2/ocf` (mirrors the Tier 3 GCS subscription).
    - Runs the Beam pipeline locally on `DirectRunner` against `./data/raw.jsonl`
      to write `./data/tier2-parquet/` (mirrors Dataflow Tier 2).
    - Queries S3 directly as baseline and `./data/*` for Parquet/Avro via
      DuckDB without incurring GCP Pub/Sub or Dataflow worker costs.
  - **Code reuse (>85%)**: S3 fetcher, cursor logic, `cloudtrail.avsc`, Avro
    encoder, Beam parsing/transforms, and DuckDB SQL statements are 100%
    reused between modes; only the sink and Beam runner flags differ.
- **Three comparison tiers** landing in GCS (or `./data/` in local mode):
  - **Tier 1 (baseline/control)**, off `cloudtrail-raw`: native GCS subscription
    (or local envelope OCF), generic envelope output.
  - **Tier 2 (structured Parquet)**, off `cloudtrail-raw`: custom Apache Beam
    (Python SDK) pipeline (DataflowRunner in cloud, DirectRunner in local),
    parses JSON, writes typed columnar Parquet.
  - **Tier 3 (structured Avro)**, off `cloudtrail-avro`: native GCS subscription
    in Avro mode (or local Go `hamba/avro/v2/ocf` sink), typed fields, no
    Beam/Dataflow needed.
- **Dataflow, not Dataproc**, for the Beam pipeline: Dataflow is Beam's native
  managed runner with first-class Pub/Sub streaming support; Dataproc is
  Spark-oriented and would mean rewriting as Spark Structured Streaming — noted
  as a legitimate stretch-goal alternative, not built now.
- **Language split & DuckDB strategy**:
  - **Go** for the poller (portable single binary, mature AWS SDK v2 + Pub/Sub
    client + `hamba/avro`).
  - **Python** for the Beam pipeline on Dataflow (mature ParquetIO and PyArrow
    schema support in streaming).
  - **Python + DuckDB CLI** for comparison and analysis (`tools/compare`).
    DuckDB can query both sides directly over the network: raw AWS CloudTrail
    gzipped JSON in S3 (`read_json` + `unnest(Records)` via the `aws` / `httpfs`
    extensions) and GCS destination files (`read_parquet` for Tier 2, and
    `read_avro` via DuckDB's official core `avro` extension for Tier 3). This
    enables the exact same SQL queries across S3 JSON, GCS Parquet, and GCS
    Avro without needing separate Python-only engines like `fastavro` for query
    benchmarks.
  - *Embedded DuckDB in Go note*: Embedded DuckDB in Go (`duckdb-go/v2`) is
    viable via CGO and runs all cloud extensions (`httpfs`, `aws`, `avro`),
    making a standalone Go verification binary possible; however, Python and the
    DuckDB CLI remain the primary exploratory choice for `tools/compare` due to
    dynamic row scanning, DataFrame/Arrow export, and tabular terminal formatting.
- **Auth for now**: local dev auth only — gcloud ADC on GCP side, Pulumi-created
  scoped IAM user on AWS side. For DuckDB GCS access, a GCS HMAC key is used
  (`CREATE SECRET (TYPE gcs, ...)`), while S3 uses standard AWS credentials
  (`CREATE SECRET (TYPE s3, PROVIDER credential_chain)`). Workload Identity
  Federation (AWS→GCP) is documented as the upgrade path before this runs
  unattended, not built now.
- **Observability: Logfire (Python) + OpenTelemetry (Go), one destination**:
  `LOGFIRE_TOKEN` is already set in the environment, so every component ships
  traces (and, where cheap, metrics) to the same Logfire project from day one —
  this doubles as a fourth, cross-cutting comparison lens (per-tier publish/
  write/query latency, side by side, without hand-rolled timing code).
  - **Python components** (`pipelines/parquet-writer`, `tools/compare`) use the
    `logfire` SDK directly: `pip install logfire`, `logfire.configure()` reads
    `LOGFIRE_TOKEN` from the environment with zero extra config. Beam DoFns get
    manual spans/counters around parse success/failure; `tools/compare` wraps
    each DuckDB query (S3 baseline, Tier 2 Parquet, Tier 3 Avro) in a
    `with logfire.span(...)` so the three tiers' query latencies land as
    directly comparable traces in the same project.
  - **Go poller** uses vanilla `go.opentelemetry.io/otel` (no Logfire-specific
    Go package exists) exporting OTLP/HTTP straight to Logfire's OTLP endpoint
    — Logfire ingests standard OTLP, so no local Collector is needed. Three
    env vars point the SDK at it: `OTEL_EXPORTER_OTLP_ENDPOINT=https://logfire-us.pydantic.dev`,
    `OTEL_EXPORTER_OTLP_HEADERS=Authorization=$LOGFIRE_TOKEN`,
    `OTEL_SERVICE_NAME=crosslake-poller` (use `logfire-eu.pydantic.dev` instead
    if the project is EU-region). Spans: one per `RunOnce` pass, one per S3
    object fetched/gunzipped, one per record published (tagged `sink=raw` /
    `sink=avro` and `mode=local` / `mode=pubsub` so cloud vs. local runs are
    filterable in the same view).
  - Each component sets a distinct `OTEL_SERVICE_NAME`/`service_name`
    (`crosslake-poller`, `crosslake-parquet-writer`, `crosslake-compare`) so
    the three tiers stay distinguishable in one Logfire project rather than
    needing three separate projects.

## Repo layout

```
crosslake/
  README.md, LEARNINGS.md (living comparison notes), Makefile, .gitignore
  .env.example                 # LOGFIRE_TOKEN, OTEL_EXPORTER_OTLP_*, OTEL_SERVICE_NAME
  docs/
    architecture.md            # diagram + prose of the 3-tier design
    schema-design-notes.md     # source of truth for CloudTrail field typing
    observability.md           # Logfire/OTEL setup, service names, dashboards/saved queries
    runbook.md                 # verification steps (mirrors "Build order" below)
  infra/pulumi/
    gcp/   index.ts, pubsub.ts, gcs.ts, iam.ts, apis.ts
    aws/   index.ts, iam.ts
  tools/
    poller/                    # Go
      cmd/poller/main.go
      internal/s3source/       # ListObjectsV2: full-prefix List + legacy ListSince (AWS S3 source)
      internal/pubsubsink/     # Cloud sink: dual publish to GCP Pub/Sub (raw + avro)
      internal/disksink/       # Local sink: writes raw.jsonl + typed Avro OCF
      internal/avroenc/        # hamba/avro against schema/cloudtrail.avsc
      internal/cursor/         # legacy last-key cursor file (unsafe loop mode only)
      internal/ledger/         # seen-object set (bucket,key,etag) backing --once/--reconcile
      internal/atomicfile/     # shared atomic JSON read/write, used by cursor + ledger
      internal/telemetry/      # OTEL TracerProvider setup (OTLP/HTTP → Logfire), span helpers
      schema/cloudtrail.avsc
      config.example.yaml
    compare/                   # Python + DuckDB: sizes.py, schema_inspect.py, query_bench.py, queries.sql, report.py, telemetry.py (logfire.configure())
  pipelines/parquet-writer/    # Python/Beam
    parquet_writer/pipeline.py, transforms.py, cloudtrail_schema.py, telemetry.py (logfire.configure())
    scripts/run_local.sh (DirectRunner), run_dataflow.sh (DataflowRunner)
  data/                        # Local mode outputs (.gitignored)
    raw.jsonl                  # Mirrored raw topic stream
    tier1-envelope/            # Local generic envelope Avro
    tier2-parquet/             # Local DirectRunner Parquet output
    tier3-avro/                # Local typed Avro (via hamba/avro/v2/ocf)
```

Judgment calls: one Pulumi TS project per cloud (two stacks, not one program
with both providers — different credentials/cadences, no cross-stack coupling
needed yet); one GCS bucket with three prefixes (`tier1-envelope/`,
`tier2-parquet/`, `tier3-avro/`, `dataflow/{staging,temp}/`) rather than three
buckets, for a simpler IAM surface.

## GCP Pulumi resources (`infra/pulumi/gcp`)

- `gcp.projects.Service`: `pubsub`, `dataflow`, `storage` APIs.
- `gcp.pubsub.Schema` (`AVRO`, definition read from
  `tools/poller/schema/cloudtrail.avsc` at synth time — Go and Pulumi share
  one schema file, no drift).
- `gcp.pubsub.Topic` × 2: `cloudtrail-raw` (no schema), `cloudtrail-avro`
  (`schemaSettings` → the schema above, `BINARY` encoding).
- `gcp.storage.Bucket` `crosslake-cloudtrail-lake` (uniform bucket-level access,
  single region e.g. `us-central1`).
- `gcp.pubsub.Subscription` × 3:
  - `cloudtrail-tier1-gcs` (on raw topic, GCS config, `outputFormat: AVRO`,
    `avroConfig.useTopicSchema: false`, `avroConfig.writeMetadata: true` →
    `tier1-envelope/`)
  - `cloudtrail-tier3-gcs` (on avro topic, GCS config, `outputFormat: AVRO`,
    `avroConfig.useTopicSchema: true`, `avroConfig.writeMetadata: false` —
    explicitly disable writeMetadata to prevent injecting Pub/Sub metadata
    fields into root record, preventing schema collision and guaranteeing clean
    typed fields matching Tier 2 Parquet → `tier3-avro/`)
  - `cloudtrail-tier2-beam-pull` (plain pull subscription on raw topic, consumed
    by the Beam pipeline)
- Service accounts + IAM: `poller-sa` (topic-scoped `pubsub.publisher` on both
  topics); `dataflow-worker-sa` (`pubsub.subscriber` on the tier2 pull
  subscription, `storage.objectAdmin` on the lake bucket, project-level
  `dataflow.worker`).
- **Critical/easy-to-miss**: `gcp.projects.ServiceIdentity` for
  `pubsub.googleapis.com` to resolve the Google-managed Pub/Sub service agent,
  then grant it `storage.objectAdmin` on the lake bucket — without this, Tier 1
  and Tier 3 GCS subscriptions silently fail to write anything.

## AWS Pulumi resources (`infra/pulumi/aws`)

- `aws.s3.getBucket` — data source referencing the existing CloudTrail bucket
  (name via Pulumi config); no bucket or CloudTrail trail is created.
- `aws.iam.User` + `aws.iam.AccessKey` + scoped policy: `s3:ListBucket` (with an
  `s3:prefix` condition) and `s3:GetObject` limited to the CloudTrail prefix.
- Policy document written as a reusable function so it can be reattached to an
  `aws.iam.Role` later (Lambda/ECS) with no logic change.

## Poller (Go, `tools/poller`)

- **Source & Sink abstraction**:
  - **Source is always AWS S3**: `internal/s3source` exposes both `List`
    (full prefix listing, no `StartAfter`) and the legacy `ListSince`
    (`StartAfter`-based). The original assumption that CloudTrail delivery
    follows key order is false. The supported `--once` path now uses
    `internal/ledger` (a seen-object set keyed by `bucket, key, etag`) via
    `List` instead of a lexicographic boundary — see `LEARNINGS.md` #20 and
    `docs/review-telemetry-plan.md`. Continuous loop mode still uses the old
    `ListSince`/cursor and still requires an explicit unsafe opt-in: the
    ledger fixes replay/re-run safety for a closed prefix, not completeness
    against a still-growing one, which needs event notifications plus a
    reconciliation cadence (see "Explicit future work" below). `--reconcile`
    re-lists a closed prefix and reports gaps against the ledger read-only.
  - **Gotcha**: each S3 object is gzip JSON of `{"Records": [...]}` — gunzip,
    then process each *individual record*, not the compressed blob or array.
  - **Pluggable Sink interface (`internal/sink`)**:
    ```go
    type Sink interface {
        WriteRecord(ctx context.Context, rawJSON []byte, avroRecord any) error
        Close() error
    }
    ```
    - **Cloud Mode (`internal/pubsubsink`)**: `cloudtrail-raw` gets verbatim JSON
      bytes; `cloudtrail-avro` gets `hamba/avro`-encoded binary bytes against
      `schema/cloudtrail.avsc`.
    - **Local Mode (`internal/disksink`)**: appends verbatim JSON lines to
      `./data/raw.jsonl` (for the local Beam runner) and writes typed Avro
      container files to `./data/tier3-avro/events.avro` using Go's
      `github.com/hamba/avro/v2/ocf` (Object Container File).
    - Flag switch: `poller --mode=local|pubsub` (local mode lets you test the S3
      pull and Avro encoding without any GCP infra).
- **Gotcha**: the avro topic's Pub/Sub Schema validates server-side at publish
  time; the Go client won't catch mismatches beforehand — running in local mode
  first against real S3 records validates `cloudtrail.avsc` serialization early.
- Portability seam: core logic in `RunOnce(ctx) error` (one poll→fetch→parse→
  publish→cursor-update pass); `main.go` wraps it in a ticker loop locally.
  Same function becomes a Cloud Run Job/Lambda handler later.
- **Telemetry (`internal/telemetry`)**: `otel.SetTracerProvider` wired to an
  `otlptracehttp` exporter reading `OTEL_EXPORTER_OTLP_ENDPOINT` /
  `OTEL_EXPORTER_OTLP_HEADERS` / `OTEL_SERVICE_NAME` from the environment
  (set via `.env`/Makefile, pointed at Logfire — see Observability section
  below). `RunOnce` opens a root span and each S3 object gets a child span with
  aggregate bytes/record-count/duration attributes. Raw keys, raw errors, and
  per-record spans are not exported. **Gotcha**: `--once`
  runs are short-lived, so `main.go` must call `tracerProvider.Shutdown(ctx)`
  (or `ForceFlush`) before exit or the batched exporter drops the last spans.
- **CloudTrail schema shape** (central design challenge, called out
  explicitly): typed fields for the stable envelope (`eventVersion`,
  `eventTime`, `eventSource`, `eventName`, `awsRegion`, `sourceIPAddress`,
  `userAgent`, `requestID`, `eventID`, `eventType`, `recipientAccountId`,
  nested `userIdentity`); the event-type-dependent nested blobs
  (`requestParameters`, `responseElements`, `additionalEventData`,
  `serviceEventDetails`) are stored as JSON-string columns rather than
  strict per-service records, since their shape varies by event type. Same
  approach mirrored in the Parquet schema (§ below) for a fair comparison.

## Tier 2 Beam pipeline (Python, `pipelines/parquet-writer`)

`ReadFromPubSub` → `ParDo(ParseCloudTrailJson())` (tagged bad-record output →
plain-text `_rejects/` prefix, not full DLQ) → `WindowInto(FixedWindows(60))`
(crucial: Beam file sinks require windowing on unbounded streaming sources to
flush files to GCS) → `WriteToParquet` with an explicit `pyarrow.schema()`
mirroring the Avro field list. `userIdentity` kept as a **nested struct column**
(a deliberate point of comparison — Parquet's native nested-column support vs.
the Avro record). Dual input mode (`--input_mode=file|pubsub`) lets pipeline
logic be iterated for free against a saved `.jsonl` sample on `DirectRunner`
before touching Dataflow/billing. `telemetry.py` calls `logfire.configure()`
once at pipeline start. `ParseCloudTrailJson` uses Beam runner-native counters
and distributions for accepted/rejected records, input sizes, and parse time;
Logfire receives one driver span and one compact run summary.

Dataflow job itself is **not** a Pulumi-managed resource (that resource type
targets template artifacts; a custom Beam pipeline would need a Flex Template
build for no real payoff here). Pulumi provisions only the supporting infra;
the pipeline is launched imperatively via `scripts/run_dataflow.sh`, run for a
short bounded window, then explicitly cancelled (`gcloud dataflow jobs
cancel`) — streaming Dataflow bills continuously.

## Observability (Logfire + OpenTelemetry)

One Logfire project, three service names, no separate Collector:

- **Setup**: `LOGFIRE_TOKEN` is already exported in the shell environment
  (confirm region: US default is `https://logfire-us.pydantic.dev`; use
  `https://logfire-eu.pydantic.dev` if the project was created in the EU).
  `.env.example` documents the full variable set so both the Go and Python
  sides pick it up the same way locally and in `make` targets:
  ```
  LOGFIRE_TOKEN=...
  OTEL_EXPORTER_OTLP_ENDPOINT=https://logfire-us.pydantic.dev
  OTEL_EXPORTER_OTLP_HEADERS=Authorization=${LOGFIRE_TOKEN}
  OTEL_SERVICE_NAME=crosslake-poller
  ```
- **Python (`pipelines/parquet-writer`, `tools/compare`)**:
  `logfire.configure()` picks up `LOGFIRE_TOKEN` with no other
  arguments (pass `service_name=` explicitly per component). Beam worker
  signals use Beam metrics; comparison iterations use bounded Logfire spans.
  `report.py` writes the complete machine-readable evidence under the
  gitignored `data/reports/` tree and exports only a compact summary.
- **Go (`tools/poller`)**: no Logfire-branded Go SDK exists (Logfire's Go
  guidance is "bring your own OpenTelemetry SDK"), so `internal/telemetry`
  uses stock `go.opentelemetry.io/otel` + `otlptracehttp`, pointed at Logfire
  purely via the three `OTEL_EXPORTER_OTLP_*`/`OTEL_SERVICE_NAME` env vars —
  no code-level Logfire dependency at all.
- **Cross-cutting payoff**: use `run_id` and `cohort_id` to correlate fan-out
  stages. Do not fabricate a single parent/child trace across many-to-one batch
  boundaries; stage traces and durable reconciliation artifacts preserve the
  actual execution model.
- **Verification**: after each smoke test (local and cloud), check the
  Logfire live view for the corresponding `service_name` and confirm spans
  landed with the expected `mode`/`sink`/`tier` attributes before moving on.

## Build / verification order

1. Scaffold directories + stub manifests (`go.mod`, `pyproject.toml`,
   `package.json`), `.gitignore` (including `/data/`), README/LEARNINGS skeletons,
   `.env.example` (`LOGFIRE_TOKEN`, `OTEL_EXPORTER_OTLP_*`, `OTEL_SERVICE_NAME`).
2. `pulumi up` in `infra/pulumi/aws` → verify credentials can read the CloudTrail
   S3 prefix and cannot touch other buckets/prefixes.
3. **Local Mode End-to-End Smoke Test (Zero GCP cost / instant feedback)**:
   - Run Go poller against real S3 with `--mode=local --once`:
     `go run ./tools/poller/cmd/poller --mode=local --once`
   - Confirm it polls S3, gunzips records, and writes `./data/raw.jsonl` and
     typed `./data/tier3-avro/events.avro` via `hamba/avro/v2/ocf`.
   - Run Beam pipeline locally on `DirectRunner`:
     `python3 -m parquet_writer.pipeline --runner=DirectRunner --input_mode=file --input_file=./data/raw.jsonl --output_prefix=./data/tier2-parquet/events`
   - Run DuckDB queries locally to compare:
     - S3 direct baseline: `SELECT unnest(Records)... FROM read_json('s3://...')`
     - Local Tier 2: `SELECT ... FROM read_parquet('./data/tier2-parquet/*.parquet')`
     - Local Tier 3: `SELECT ... FROM read_avro('./data/tier3-avro/*.avro')`
   - *Payoff*: Validate all schema mappings, gunzip unnesting, and Parquet/Avro
     encoding in seconds before spinning up any GCP infrastructure.
   - Check the Logfire live view: `crosslake-poller` spans from the `--once`
     run, and `crosslake-parquet-writer` spans from the `DirectRunner` pass —
     confirms OTLP export and `Shutdown()`/flush both work before cloud costs
     start accruing.
4. `pulumi up` in `infra/pulumi/gcp` → provision GCP Pub/Sub topics, schemas,
   bucket, and subscriptions. Verify with `gcloud pubsub topics list`, `gsutil ls`.
5. `make poller-config` — generate poller config from `pulumi stack output
   --json` + AWS profile.
6. Cloud smoke test: run poller with `--mode=pubsub --once` against S3; pull from
   a temporary debug subscription and verify both topics receive valid messages.
7. Confirm Tier 1 and Tier 3 GCS subscriptions wrote files for that message
   (`gsutil ls`); open the Tier 3 file and confirm real typed fields, not the
   generic envelope.
8. Switch Beam pipeline to `DataflowRunner` against the real pull subscription,
   run briefly, cancel the job, confirm Parquet output in GCS.
9. Capture a closed, bounded source cohort in a manifest and require all three
   tiers to reconcile to its object and event fingerprints. Do not use the
   current `last_key` loop for this step.
10. Run `tools/compare`: an explicitly labelled size inventory, a side-by-side schema dump,
    and query benchmarks using DuckDB across S3 and GCS:
    - S3 direct baseline: `SELECT unnest(Records)... FROM read_json('s3://...')`
    - Tier 2: `SELECT ... FROM read_parquet('gs://.../tier2-parquet/*/*.parquet')`
    - Tier 3: `SELECT ... FROM read_avro('gs://.../tier3-avro/*.avro')` via DuckDB's
      core `avro` extension.
    Write results into `LEARNINGS.md`. Recommended concrete experiment:
    republish one record with an added field and observe how each format's
    readers handle it — Avro's writer/reader schema resolution vs. Parquet's
    per-file footer schema needing explicit schema-unification on read (PyArrow
    `unify_schemas` / DuckDB `union_by_name`). Pull the per-tier query-latency
    spans back from Logfire (`crosslake-compare` service) to corroborate the
    manual timings in `LEARNINGS.md`.
11. `pulumi destroy` (or at minimum cancel the Dataflow job) between sessions —
    ongoing cost surface is Dataflow workers, not idle Pub/Sub/GCS.

## Risks / gotchas specific to this design

- Missing IAM grant to the Pub/Sub-managed service agent → Tier 1/3 silently
  write nothing (no obvious error).
- Tier 3 GCS subscription schema collision: when `useTopicSchema: true`, set
  `writeMetadata: false`. If `writeMetadata: true` is enabled, Pub/Sub forcibly
  adds `subscription_name`, `message_id`, `publish_time`, and `attributes` to
  the root record, which fails if your schema contains identical field names.
- Beam streaming file sink requires windowing: writing Parquet from an unbounded
  source (`ReadFromPubSub`) without an explicit window (e.g. `FixedWindows`)
  causes Beam to throw an error or never close and flush files to GCS.
- DuckDB GCS authentication: DuckDB's `httpfs` accesses GCS via the S3-compatible
  XML API, which requires GCS HMAC keys (`CREATE SECRET (TYPE gcs, ...)`),
  whereas S3 can use standard credential chains (`TYPE s3, PROVIDER credential_chain`).
- Dataflow worker SA needs *subscription-scoped* `pubsub.subscriber`, not just
  topic-level.
- CloudTrail's per-event-type field variability is the central schema-design
  challenge — addressed via JSON-string escape-hatch columns in both formats.
- Avro schema validation on the avro topic is server-side; check publish
  errors explicitly, don't assume success.
- gzip fan-out bug: must publish per-record, not per-object/array.
- Cursor file has no locking — fine solo, but never run two poller instances
  concurrently.
- Streaming Dataflow bills continuously — remember to cancel test runs.
- OTEL Go SDK batches spans in the background; a short-lived `--once` process
  that exits without `tracerProvider.Shutdown(ctx)` silently drops unflushed
  spans (no error) — easy to mistake for "the poller ran fine, telemetry is
  just broken."
- Missing/wrong `OTEL_SERVICE_NAME` shows spans as `unknown_service` in
  Logfire, making the three components indistinguishable in one project.
- US vs. EU Logfire region is endpoint-selected (`logfire-us` vs.
  `logfire-eu.pydantic.dev`); pointing at the wrong region silently sends data
  nowhere useful rather than erroring.

## Explicit future work (not built now)

HA/monitoring/DLQ hardening; S3 event notifications + SQS instead of polling
once the poller leaves the local machine; Workload Identity Federation
replacing static AWS IAM user keys / any GCP SA key file; poller as a Cloud Run
Job or Lambda/ECS task (reusing `RunOnce`); Dataflow Flex Template packaging if
this needs to become repeatable/scheduled; prefix-scoped GCS IAM via IAM
Conditions instead of bucket-level `storage.objectAdmin`; Dataproc/Spark
Structured Streaming as an alternative Tier 2 implementation, for comparison.

## Critical files to create first

- `infra/pulumi/gcp/pubsub.ts`, `infra/pulumi/gcp/iam.ts`
- `tools/poller/schema/cloudtrail.avsc`, `tools/poller/cmd/poller/main.go`
- `tools/poller/internal/telemetry/telemetry.go`
- `pipelines/parquet-writer/parquet_writer/pipeline.py`
- `tools/compare/compare/sizes.py`
- `.env.example`
