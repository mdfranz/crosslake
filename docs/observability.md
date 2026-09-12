# Observability: Logfire (Python) + OpenTelemetry (Go)

One Logfire project (`<your-project>`), three service names, no
separate Collector. See PLAN.md ("Observability (Logfire + OpenTelemetry)")
for the original design; this doc is the as-built version.

## One-time setup

```sh
uvx logfire --region=us auth
uvx logfire --region=us projects use --org <your-org> <your-project>
```

This writes `.logfire/logfire_credentials.json` at the repo root (gitignored
by its own auto-generated `.logfire/.gitignore`). It never needs to be
created more than once per machine.

**Don't** let each Python subproject auto-mint its own credentials by
running `logfire.configure()` without `LOGFIRE_TOKEN` set from a
subdirectory -- discovery only checks the current working directory, not
parent directories, so this silently creates (and scatters) a second write
token for the same project. Always `source scripts/logfire-env.sh` first.

## Day to day

```sh
source scripts/logfire-env.sh   # exports LOGFIRE_TOKEN + OTEL_EXPORTER_OTLP_*
```

This is what every Makefile target does before running anything. It reads
the token out of `.logfire/logfire_credentials.json` and never prints it.

- **Python** (`pipelines/parquet-writer`, `tools/compare`): each has a
  `telemetry.py` calling `logfire.configure(service_name=..., advanced=...)`,
  reading `LOGFIRE_TOKEN` from the environment.
- **Go** (`tools/poller`): `internal/telemetry` uses stock
  `go.opentelemetry.io/otel` + `otlptracehttp` -- no Logfire-branded Go SDK
  exists, so it's reached purely via `OTEL_EXPORTER_OTLP_ENDPOINT` /
  `OTEL_EXPORTER_OTLP_HEADERS` / `OTEL_SERVICE_NAME`.

Service names: `crosslake-poller`, `crosslake-parquet-writer`,
`crosslake-compare`.

## Gotchas hit while building this

- **The env var is `LOGFIRE_API_TOKEN`, not `LOGFIRE_TOKEN`, in this Claude
  Code environment.** That var is scoped for the Logfire MCP plugin's own
  API calls, not OTLP ingestion -- sending it as the `Authorization` header
  gets a `401 Unauthorized`. Use a project write token instead (see setup
  above), which is what `scripts/logfire-env.sh` exports as `LOGFIRE_TOKEN`.
- **Short-lived `--once` Go runs drop their last spans** if the process
  exits before `tracerProvider.Shutdown(ctx)` flushes the batch exporter --
  `cmd/poller/main.go` defers this with a fresh (non-cancelled) context.
- **A driver trace is not a parent for every Beam element.** `DoFn.process()`
  runs in worker threads/processes where the driver's ambient OTEL context does
  not propagate. An earlier workaround re-attached one captured `traceparent`
  to every record, producing high-volume spans and implying that distributed
  worker work was one local call tree. Element-level accepted/rejected counts,
  input-size distribution, and parse-duration distribution now use Beam
  metrics. Logfire gets one driver run span and a compact counter summary.
- **Cross-service trace stitching (poller -> Beam) is intentionally not
  done.** The interchange between them is a file (`data/raw.jsonl`), not a
  traced RPC, and one Beam run reads the cumulative output of many
  independent poller runs -- there's no single causal parent trace to
  attach to (many-to-one, not one-to-one). A shared correlation attribute
  (e.g. a `batch_id`) would get most of the practical value without
  fabricating a causality link that isn't real. Use a `run_id`/`cohort_id`
  correlation field plus durable reconciliation artifacts instead of forcing a
  single trace across batch and fan-out boundaries.

## Data minimization and cardinality

Telemetry is not a second copy of CloudTrail. Do not export bucket names,
object keys, account IDs, ARNs, source IPs, user agents, request/response
payloads, or raw exception strings. Use stable error categories and keep
sensitive diagnostics in local logs. Keep metric dimensions bounded; run and
cohort IDs are useful correlation fields on summaries, not metric labels.

The authoritative experiment record is a gitignored run manifest containing
cohort fingerprints, stage counts, versions, and benchmark parameters. Traces
and metrics are a searchable projection. The rationale and target signal model
are in [`review-telemetry-plan.md`](review-telemetry-plan.md).

**"Raw exception strings" means exactly that -- even the message, not just
obvious PII.** `pipelines/parquet-writer/parquet_writer/pipeline.py`'s
`_sample_rejects` surfaces *why* records were rejected without violating
this: it reads the exception *type name* only (`"JSONDecodeError"`,
`"ValueError"`, ...) out of the local `_rejects` file, discarding the
message text and the raw CloudTrail record that follow it on the same
line. `f"parse error: {e}"`-style messages count as raw exception strings
even though they look like a category label -- LEARNINGS.md item 16 has
the full story, including catching this before it shipped.
