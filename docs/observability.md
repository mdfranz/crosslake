# Observability: Logfire (Python) + OpenTelemetry (Go)

One Logfire project (`endpoint-plus-plus`), three service names, no
separate Collector. See PLAN.md ("Observability (Logfire + OpenTelemetry)")
for the original design; this doc is the as-built version.

## One-time setup

```sh
uvx logfire --region=us auth
uvx logfire --region=us projects use --org <your-org> endpoint-plus-plus
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
- **Beam breaks span parenting across the DoFn worker-thread boundary.**
  `DoFn.process()` runs in worker threads/processes where Python's ambient
  OTEL context (contextvars) does not propagate from the span opened in
  `main()`. Without an explicit fix, every `parse_record` span comes out as
  its own orphaned root trace (`parent_span_id: null`) instead of nesting
  under `run_pipeline` -- confirmed via a live Logfire query during
  development. Fix: capture a W3C `traceparent` string from the
  `run_pipeline` span *before* building the Beam graph, pass it into
  `ParseCloudTrailJson(traceparent)`, and re-attach it
  (`opentelemetry.context.attach(...)`) at the top of every `process()`
  call. See `parquet_writer/transforms.py` and `parquet_writer/pipeline.py`.
- **Cross-service trace stitching (poller -> Beam) is intentionally not
  done.** The interchange between them is a file (`data/raw.jsonl`), not a
  traced RPC, and one Beam run reads the cumulative output of many
  independent poller runs -- there's no single causal parent trace to
  attach to (many-to-one, not one-to-one). A shared correlation attribute
  (e.g. a `batch_id`) would get most of the practical value without
  fabricating a causality link that isn't real; full `traceparent`
  propagation across that boundary stays the stretch goal PLAN.md already
  called out.
