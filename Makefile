# crosslake -- Local Mode targets only (no GCP; see PLAN.md).
# Every target sources scripts/logfire-env.sh so telemetry reaches Logfire
# without printing the write token.

SHELL := /bin/bash

.PHONY: poll-once poll-loop beam-local compare-sizes compare-schema compare-bench compare-encode compare-report go-build go-test

poll-once:
	cd tools/poller && \
	. ../../scripts/logfire-env.sh && \
	go run ./cmd/poller --mode=local --once

poll-loop:
	cd tools/poller && \
	. ../../scripts/logfire-env.sh && \
	go run ./cmd/poller --mode=local

beam-local:
	cd pipelines/parquet-writer && \
	. ../../scripts/logfire-env.sh && \
	uv run python3 -m parquet_writer.pipeline \
		--runner=DirectRunner \
		--input_mode=file \
		--input_file=../../data/raw.jsonl \
		--output_prefix=../../data/tier2-parquet/events

compare-sizes:
	cd tools/compare && . ../../scripts/logfire-env.sh && uv run python3 -m compare.sizes

compare-schema:
	cd tools/compare && . ../../scripts/logfire-env.sh && uv run python3 -m compare.schema_inspect

compare-bench:
	cd tools/compare && . ../../scripts/logfire-env.sh && uv run python3 -m compare.query_bench

compare-encode:
	cd tools/compare && . ../../scripts/logfire-env.sh && uv run python3 -m compare.encode_bench

compare-report:
	cd tools/compare && . ../../scripts/logfire-env.sh && uv run python3 -m compare.report

go-build:
	cd tools/poller && go build ./...

go-test:
	cd tools/poller && go test ./...
