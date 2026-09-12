"""Tier 2 Beam pipeline: parses CloudTrail JSON and writes typed columnar
Parquet. See PLAN.md ("Tier 2 Beam pipeline").

Local Mode (this build): --input_mode=file reads a saved .jsonl sample
(./data/raw.jsonl, written by the Go poller) and runs on DirectRunner --
this is a bounded source, so no windowing is needed.

Cloud Mode (--input_mode=pubsub, DataflowRunner) is designed but not
implemented in this build: reading an unbounded PubSub source would need
WindowInto(FixedWindows(...)) before the file sinks, or Beam will never
flush -- see PLAN.md ("Risks / gotchas").
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.options.pipeline_options import PipelineOptions
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from parquet_writer import telemetry
from parquet_writer.cloudtrail_schema import CLOUDTRAIL_SCHEMA
from parquet_writer.transforms import REJECTS_TAG, ParseCloudTrailJson


def build_pipeline(
    p: beam.Pipeline, input_mode: str, input_file: str, output_prefix: str, traceparent: str
) -> None:
    if input_mode != "file":
        raise NotImplementedError(
            f"input_mode={input_mode!r} not implemented in this build -- "
            "Local Mode only supports --input_mode=file (see PLAN.md)"
        )

    lines = p | "ReadRawJsonl" >> ReadFromText(input_file)

    parsed = lines | "ParseCloudTrailJson" >> beam.ParDo(
        ParseCloudTrailJson(traceparent)
    ).with_outputs(REJECTS_TAG, main="records")

    _ = (
        parsed.records
        | "WriteParquet"
        >> WriteToParquet(
            file_path_prefix=output_prefix,
            schema=CLOUDTRAIL_SCHEMA,
            file_name_suffix=".parquet",
            # gzip (zlib/deflate family), to match the Tier 3 Avro sink's
            # Deflate codec (see disksink.go) -- both default to no
            # compression otherwise, which makes "vs. the original gzip"
            # comparison meaningless. zstd was tried first but DuckDB's avro
            # extension can't read hamba/avro's zstandard OCF output, so
            # deflate/gzip is the fair common ground across both tiers.
            codec="gzip",
        )
    )

    _ = (
        parsed[REJECTS_TAG]
        | "FormatRejects" >> beam.Map(lambda pair: f"{pair[1]}\t{pair[0]}")
        | "WriteRejects" >> WriteToText(output_prefix + "_rejects", file_name_suffix=".txt")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", default="DirectRunner")
    parser.add_argument("--input_mode", default="file", choices=["file", "pubsub"])
    parser.add_argument("--input_file", default="../../data/raw.jsonl")
    parser.add_argument("--output_prefix", default="../../data/tier2-parquet/events")
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args.append(f"--runner={known_args.runner}")

    logging.basicConfig(level=logging.INFO)
    telemetry.configure()

    import logfire

    with logfire.span(
        "run_pipeline",
        runner=known_args.runner,
        input_mode=known_args.input_mode,
    ):
        # Capture this span as a W3C traceparent *before* the Beam graph
        # runs: DoFn.process() executes in worker threads/processes where
        # ambient OTEL context does not cross the boundary, so each
        # ParseCloudTrailJson call re-attaches this explicitly (see
        # transforms.py) instead of coming out as an orphaned root trace.
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)
        traceparent = carrier["traceparent"]

        options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=options) as p:
            build_pipeline(
                p, known_args.input_mode, known_args.input_file, known_args.output_prefix, traceparent
            )

    logfire.force_flush()


if __name__ == "__main__":
    main()
