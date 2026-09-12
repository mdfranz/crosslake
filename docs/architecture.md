# Architecture (Local Mode, as built)

Full design (including the not-yet-built GCP Cloud Mode) is in `PLAN.md`.
This is the as-built Local Mode diagram.

```
                     real CloudTrail delivery
                              |
                              v
   AWS S3 (your CloudTrail bucket, AWSLogs/<acct>/CloudTrail/<region>/...)
                              |
                              |  ListObjectsV2 + cursor, gunzip, per-record
                              v
                    tools/poller (Go, --mode=local)
                    /                          \
                   v                            v
     ./data/raw.jsonl                ./data/tier3-avro/events.avro
     (verbatim JSON lines,            (typed CloudTrailEvent record,
      appended)                        hamba/avro/v2/ocf, deflate,
        |                              appended across runs)
        |
        v
  pipelines/parquet-writer (Python/Beam, DirectRunner)
  ReadFromText -> ParseCloudTrailJson -> WriteToParquet
        |
        v
  ./data/tier2-parquet/events-*.parquet
  (typed CloudTrailEvent schema, gzip)

                              |
                              v
              tools/compare (Python + DuckDB)
   views: s3_baseline (direct S3 JSON) | tier2_parquet | tier3_avro
   sizes.py | schema_inspect.py | query_bench.py | encode_bench.py | report.py
                              |
                              v
                    LEARNINGS.md + Logfire traces
```

## Why this shape

- **Source is always real S3**, in both this build and the future Cloud
  Mode -- the poller's `s3source` package doesn't change when Cloud Mode is
  added; only the sink does (`internal/pubsubsink` vs `internal/disksink`).
- **raw.jsonl and events.avro are siblings, not a pipeline stage into each
  other** -- both are written directly from the same parsed record by the
  poller in one pass, mirroring how Cloud Mode's dual-publish (to
  `cloudtrail-raw` and `cloudtrail-avro`) works: two independent outputs
  from one source read, not one derived from the other.
- **The Beam pipeline reads raw.jsonl, not events.avro** -- Tier 2 is
  designed to parse the *raw* JSON path (mirroring `cloudtrail-raw` in Cloud
  Mode), not to convert Avro to Parquet. This keeps the Tier 2 and Tier 3
  code paths independent, which is the point of the comparison.
- **`tools/compare`'s views normalize just enough** (`eventName`,
  `eventSource`, `eventTime`) that the *same SQL text* runs against S3 JSON,
  local Parquet, and local Avro -- see `compare/queries.sql`.

## Not built yet (see PLAN.md)

GCP Pub/Sub (`cloudtrail-raw`, `cloudtrail-avro` topics with a Pub/Sub
Schema), the three GCS subscriptions (Tier 1 generic envelope, Tier 3
structured Avro), a real Dataflow job for Tier 2, and the AWS/GCP Pulumi
stacks that would provision all of that.
