"""ParDo that turns one raw CloudTrail record (one JSON object per line in
raw.jsonl) into the typed dict shape matching
cloudtrail_schema.CLOUDTRAIL_SCHEMA. Mirrors
tools/poller/internal/avroenc/record.go field-for-field so Tier 2 (Parquet)
and Tier 3 (Avro) are a fair comparison.
"""

import json
from datetime import datetime, timezone

import apache_beam as beam
import logfire
from opentelemetry import context as otel_context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

REJECTS_TAG = "rejects"


def _parse_time(s):
    if not s:
        return None
    # CloudTrail timestamps are RFC3339 with a literal 'Z' suffix.
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _json_or_none(value):
    if value is None:
        return None
    return json.dumps(value)


class ParseCloudTrailJson(beam.DoFn):
    """Parses one raw CloudTrail JSON record. Bad records go to the
    REJECTS_TAG side output as (raw_line, error) pairs instead of a full
    DLQ -- see PLAN.md ("Tier 2 Beam pipeline").

    traceparent is a W3C traceparent string captured from the pipeline-run
    span *before* the Beam graph is built. Beam's DirectRunner (and any
    other runner) executes DoFn.process() in worker threads/processes where
    Python's ambient OTEL context does not cross the boundary -- without
    explicitly re-attaching this parent context in each process() call,
    every parse_record span comes out as its own orphaned root trace
    instead of a child of run_pipeline.
    """

    def __init__(self, traceparent: str):
        self._traceparent = traceparent

    def process(self, element: str):
        token = otel_context.attach(
            TraceContextTextMapPropagator().extract({"traceparent": self._traceparent})
        )
        try:
            with logfire.span("parse_record"):
                try:
                    raw = json.loads(element)
                except json.JSONDecodeError as e:
                    logfire.metric_counter("parquet_writer.rejected_records").add(1)
                    yield beam.pvalue.TaggedOutput(REJECTS_TAG, (element, f"invalid JSON: {e}"))
                    return

                try:
                    record = self._to_record(raw)
                except Exception as e:  # noqa: BLE001 -- one bad record shouldn't kill the pipeline
                    logfire.metric_counter("parquet_writer.rejected_records").add(1)
                    yield beam.pvalue.TaggedOutput(REJECTS_TAG, (element, f"parse error: {e}"))
                    return

                logfire.metric_counter("parquet_writer.parsed_records").add(1)
                yield record
        finally:
            otel_context.detach(token)

    @staticmethod
    def _to_record(raw: dict) -> dict:
        user_identity = raw.get("userIdentity") or {}
        return {
            "eventVersion": raw.get("eventVersion"),
            "eventTime": _parse_time(raw.get("eventTime")),
            "eventSource": raw.get("eventSource"),
            "eventName": raw.get("eventName"),
            "eventType": raw.get("eventType"),
            "eventCategory": raw.get("eventCategory"),
            "awsRegion": raw.get("awsRegion"),
            "sourceIPAddress": raw.get("sourceIPAddress"),
            "userAgent": raw.get("userAgent"),
            "requestID": raw.get("requestID"),
            "eventID": raw.get("eventID"),
            "sharedEventID": raw.get("sharedEventID"),
            "recipientAccountId": raw.get("recipientAccountId"),
            "readOnly": raw.get("readOnly"),
            "managementEvent": raw.get("managementEvent"),
            "userIdentity": {
                "type": user_identity.get("type"),
                "principalId": user_identity.get("principalId"),
                "arn": user_identity.get("arn"),
                "accountId": user_identity.get("accountId"),
                "accessKeyId": user_identity.get("accessKeyId"),
                "userName": user_identity.get("userName"),
                "invokedBy": user_identity.get("invokedBy"),
                "sessionContextJson": _json_or_none(user_identity.get("sessionContext")),
            },
            "requestParametersJson": _json_or_none(raw.get("requestParameters")),
            "responseElementsJson": _json_or_none(raw.get("responseElements")),
            "additionalEventDataJson": _json_or_none(raw.get("additionalEventData")),
            "resourcesJson": _json_or_none(raw.get("resources")),
            "serviceEventDetailsJson": _json_or_none(raw.get("serviceEventDetails")),
        }
