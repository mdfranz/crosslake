"""ParDo that turns one raw CloudTrail record (one JSON object per line in
raw.jsonl) into the typed dict shape matching
cloudtrail_schema.CLOUDTRAIL_SCHEMA. Mirrors
tools/poller/internal/avroenc/record.go field-for-field so Tier 2 (Parquet)
and Tier 3 (Avro) are a fair comparison.
"""

import json
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

REJECTS_TAG = "rejects"
METRIC_NAMESPACE = "crosslake.parquet_writer"


def _parse_time(s):
    if not s:
        return None
    # Accept RFC3339 fractional seconds and explicit offsets, not only the
    # most common whole-second Z form.
    parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("eventTime must include an RFC3339 timezone")
    return parsed.astimezone(timezone.utc)


def _json_or_none(value):
    """Canonical JSON: sorted keys, compact separators, real UTF-8 instead
    of \\uXXXX escapes. Without this, the escape-hatch JSON-string columns
    end up with different content than the Go poller's equivalent
    canonicalJSONPtr (tools/poller/internal/avroenc/record.go), which
    re-serializes with Go's default map-key sorting and SetEscapeHTML(false)
    -- a real, measured divergence (see LEARNINGS.md) even though both
    sides parse the same source JSON with the same logical content.
    """
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ParseCloudTrailJson(beam.DoFn):
    """Parses one raw CloudTrail JSON record. Bad records go to the
    REJECTS_TAG side output as (raw_line, error) pairs instead of a full
    DLQ -- see PLAN.md ("Tier 2 Beam pipeline").

    Per-element signals use Beam's runner-native metrics. A Logfire span per
    record is both expensive and misleading in a distributed runner: all
    records were previously attached to one driver-created trace context.
    """

    def __init__(self):
        self._parsed = Metrics.counter(METRIC_NAMESPACE, "parsed_records")
        self._rejected_json = Metrics.counter(METRIC_NAMESPACE, "rejected_invalid_json")
        self._rejected_parse = Metrics.counter(METRIC_NAMESPACE, "rejected_parse_error")
        self._input_bytes = Metrics.distribution(METRIC_NAMESPACE, "input_record_bytes")
        self._parse_us = Metrics.distribution(METRIC_NAMESPACE, "parse_duration_us")

    def process(self, element: str):
        started_ns = time.perf_counter_ns()
        self._input_bytes.update(len(element.encode("utf-8")))
        try:
            raw = json.loads(element)
        except json.JSONDecodeError as e:
            self._rejected_json.inc()
            self._parse_us.update((time.perf_counter_ns() - started_ns) // 1_000)
            yield beam.pvalue.TaggedOutput(REJECTS_TAG, (element, f"invalid JSON: {e}"))
            return

        try:
            record = self._to_record(raw)
        except Exception as e:  # noqa: BLE001 -- one bad record shouldn't kill the pipeline
            self._rejected_parse.inc()
            self._parse_us.update((time.perf_counter_ns() - started_ns) // 1_000)
            yield beam.pvalue.TaggedOutput(REJECTS_TAG, (element, f"parse error: {e}"))
            return

        self._parsed.inc()
        self._parse_us.update((time.perf_counter_ns() - started_ns) // 1_000)
        yield record

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
