"""Tier 2 pyarrow schema, hand-kept in sync with
tools/poller/schema/cloudtrail.avsc field-for-field so the Avro/Parquet
comparison is fair. Field list is grounded in a real record sampled from
live CloudTrail delivery (see docs/schema-design-notes.md), not the AWS
docs alone.

``userIdentity`` is kept as a nested struct column -- a deliberate point of
comparison against the Avro record (Parquet's native nested-column support).
Event-type-dependent blobs (requestParameters, responseElements,
additionalEventData, resources, serviceEventDetails) stay as JSON-string
escape-hatch columns since their shape varies by eventName/eventSource.
"""

import pyarrow as pa

USER_IDENTITY_STRUCT = pa.struct(
    [
        ("type", pa.string()),
        ("principalId", pa.string()),
        ("arn", pa.string()),
        ("accountId", pa.string()),
        ("accessKeyId", pa.string()),
        ("userName", pa.string()),
        ("invokedBy", pa.string()),
        ("sessionContextJson", pa.string()),
    ]
)

# eventTime is a genuine timestamp column here (not a string) -- the same
# comparison point as the Avro side's timestamp-millis logical type.
CLOUDTRAIL_SCHEMA = pa.schema(
    [
        ("eventVersion", pa.string()),
        ("eventTime", pa.timestamp("ms", tz="UTC")),
        ("eventSource", pa.string()),
        ("eventName", pa.string()),
        ("eventType", pa.string()),
        ("eventCategory", pa.string()),
        ("awsRegion", pa.string()),
        ("sourceIPAddress", pa.string()),
        ("userAgent", pa.string()),
        ("requestID", pa.string()),
        ("eventID", pa.string()),
        ("sharedEventID", pa.string()),
        ("recipientAccountId", pa.string()),
        ("readOnly", pa.bool_()),
        ("managementEvent", pa.bool_()),
        ("userIdentity", USER_IDENTITY_STRUCT),
        ("requestParametersJson", pa.string()),
        ("responseElementsJson", pa.string()),
        ("additionalEventDataJson", pa.string()),
        ("resourcesJson", pa.string()),
        ("serviceEventDetailsJson", pa.string()),
    ]
)
