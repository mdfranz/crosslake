# Schema design notes

Source of truth for CloudTrail field typing, grounded in a real record
sampled from live CloudTrail delivery during development (not the AWS docs
alone) -- see `tools/poller/schema/cloudtrail.avsc` (Avro/Tier 3) and
`pipelines/parquet-writer/parquet_writer/cloudtrail_schema.py` (Parquet/Tier
2), which are hand-kept in sync field-for-field.

## What the real sample showed

A `GetBucketAcl` event from `cloudtrail.amazonaws.com` (an AWSService
identity) had this top-level key set:

```
eventVersion, userIdentity, eventTime, eventSource, eventName, awsRegion,
sourceIPAddress, userAgent, requestParameters, responseElements,
additionalEventData, requestID, eventID, readOnly, resources, eventType,
managementEvent, recipientAccountId, sharedEventID, eventCategory
```

This is a wider set than the original plan's field list -- `readOnly`,
`resources`, `managementEvent`, `sharedEventID`, and `eventCategory` weren't
in the initial design and were added once seen in real data.

## userIdentity varies materially by `type`

The sampled `AWSService` identity had only `type` and `invokedBy`:

```json
{"type": "AWSService", "invokedBy": "cloudtrail.amazonaws.com"}
```

An `IAMUser` or `AssumedRole` identity carries a very different shape
(`arn`, `accountId`, `accessKeyId`, `userName`, and for `AssumedRole` a
deeply-nested `sessionContext` with role/session issuer info). Rather than
model every identity type as its own strict record, the schema keeps a
superset of the stable scalar fields (`type`, `principalId`, `arn`,
`accountId`, `accessKeyId`, `userName`, `invokedBy`) plus one JSON-string
escape hatch (`sessionContextJson`) for the part that's genuinely
per-role-shaped. This is still a strict nested Avro record / Parquet struct
-- the point of comparison PLAN.md called out -- just with mostly-null
fields depending on identity type, which is normal and expected.

## Event-type-dependent blobs stay as JSON strings

`requestParameters`, `responseElements`, `additionalEventData`, `resources`,
and `serviceEventDetails` are kept as JSON-string columns in both Avro and
Parquet, per the original design: their shape varies by
`eventName`/`eventSource` and isn't worth strict per-service modeling for
this comparison.

## eventTime is a genuine typed column, not a string

Both tiers store `eventTime` as a real timestamp (Avro `timestamp-millis`
logical type / Go `time.Time`; Parquet `timestamp("ms", tz="UTC")` / Python
`datetime`), parsed from CloudTrail's `RFC3339` string with `time.Parse` /
`datetime.strptime`. This is deliberate: it's one of the clearest points
where "genuinely typed" (Tier 2/3) differs from the generic envelope
(Tier 1), where a Dataflow-templated `Pub/Sub to Avro` sink would leave
`eventTime` as an opaque string inside the raw JSON payload bytes.

## Schema mismatch found during testing

DuckDB reports `tier2_parquet.eventTime` as `TIMESTAMP WITH TIME ZONE` but
`tier3_avro.eventTime` as `TIMESTAMP` (no timezone) -- both are UTC
internally, but pyarrow's `timestamp("ms", tz="UTC")` round-trips through
Parquet with an explicit zone annotation, while Avro's `timestamp-millis`
logical type has no zone concept at all (Avro timestamps are always UTC by
spec, just not tagged as such in the type). Worth calling out in
LEARNINGS.md as a real, observed Avro-vs-Parquet typing difference, not a
bug -- readers of the Avro file need to already know it's UTC.
