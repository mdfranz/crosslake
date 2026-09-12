-- Reference queries run identically across all three sources by
-- compare/query_bench.py, substituting {view} = s3_baseline | tier2_parquet
-- | tier3_avro. All three expose eventName/eventSource/eventTime under the
-- same names (see compare/db.py), so the exact same SQL runs unmodified
-- against S3 JSON, GCS/local Parquet, and GCS/local Avro -- the point of
-- putting DuckDB in front of all three. See PLAN.md ("Language split &
-- DuckDB strategy").
--
-- An optional `-- views: v1,v2` line right after `-- name:` restricts a
-- query to a subset of views (parsed by query_bench.py); use it for
-- anything touching a column s3_baseline doesn't expose (userIdentity,
-- readOnly, ...). Every query below through cohort_signature predates this
-- and intentionally stayed 3-way; the ones after it target the
-- Avro-vs-Parquet-specific dimensions PLAN.md's storage-layout benchmark
-- calls for (projection width, selectivity, nested-field access) that the
-- original four queries -- all full-scan, all narrow single-column
-- aggregates, no filter, no nesting -- didn't exercise at all.

-- name: top_event_names
-- ORDER BY needs a tiebreaker: CloudTrail has many eventNames tied at low
-- counts, so `ORDER BY n DESC LIMIT 10` alone is non-deterministic at the
-- cutoff -- caught by query_bench.py's result-hash check reporting
-- views_match=False even when every view's full data actually agreed.
SELECT eventName, count(*) AS n
FROM {view}
GROUP BY eventName
ORDER BY n DESC, eventName ASC
LIMIT 10;

-- name: count_by_source
SELECT eventSource, count(*) AS n
FROM {view}
GROUP BY eventSource
ORDER BY n DESC;

-- name: time_range
SELECT min(eventTime) AS earliest, max(eventTime) AS latest, count(*) AS n
FROM {view};

-- name: cohort_signature
-- Order-independent fingerprint catches missing/duplicate event IDs without
-- writing CloudTrail identifiers into the report or telemetry.
SELECT
    count(*) AS n,
    count(DISTINCT eventID) AS distinct_event_ids,
    bit_xor(hash(coalesce(eventID, ''))) AS event_id_fingerprint
FROM {view};

-- name: count_only
-- Zero-column projection baseline. A columnar reader can in principle
-- answer this from file metadata (row-group/row counts) without touching
-- any column's data at all; a row-oriented reader still has to walk
-- records. If Parquet's advantage over Avro is genuinely about column
-- skipping (not just a faster decoder), it should show up most here.
SELECT count(*) AS n
FROM {view};

-- name: filtered_low_selectivity
-- Broad filter: 'PutObject' matches ~64% of this cohort's rows. Tests
-- whether row-group/block skipping helps when most rows match anyway --
-- expect little to no advantage for either format, since almost nothing
-- can be skipped regardless of how it's indexed.
SELECT count(*) AS n
FROM {view}
WHERE eventName = 'PutObject';

-- name: filtered_high_selectivity
-- Narrow filter: 'DescribeOrganization' matches ~0.3% of rows (check with
-- SELECT eventName, count(*) ... for the current cohort -- CloudTrail
-- traffic composition varies by day). Tests whether Parquet's per-row-group
-- min/max statistics let it skip most of the file, versus Avro having no
-- comparable indexing and needing to scan/decompress every block to check.
SELECT count(*) AS n
FROM {view}
WHERE eventName = 'DescribeOrganization';

-- name: nested_identity_breakdown
-- views: tier2_parquet,tier3_avro
-- Nested-field access (userIdentity.type). s3_baseline only normalizes
-- eventName/eventSource/eventTime (see compare/db.py), not the full typed
-- schema, so this is Avro-vs-Parquet only. Tests whether Parquet's native
-- struct-column support lets it prune sibling struct fields (arn,
-- accessKeyId, sessionContextJson, ...) that this query never touches, or
-- whether Avro's row-oriented record still has to deserialize the whole
-- userIdentity record regardless.
SELECT userIdentity.type AS identity_type, count(*) AS n
FROM {view}
GROUP BY identity_type
ORDER BY n DESC, identity_type ASC;

-- name: boolean_breakdown
-- views: tier2_parquet,tier3_avro
-- Low-cardinality boolean column -- a case Parquet's dictionary/RLE
-- encoding is specifically good at. Avro has no per-column encoding at all
-- (its compression, if any, is applied to the whole serialized block).
SELECT readOnly, count(*) AS n
FROM {view}
GROUP BY readOnly
ORDER BY readOnly;

-- name: full_row_materialize
-- views: tier2_parquet,tier3_avro
-- The opposite extreme from count_only: touches every column, forcing
-- full-row reconstruction. This is the case where a row-oriented format
-- like Avro should be most competitive with (or beat) a columnar one,
-- since Parquet has to stitch the row back together from many separate
-- column chunks while Avro's on-disk layout already is a row. eventTime
-- is read as max(eventTime) (a typed value, not cast to a string) rather
-- than folded into the char-count sum, since tier2/tier3 differ in
-- timestamp-with-timezone annotation (see docs/schema-design-notes.md) --
-- a real, already-documented Avro/Parquet typing difference, not something
-- this particular query is trying to catch.
SELECT
    count(*) AS n,
    max(eventTime) AS latest,
    sum(
        length(coalesce(eventVersion, '')) + length(coalesce(eventSource, '')) +
        length(coalesce(eventName, '')) + length(coalesce(eventType, '')) +
        length(coalesce(eventCategory, '')) + length(coalesce(awsRegion, '')) +
        length(coalesce(sourceIPAddress, '')) + length(coalesce(userAgent, '')) +
        length(coalesce(requestID, '')) + length(coalesce(eventID, '')) +
        length(coalesce(sharedEventID, '')) + length(coalesce(recipientAccountId, '')) +
        length(coalesce(CAST(readOnly AS VARCHAR), '')) +
        length(coalesce(CAST(managementEvent AS VARCHAR), '')) +
        length(coalesce(userIdentity.type, '')) + length(coalesce(userIdentity.principalId, '')) +
        length(coalesce(userIdentity.arn, '')) + length(coalesce(userIdentity.accountId, '')) +
        length(coalesce(userIdentity.accessKeyId, '')) + length(coalesce(userIdentity.userName, '')) +
        length(coalesce(userIdentity.invokedBy, '')) + length(coalesce(userIdentity.sessionContextJson, '')) +
        length(coalesce(requestParametersJson, '')) + length(coalesce(responseElementsJson, '')) +
        length(coalesce(additionalEventDataJson, '')) + length(coalesce(resourcesJson, '')) +
        length(coalesce(serviceEventDetailsJson, ''))
    ) AS total_chars
FROM {view};
