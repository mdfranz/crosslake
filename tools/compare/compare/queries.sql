-- Reference queries run identically across all three sources by
-- compare/query_bench.py, substituting {view} = s3_baseline | tier2_parquet
-- | tier3_avro. All three expose eventName/eventSource/eventTime under the
-- same names (see compare/db.py), so the exact same SQL runs unmodified
-- against S3 JSON, GCS/local Parquet, and GCS/local Avro -- the point of
-- putting DuckDB in front of all three. See PLAN.md ("Language split &
-- DuckDB strategy").

-- name: top_event_names
SELECT eventName, count(*) AS n
FROM {view}
GROUP BY eventName
ORDER BY n DESC
LIMIT 10;

-- name: count_by_source
SELECT eventSource, count(*) AS n
FROM {view}
GROUP BY eventSource
ORDER BY n DESC;

-- name: time_range
SELECT min(eventTime) AS earliest, max(eventTime) AS latest, count(*) AS n
FROM {view};
