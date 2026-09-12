"""Shared DuckDB connection setup for the compare tool: installs the
required extensions and registers the three comparison sources as views --
see PLAN.md ("Language split & DuckDB strategy").

S3 credentials use DuckDB's own credential_chain provider (same AWS
credentials the poller/CLI already use); no GCS HMAC key is needed since
this build is Local Mode only (no GCS Parquet/Avro yet).
"""

from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]  # db.py -> compare/ -> tools/compare/ -> tools/ -> repo root
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DEFAULT_POLLER_CONFIG = REPO_ROOT / "tools" / "poller" / "config.yaml"


def load_poller_config(path: Path = DEFAULT_POLLER_CONFIG) -> dict:
    """Reuses tools/poller/config.yaml for the bucket/prefix so the compare
    tool always looks at the same S3 range the poller just pulled from."""
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found -- copy tools/poller/config.example.yaml to "
            "config.yaml and fill in your bucket/prefix first"
        )
    return yaml.safe_load(path.read_text())


def connect(
    data_dir: Path = DEFAULT_DATA_DIR,
    poller_config_path: Path = DEFAULT_POLLER_CONFIG,
) -> duckdb.DuckDBPyConnection:
    cfg = load_poller_config(poller_config_path)
    bucket = cfg["aws"]["s3_bucket"]
    # aws.s3_prefixes (a list) takes precedence over the single aws.s3_prefix
    # the poller itself uses, for comparing against a cohort spanning more
    # than one day/prefix -- e.g. after ingesting several days via the
    # poller's --s3-prefix override. tools/poller/cmd/poller/config.go
    # doesn't recognize this key and yaml.Unmarshal ignores it harmlessly;
    # it's additive and compare-only, never read by the poller.
    prefixes = cfg["aws"].get("s3_prefixes") or [cfg["aws"]["s3_prefix"]]
    prefixes = [p.rstrip("/") for p in prefixes]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL avro; LOAD avro;")
    # CHAIN 'env' restricts DuckDB's AWS credential search to environment
    # variables only. Without it, PROVIDER credential_chain's default search
    # order also tries EC2 instance metadata (IMDS) -- a ~1s-per-attempt
    # timeout since this never runs on EC2 -- adding up to a measured ~8s on
    # every single connect(), regardless of whether any query touches S3.
    # This is this project's dominant per-invocation cost by far (identified
    # via `logfire-query` review: every compare.* tool pays this once at
    # startup). If auth here ever moves off static env-var credentials
    # (e.g. to an AWS profile/SSO), broaden to `CHAIN 'env;config;sts;sso'`
    # -- still excluding 'instance', which is the slow one off EC2.
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env');")

    # DuckDB's glob() doesn't support brace expansion (`{10,11}` matches
    # nothing, confirmed by testing) -- read_json's own file-list argument
    # does accept a Python-style list of globs, one per prefix.
    s3_globs = [f"s3://{bucket}/{p}/**/*.json.gz" for p in prefixes]
    s3_glob_list = "[" + ", ".join(f"'{g}'" for g in s3_globs) + "]"
    parquet_glob = str(data_dir / "tier2-parquet" / "*.parquet")
    avro_path = str(data_dir / "tier3-avro" / "events.avro")

    # S3 baseline: Records kept as a JSON[] column (not auto-inferred structs)
    # -- CloudTrail's per-event-type field variability makes DuckDB's struct
    # unification either explode or fail outright across a real day of mixed
    # events. json_extract_string pulls out only the fields we need.
    #
    # The unnest happens in its own CTE, separate from the projection that
    # extracts eventName/etc. A single `SELECT json_extract_string(r, ...)
    # FROM read_json(...), unnest(Records) AS t(r)` measured fine unfiltered,
    # but adding a WHERE on the extracted column made DuckDB plan it as a
    # LEFT_DELIM_JOIN (a correlated/lateral-join strategy) that re-executes
    # READ_JSON's remote S3 scan repeatedly -- a query.sql filtered query
    # went from ~9s to ~85-92s from this alone (see LEARNINGS.md). Forcing
    # the unnest to fully materialize in its own CTE before any filter can
    # reference the unnested value keeps every s3_baseline query as a flat
    # scan regardless of what's filtered downstream.
    con.execute(f"""
        CREATE OR REPLACE VIEW s3_baseline AS
        WITH unnested AS (
            SELECT unnest(Records) AS record
            FROM read_json({s3_glob_list}, columns={{'Records': 'JSON[]'}})
        )
        SELECT
            json_extract_string(record, '$.eventName') AS eventName,
            json_extract_string(record, '$.eventSource') AS eventSource,
            json_extract_string(record, '$.eventTime') AS eventTime,
            json_extract_string(record, '$.eventID') AS eventID,
            record
        FROM unnested
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW tier2_parquet AS
        SELECT * FROM read_parquet('{parquet_glob}')
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW tier3_avro AS
        SELECT * FROM read_avro('{avro_path}')
    """)

    return con
