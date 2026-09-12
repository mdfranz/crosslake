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
    prefix = cfg["aws"]["s3_prefix"].rstrip("/")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL avro; LOAD avro;")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain);")

    s3_glob = f"s3://{bucket}/{prefix}/**/*.json.gz"
    parquet_glob = str(data_dir / "tier2-parquet" / "*.parquet")
    avro_path = str(data_dir / "tier3-avro" / "events.avro")

    # S3 baseline: Records kept as a JSON[] column (not auto-inferred structs)
    # -- CloudTrail's per-event-type field variability makes DuckDB's struct
    # unification either explode or fail outright across a real day of mixed
    # events. json_extract_string pulls out only the fields we need.
    con.execute(f"""
        CREATE OR REPLACE VIEW s3_baseline AS
        SELECT
            json_extract_string(r, '$.eventName') AS eventName,
            json_extract_string(r, '$.eventSource') AS eventSource,
            json_extract_string(r, '$.eventTime') AS eventTime,
            json_extract_string(r, '$.eventID') AS eventID,
            r AS record
        FROM read_json('{s3_glob}', columns={{'Records': 'JSON[]'}}), unnest(Records) AS t(r)
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
