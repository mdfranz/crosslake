"""Size inventory across the current S3 prefix and cumulative local tiers.

These values are not a format comparison until an experiment manifest proves
the remote and local cohorts match and accounts for fields omitted by the typed
schema. See docs/review-telemetry-plan.md.
"""

from pathlib import Path

import duckdb
import logfire

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, connect, load_poller_config


def s3_gzip_total_bytes(con: duckdb.DuckDBPyConnection, bucket: str, prefixes: list[str]) -> tuple[int, int]:
    """Returns (object_count, total_compressed_bytes) summed across all
    prefixes, via S3's own object listing (not a JSON parse). Must be
    called with the *same* prefix list compare.db.connect() used to build
    s3_baseline (see its aws.s3_prefixes docstring) -- this function used
    to always read the single aws.s3_prefix regardless of s3_prefixes,
    silently measuring the wrong cohort's size whenever they differed."""
    n_files_total = 0
    bytes_total = 0
    for prefix in prefixes:
        glob = f"s3://{bucket}/{prefix.rstrip('/')}/**/*.json.gz"
        n_files, total = con.execute(
            f"SELECT count(*), coalesce(sum(size), 0) FROM read_blob('{glob}')"
        ).fetchone()
        n_files_total += n_files
        bytes_total += total
    return n_files_total, bytes_total


def local_file_bytes(path: Path) -> int:
    if path.is_dir():
        return sum(p.stat().st_size for p in path.glob("**/*") if p.is_file())
    return path.stat().st_size if path.exists() else 0


def run(data_dir: Path = DEFAULT_DATA_DIR, poller_config_path: Path = DEFAULT_POLLER_CONFIG) -> dict:
    cfg = load_poller_config(poller_config_path)
    con = connect(data_dir, poller_config_path)

    prefixes = cfg["aws"].get("s3_prefixes") or [cfg["aws"]["s3_prefix"]]
    with logfire.span("sizes.s3_baseline"):
        n_files, s3_gzip_bytes = s3_gzip_total_bytes(con, cfg["aws"]["s3_bucket"], prefixes)

    raw_jsonl_bytes = local_file_bytes(data_dir / "raw.jsonl")
    tier2_bytes = local_file_bytes(data_dir / "tier2-parquet")
    tier3_bytes = local_file_bytes(data_dir / "tier3-avro")

    result = {
        "s3_gzip_original": {"files": n_files, "bytes": s3_gzip_bytes},
        "raw_jsonl_uncompressed": {"bytes": raw_jsonl_bytes},
        "tier2_parquet": {"bytes": tier2_bytes},
        "tier3_avro": {"bytes": tier3_bytes},
    }
    logfire.info("sizes computed", **{k: v["bytes"] for k, v in result.items()})
    return result


def _fmt(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def main():
    telemetry.configure()
    result = run()
    baseline = result["s3_gzip_original"]["bytes"]
    print(f"{'source':<28}{'bytes':>12}{'vs S3 gzip':>14}")
    for name, info in result.items():
        b = info["bytes"]
        ratio = f"{b / baseline:.2f}x" if baseline else "n/a"
        print(f"{name:<28}{_fmt(b):>12}{ratio:>14}")
    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
