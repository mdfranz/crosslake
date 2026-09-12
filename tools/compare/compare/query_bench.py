"""Runs the reference queries in queries.sql identically across all three
sources and times each, wrapped in Logfire spans -- see PLAN.md ("Build /
verification order", step 10). Because compare.db exposes a consistent
eventName/eventSource/eventTime shape on all three views, the exact same
SQL text runs unmodified against S3 JSON, local Parquet, and local Avro.
"""

import re
import time
from pathlib import Path

import logfire

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, connect

QUERIES_PATH = Path(__file__).parent / "queries.sql"
VIEWS = ["s3_baseline", "tier2_parquet", "tier3_avro"]


def load_queries(path: Path = QUERIES_PATH) -> dict[str, str]:
    """Parses `-- name: <name>` blocks out of queries.sql."""
    text = path.read_text()
    blocks = re.split(r"^-- name: (\w+)\s*$", text, flags=re.MULTILINE)[1:]
    return {name: sql.strip().rstrip(";") for name, sql in zip(blocks[0::2], blocks[1::2])}


def run(data_dir: Path = DEFAULT_DATA_DIR, poller_config_path: Path = DEFAULT_POLLER_CONFIG):
    con = connect(data_dir, poller_config_path)
    queries = load_queries()

    results = []
    for name, sql_template in queries.items():
        for view in VIEWS:
            sql = sql_template.format(view=view)
            with logfire.span("query", query_name=name, view=view):
                t0 = time.perf_counter()
                con.execute(sql).fetchall()
                elapsed_ms = (time.perf_counter() - t0) * 1000
            results.append({"query": name, "view": view, "elapsed_ms": elapsed_ms})
    return results


def main():
    telemetry.configure()
    results = run()

    print(f"{'query':<20}{'view':<16}{'elapsed_ms':>12}")
    for r in results:
        print(f"{r['query']:<20}{r['view']:<16}{r['elapsed_ms']:>12.1f}")

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
