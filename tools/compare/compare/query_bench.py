"""Runs the reference queries in queries.sql identically across all three
sources and times each, wrapped in Logfire spans -- see PLAN.md ("Build /
verification order", step 10). Because compare.db exposes a consistent
eventName/eventSource/eventTime shape on all three views, the exact same
SQL text runs unmodified against S3 JSON, local Parquet, and local Avro.
"""

import argparse
import hashlib
import json
import math
import random
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import logfire

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, connect

QUERIES_PATH = Path(__file__).parent / "queries.sql"
VIEWS = ["s3_baseline", "tier2_parquet", "tier3_avro"]
DEFAULT_WARMUPS = 1
DEFAULT_REPEATS = 5
DEFAULT_SEED = 1729


def load_queries(path: Path = QUERIES_PATH) -> dict[str, str]:
    """Parses `-- name: <name>` blocks out of queries.sql."""
    text = path.read_text()
    blocks = re.split(r"^-- name: (\w+)\s*$", text, flags=re.MULTILINE)[1:]
    return {name: sql.strip().rstrip(";") for name, sql in zip(blocks[0::2], blocks[1::2])}


def _json_default(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _result_digest(rows: list[tuple]) -> str:
    """Order-independent digest used to verify timed queries returned the
    same content without putting CloudTrail-derived values in telemetry."""
    encoded = sorted(
        json.dumps(row, default=_json_default, separators=(",", ":")) for row in rows
    )
    return hashlib.sha256(("\n".join(encoded) + "\n").encode()).hexdigest()


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    poller_config_path: Path = DEFAULT_POLLER_CONFIG,
    *,
    warmups: int = DEFAULT_WARMUPS,
    repeats: int = DEFAULT_REPEATS,
    seed: int = DEFAULT_SEED,
):
    if warmups < 0:
        raise ValueError("warmups must be >= 0")
    if repeats < 1:
        raise ValueError("repeats must be >= 1")

    con = connect(data_dir, poller_config_path)
    queries = load_queries()
    workload = [
        (name, view, sql_template.format(view=view))
        for name, sql_template in queries.items()
        for view in VIEWS
    ]

    # Warm every query/view pair before measurement. This intentionally
    # measures warm-cache behavior; cold-cache/storage-layout experiments are
    # a separate workload and must be labelled separately.
    for _ in range(warmups):
        for _, _, sql in workload:
            con.execute(sql).fetchall()

    rng = random.Random(seed)
    observations = {(name, view): [] for name, view, _ in workload}
    digests = {(name, view): set() for name, view, _ in workload}
    row_counts = {(name, view): set() for name, view, _ in workload}

    for iteration in range(repeats):
        shuffled = workload.copy()
        rng.shuffle(shuffled)
        for name, view, sql in shuffled:
            with logfire.span(
                "query", query_name=name, view=view, iteration=iteration, cache_state="warm"
            ):
                t0 = time.perf_counter()
                rows = con.execute(sql).fetchall()
                elapsed_ms = (time.perf_counter() - t0) * 1000
            observations[(name, view)].append(elapsed_ms)
            digests[(name, view)].add(_result_digest(rows))
            row_counts[(name, view)].add(len(rows))

    results = []
    for name in queries:
        for view in VIEWS:
            elapsed = observations[(name, view)]
            result_hashes = digests[(name, view)]
            counts = row_counts[(name, view)]
            results.append(
                {
                    "query": name,
                    "view": view,
                    "cache_state": "warm",
                    "warmups": warmups,
                    "repeats": repeats,
                    "seed": seed,
                    "median_ms": statistics.median(elapsed),
                    "p95_ms": _percentile(elapsed, 0.95),
                    "min_ms": min(elapsed),
                    "max_ms": max(elapsed),
                    "result_hash": next(iter(result_hashes)) if len(result_hashes) == 1 else None,
                    "result_stable": len(result_hashes) == 1 and len(counts) == 1,
                    "result_rows": next(iter(counts)) if len(counts) == 1 else None,
                }
            )

    by_query = {
        name: {r["result_hash"] for r in results if r["query"] == name}
        for name in queries
    }
    for result in results:
        hashes = by_query[result["query"]]
        result["views_match"] = None not in hashes and len(hashes) == 1
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    telemetry.configure()
    results = run(warmups=args.warmups, repeats=args.repeats, seed=args.seed)

    print(f"{'query':<20}{'view':<16}{'median_ms':>12}{'p95_ms':>12}{'match':>8}")
    for r in results:
        print(
            f"{r['query']:<20}{r['view']:<16}"
            f"{r['median_ms']:>12.1f}{r['p95_ms']:>12.1f}{str(r['views_match']):>8}"
        )

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
