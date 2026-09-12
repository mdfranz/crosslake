"""Pure encode-time comparison: Avro (fastavro) vs Parquet (pyarrow), same
process, same already-typed rows, same compression family (deflate/gzip) --
answers "which format is faster/smaller to encode" without conflating it
with the poller/Beam languages' own runtime overhead (Go vs Python), which
a cross-service timing comparison would.

Rows come from the real tier2_parquet view (read via DuckDB's Arrow export)
rather than being re-synthesized, so both encoders see identical data.
"""

import argparse
import io
import json
import math
import random
import statistics
import time
from pathlib import Path

import fastavro
import logfire
import pyarrow.parquet as pq

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, REPO_ROOT, connect

AVRO_SCHEMA_PATH = REPO_ROOT / "tools" / "poller" / "schema" / "cloudtrail.avsc"
DEFAULT_WARMUPS = 1
DEFAULT_REPEATS = 5
DEFAULT_SEED = 1729


def _encode_parquet(table) -> tuple[float, int]:
    buf = io.BytesIO()
    t0 = time.perf_counter()
    pq.write_table(table, buf, compression="gzip")
    return (time.perf_counter() - t0) * 1000, buf.tell()


def _encode_avro(schema: dict, rows: list[dict]) -> tuple[float, int]:
    buf = io.BytesIO()
    t0 = time.perf_counter()
    fastavro.writer(buf, schema, rows, codec="deflate")
    return (time.perf_counter() - t0) * 1000, buf.tell()


def _summarize(observations: list[tuple[float, int]], n_records: int) -> dict:
    elapsed = [item[0] for item in observations]
    sizes = {item[1] for item in observations}
    median_ms = statistics.median(elapsed)
    p95_ms = sorted(elapsed)[max(0, math.ceil(0.95 * len(elapsed)) - 1)]
    return {
        "median_ms": median_ms,
        "p95_ms": p95_ms,
        "min_ms": min(elapsed),
        "max_ms": max(elapsed),
        "bytes": next(iter(sizes)) if len(sizes) == 1 else None,
        "size_stable": len(sizes) == 1,
        "records_per_sec": n_records / (median_ms / 1000) if median_ms else None,
    }


def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    poller_config_path: Path = DEFAULT_POLLER_CONFIG,
    *,
    warmups: int = DEFAULT_WARMUPS,
    repeats: int = DEFAULT_REPEATS,
    seed: int = DEFAULT_SEED,
) -> dict:
    if warmups < 0:
        raise ValueError("warmups must be >= 0")
    if repeats < 1:
        raise ValueError("repeats must be >= 1")

    con = connect(data_dir, poller_config_path)
    table = con.execute("SELECT * FROM tier2_parquet").to_arrow_table()
    rows = table.to_pylist()
    n = len(rows)
    avro_schema = fastavro.parse_schema(json.loads(AVRO_SCHEMA_PATH.read_text()))

    encoders = {
        "parquet": lambda: _encode_parquet(table),
        "avro": lambda: _encode_avro(avro_schema, rows),
    }
    for _ in range(warmups):
        for encode in encoders.values():
            encode()

    observations = {name: [] for name in encoders}
    rng = random.Random(seed)
    for iteration in range(repeats):
        formats = list(encoders)
        rng.shuffle(formats)
        for fmt in formats:
            with logfire.span("encode", format=fmt, n_records=n, iteration=iteration):
                observations[fmt].append(encoders[fmt]())

    return {
        "n_records": n,
        "warmups": warmups,
        "repeats": repeats,
        "seed": seed,
        "parquet": _summarize(observations["parquet"], n),
        "avro": _summarize(observations["avro"], n),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    telemetry.configure()
    result = run(warmups=args.warmups, repeats=args.repeats, seed=args.seed)
    print(f"records: {result['n_records']}")
    print(f"{'format':<10}{'median_ms':>12}{'p95_ms':>12}{'bytes':>10}{'records/sec':>14}")
    for fmt in ("parquet", "avro"):
        r = result[fmt]
        bytes_display = str(r["bytes"]) if r["bytes"] is not None else "varies"
        throughput = r["records_per_sec"] or 0
        print(
            f"{fmt:<10}{r['median_ms']:>12.1f}{r['p95_ms']:>12.1f}"
            f"{bytes_display:>10}{throughput:>14.0f}"
        )

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
