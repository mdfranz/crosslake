"""Pure encode-time comparison: Avro (fastavro) vs Parquet (pyarrow), same
process, same already-typed rows, same compression family (deflate/gzip) --
answers "which format is faster/smaller to encode" without conflating it
with the poller/Beam languages' own runtime overhead (Go vs Python), which
a cross-service timing comparison would.

Rows come from the real tier2_parquet view (read via DuckDB's Arrow export)
rather than being re-synthesized, so both encoders see identical data.
"""

import io
import json
import time
from pathlib import Path

import fastavro
import logfire
import pyarrow.parquet as pq

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, REPO_ROOT, connect

AVRO_SCHEMA_PATH = REPO_ROOT / "tools" / "poller" / "schema" / "cloudtrail.avsc"


def run(data_dir: Path = DEFAULT_DATA_DIR, poller_config_path: Path = DEFAULT_POLLER_CONFIG) -> dict:
    con = connect(data_dir, poller_config_path)
    table = con.execute("SELECT * FROM tier2_parquet").to_arrow_table()
    rows = table.to_pylist()
    n = len(rows)

    with logfire.span("encode_parquet", n_records=n):
        buf = io.BytesIO()
        t0 = time.perf_counter()
        pq.write_table(table, buf, compression="gzip")
        parquet_ms = (time.perf_counter() - t0) * 1000
        parquet_bytes = buf.tell()

    avro_schema = fastavro.parse_schema(json.loads(AVRO_SCHEMA_PATH.read_text()))
    with logfire.span("encode_avro", n_records=n):
        buf = io.BytesIO()
        t0 = time.perf_counter()
        fastavro.writer(buf, avro_schema, rows, codec="deflate")
        avro_ms = (time.perf_counter() - t0) * 1000
        avro_bytes = buf.tell()

    return {
        "n_records": n,
        "parquet": {"ms": parquet_ms, "bytes": parquet_bytes, "records_per_sec": n / (parquet_ms / 1000)},
        "avro": {"ms": avro_ms, "bytes": avro_bytes, "records_per_sec": n / (avro_ms / 1000)},
    }


def main():
    telemetry.configure()
    result = run()
    print(f"records: {result['n_records']}")
    print(f"{'format':<10}{'ms':>10}{'bytes':>10}{'records/sec':>14}")
    for fmt in ("parquet", "avro"):
        r = result[fmt]
        print(f"{fmt:<10}{r['ms']:>10.1f}{r['bytes']:>10}{r['records_per_sec']:>14.0f}")

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
