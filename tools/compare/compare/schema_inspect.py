"""Side-by-side schema dump for Tier 2 (Parquet) vs Tier 3 (Avro) -- see
PLAN.md ("Build / verification order", step 10)."""

from pathlib import Path

import logfire

from compare import telemetry
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, connect


def run(data_dir: Path = DEFAULT_DATA_DIR, poller_config_path: Path = DEFAULT_POLLER_CONFIG):
    con = connect(data_dir, poller_config_path)
    tier2 = {
        row[0]: row[1] for row in con.execute("DESCRIBE SELECT * FROM tier2_parquet").fetchall()
    }
    tier3 = {
        row[0]: row[1] for row in con.execute("DESCRIBE SELECT * FROM tier3_avro").fetchall()
    }
    return tier2, tier3


def main():
    telemetry.configure()
    with logfire.span("schema_inspect"):
        tier2, tier3 = run()

    all_cols = sorted(set(tier2) | set(tier3))
    print(f"{'column':<28}{'tier2_parquet':<45}{'tier3_avro':<45}")
    for col in all_cols:
        t2 = tier2.get(col, "-- MISSING --")
        t3 = tier3.get(col, "-- MISSING --")
        flag = "  <-- differs" if t2 != t3 else ""
        print(f"{col:<28}{t2:<45}{t3:<45}{flag}")

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
