"""Loads the poller's durable ingest-time manifest(s)
(tools/poller/internal/manifest) and checks tools/compare's live numbers
against them.

`cohort_signature` (queries.sql, run via query_bench.py) already proves the
three comparison views agree with EACH OTHER at query time. It can't prove
that agreement matches what the poller actually, durably committed at
ingest time -- e.g. if the Beam pipeline were re-run after S3 drifted, all
three views could agree with each other on a cohort that isn't the one
originally ingested, and cohort_signature would report a clean
reconciliation on the wrong data. The manifest is the independent, durable
record this module checks against -- see docs/review-telemetry-plan.md's
"the comparison cohort is uncontrolled" finding and
tools/poller/internal/manifest's package doc.
"""

import json
from pathlib import Path

import duckdb

from compare import sizes as sizes_module
from compare.db import DEFAULT_DATA_DIR, DEFAULT_POLLER_CONFIG, connect, load_poller_config

DEFAULT_MANIFEST_NAME = "ledger.manifest.json"


def manifest_paths(poller_config_path: Path = DEFAULT_POLLER_CONFIG) -> list[Path]:
    """Resolves which manifest file(s) to check against, mirroring
    aws.s3_prefixes' pattern (compare-only, additive to config.yaml): an
    optional top-level `manifest_files` key lists more than one manifest for
    a cohort spanning several prefixes (e.g. a multi-day --s3-prefix
    backfill, one manifest per prefix/ledger file); absent, this defaults to
    the single manifest a plain `--once` run writes next to config.yaml."""
    cfg = load_poller_config(poller_config_path)
    poller_dir = poller_config_path.parent
    names = cfg.get("manifest_files") or [DEFAULT_MANIFEST_NAME]
    return [poller_dir / name for name in names]


def load(paths: list[Path]) -> list[dict]:
    manifests = []
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(
                f"{p} not found -- run `poller --mode=local --once` first (see docs/runbook.md)"
            )
        manifests.append(json.loads(p.read_text()))
    return manifests


def combined_totals(manifests: list[dict]) -> dict:
    """Sums per-manifest totals -- correct as long as each manifest covers a
    disjoint prefix (the normal case: one manifest per day/backfill). A
    cohort assembled from overlapping prefixes would double-count; nothing
    here detects that, matching compare/db.py's existing aws.s3_prefixes
    assumption of disjoint ranges."""
    return {
        "cohort_ids": sorted({m["cohort_id"] for m in manifests}),
        "object_count": sum(m["object_count"] for m in manifests),
        "total_bytes": sum(m["total_bytes"] for m in manifests),
        "total_records": sum(m["total_records"] for m in manifests),
    }


def check(con: duckdb.DuckDBPyConnection, sizes_result: dict, manifests: list[dict]) -> dict:
    """Cross-checks tools/compare's live S3 listing and view row counts
    against the poller's durable manifest(s). Never raises -- returns a
    result dict with a `pass` boolean per check and an overall `all_pass`;
    report.py decides how to fail closed on the result, the same pattern
    query_bench.py's `views_match` already uses."""
    totals = combined_totals(manifests)
    checks = {}

    live_objects = sizes_result["s3_gzip_original"]["files"]
    live_bytes = sizes_result["s3_gzip_original"]["bytes"]
    checks["s3_object_count"] = {
        "manifest": totals["object_count"],
        "live": live_objects,
        "pass": live_objects == totals["object_count"],
    }
    checks["s3_compressed_bytes"] = {
        "manifest": totals["total_bytes"],
        "live": live_bytes,
        "pass": live_bytes == totals["total_bytes"],
    }

    # Record counts: the manifest's total_records is what the poller itself
    # counted while writing each object's records (cmd/poller's
    # committedObject.RecordsWritten) -- comparing every view against that
    # one durable number, rather than only against each other, is what
    # catches a regenerated tier2/tier3 that drifted from the original
    # ingest even though the three views still agree with each other.
    for view in ("s3_baseline", "tier2_parquet", "tier3_avro"):
        live_records = con.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
        checks[f"{view}_record_count"] = {
            "manifest": totals["total_records"],
            "live": live_records,
            "pass": live_records == totals["total_records"],
        }

    return {
        "cohort_ids": totals["cohort_ids"],
        "checks": checks,
        "all_pass": all(c["pass"] for c in checks.values()),
    }


def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    poller_config_path: Path = DEFAULT_POLLER_CONFIG,
    *,
    sizes_result: dict | None = None,
) -> dict:
    """Standalone entry point (also used by report.py): loads the
    manifest(s), connects, and runs check(). Pass an already-computed
    sizes_result (report.py's collect() calls sizes.run() first anyway) to
    avoid a second live S3 listing; omitted, this computes its own.

    Raises FileNotFoundError if no manifest exists yet -- callers that want
    a soft failure (e.g. report.py, which should still render a report
    explaining what's missing) catch it rather than this function
    swallowing it, so "no manifest" is never silently treated as "nothing
    to check"."""
    manifests = load(manifest_paths(poller_config_path))
    con = connect(data_dir, poller_config_path)
    if sizes_result is None:
        sizes_result = sizes_module.run(data_dir, poller_config_path)
    return check(con, sizes_result, manifests)


def main():
    result = run()
    print(f"cohort_ids: {', '.join(result['cohort_ids'])}")
    print(f"{'check':<28}{'manifest':>14}{'live':>14}{'pass':>8}")
    for name, c in result["checks"].items():
        print(f"{name:<28}{c['manifest']:>14}{c['live']:>14}{str(c['pass']):>8}")
    if not result["all_pass"]:
        raise SystemExit("manifest reconciliation failed: see mismatched checks above")


if __name__ == "__main__":
    main()
