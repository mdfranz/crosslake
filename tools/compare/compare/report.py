"""Runs the comparison and writes Markdown plus a machine-readable, gitignored
run artifact. The artifact is the evidence record; telemetry is its searchable
summary."""

import argparse
import importlib.metadata
import json
import platform
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import logfire

from compare import encode_bench, manifest, query_bench, schema_inspect, sizes, telemetry
from compare.db import DEFAULT_DATA_DIR, REPO_ROOT


def _git_metadata() -> dict:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()

    return {"commit": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain"))}


def _package_versions() -> dict:
    names = ["apache-beam", "duckdb", "fastavro", "logfire", "pyarrow"]
    versions = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def collect() -> dict:
    size_result = sizes.run()
    tier2_schema, tier3_schema = schema_inspect.run()
    bench_result = query_bench.run()
    encode_result = encode_bench.run()

    cohort_results = [r for r in bench_result if r["query"] == "cohort_signature"]
    cohort_reconciled = bool(cohort_results) and all(
        r["views_match"] and r["result_stable"] for r in cohort_results
    )
    cohort_id = cohort_results[0]["result_hash"][:16] if cohort_reconciled else None

    # cohort_signature (above) only proves the three views agree with EACH
    # OTHER right now; it can't prove that agreement matches what the
    # poller actually, durably committed at ingest time. manifest_result
    # checks against that independent, durable record instead -- see
    # compare/manifest.py's module doc. Missing manifest files are a
    # reportable failure, not a silent skip: cohort trust has no fallback
    # when there's nothing durable to check against.
    try:
        manifest_result = manifest.run(sizes_result=size_result)
    except FileNotFoundError as e:
        manifest_result = {"error": str(e), "cohort_ids": [], "checks": {}, "all_pass": False}

    return {
        "artifact_schema": 1,
        "run_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "experiment": "local_mode_smoke",
        "cohort": {"id": cohort_id, "reconciled": cohort_reconciled},
        "manifest": manifest_result,
        "git": _git_metadata(),
        "runtime": {
            "python": platform.python_version(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "packages": _package_versions(),
        },
        "sizes": size_result,
        "schemas": {"tier2_parquet": tier2_schema, "tier3_avro": tier3_schema},
        "queries": bench_result,
        "encode": encode_result,
    }


def render_markdown(report: dict | None = None) -> str:
    report = report or collect()
    size_result = report["sizes"]
    tier2_schema = report["schemas"]["tier2_parquet"]
    tier3_schema = report["schemas"]["tier3_avro"]
    bench_result = report["queries"]
    encode_result = report["encode"]

    baseline = size_result["s3_gzip_original"]["bytes"]
    manifest_result = report["manifest"]
    lines = [
        f"## Comparison run: {report['created_at']}",
        "",
        f"Run ID: `{report['run_id']}`; commit: `{report['git']['commit'][:12]}`; "
        f"working tree dirty: `{report['git']['dirty']}`.",
        f"Cohort ID: `{report['cohort']['id'] or 'UNRECONCILED'}`; "
        f"reconciled: `{report['cohort']['reconciled']}`.",
        "",
        "### Manifest reconciliation (durable ingest-time record vs. live numbers)",
        "",
        "cohort_signature (below) only proves the three views agree with each "
        "other right now. This checks against the poller's independent, durable "
        "manifest instead -- see `compare/manifest.py`.",
        "",
    ]
    if manifest_result.get("error"):
        lines += [f"**FAIL**: {manifest_result['error']}", ""]
    else:
        lines += [
            f"Cohort ID(s): `{', '.join(manifest_result['cohort_ids'])}`; "
            f"all checks pass: `{manifest_result['all_pass']}`.",
            "",
            "| check | manifest | live | pass |",
            "|---|---:|---:|---|",
        ]
        for name, c in manifest_result["checks"].items():
            lines.append(f"| {name} | {c['manifest']:,} | {c['live']:,} | {c['pass']} |")
        lines.append("")

    lines += [
        "### Unreconciled size snapshot",
        "",
        "These ratios compare the current remote prefix with cumulative local output. "
        "Do not treat them as format evidence until the cohort signature matches.",
        "",
        "| source | bytes | vs current S3 prefix |",
        "|---|---:|---:|",
    ]
    for name, info in size_result.items():
        b = info["bytes"]
        ratio = (
            f"{b / baseline:.2f}x"
            if baseline and (name == "s3_gzip_original" or report["cohort"]["reconciled"])
            else "n/a"
        )
        lines.append(f"| {name} | {b:,} | {ratio} |")

    lines += ["", "### Schema differences (Tier 2 Parquet vs Tier 3 Avro)", ""]
    diffs = [
        f"- `{col}`: parquet=`{tier2_schema.get(col, 'MISSING')}` "
        f"vs avro=`{tier3_schema.get(col, 'MISSING')}`"
        for col in sorted(set(tier2_schema) | set(tier3_schema))
        if tier2_schema.get(col) != tier3_schema.get(col)
    ]
    lines += diffs if diffs else ["- No column-level differences."]

    lines += ["", "### Query benchmark (warm cache, randomized order)", ""]
    lines += [
        "| query | view | median_ms | p95_ms | repeats | views match |",
        "|---|---|---:|---:|---:|---|",
    ]
    for r in bench_result:
        match_display = "n/a" if r["views_match"] is None else r["views_match"]
        lines.append(
            f"| {r['query']} | {r['view']} | {r['median_ms']:.1f} | "
            f"{r['p95_ms']:.1f} | {r['repeats']} | {match_display} |"
        )

    lines += [
        "",
        "### Encode benchmark (same process, same rows, deflate/gzip both sides)",
        "",
        f"`{encode_result['n_records']}` records.",
        "",
        f"Warmups: `{encode_result['warmups']}`; randomized measured repeats: "
        f"`{encode_result['repeats']}`.",
        "",
        "| format | median_ms | p95_ms | bytes | records/sec |",
        "|---|---:|---:|---:|---:|",
    ]
    for fmt in ("parquet", "avro"):
        r = encode_result[fmt]
        bytes_display = f"{r['bytes']:,}" if r["bytes"] is not None else "varies"
        throughput = f"{r['records_per_sec']:.0f}" if r["records_per_sec"] is not None else "n/a"
        lines.append(
            f"| {fmt} | {r['median_ms']:.1f} | {r['p95_ms']:.1f} | "
            f"{bytes_display} | {throughput} |"
        )

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_DATA_DIR / "reports")
    parser.add_argument("--no-artifact", action="store_true")
    args = parser.parse_args()

    telemetry.configure()
    with logfire.span("report"):
        report = collect()
        md = render_markdown(report)
        logfire.info(
            "comparison completed",
            run_id=report["run_id"],
            cohort_id=report["cohort"]["id"],
            experiment=report["experiment"],
            git_commit=report["git"]["commit"],
            cohort_reconciled=report["cohort"]["reconciled"],
            manifest_all_pass=report["manifest"]["all_pass"],
            manifest_cohort_ids=report["manifest"]["cohort_ids"],
            # views_match is None (not False) for queries intentionally
            # scoped to one view (see query_bench.py) -- exclude those
            # rather than let all() treat None as a mismatch.
            query_views_match=all(
                r["views_match"] for r in report["queries"] if r["views_match"] is not None
            ),
        )

    if not args.no_artifact:
        args.artifact_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stem = f"{report['created_at'].replace(':', '')}_{report['run_id']}"
        json_path = args.artifact_dir / f"{stem}.json"
        md_path = args.artifact_dir / f"{stem}.md"
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        md_path.write_text(md)
        print(f"artifact: {json_path}")
    print(md)

    import logfire as lf

    lf.force_flush()
    failures = []
    if not report["cohort"]["reconciled"]:
        failures.append("tier cohort signatures do not match")
    if not report["manifest"]["all_pass"]:
        reason = report["manifest"].get("error") or "live numbers don't match the poller's manifest"
        failures.append(f"manifest reconciliation failed: {reason}")
    if failures:
        raise SystemExit("comparison failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
