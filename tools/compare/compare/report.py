"""Runs sizes + schema_inspect + query_bench and renders one Markdown
section, suitable for pasting into LEARNINGS.md -- see PLAN.md ("Build /
verification order", step 10)."""

from datetime import datetime, timezone

import logfire

from compare import encode_bench, query_bench, schema_inspect, sizes, telemetry


def render_markdown() -> str:
    size_result = sizes.run()
    tier2_schema, tier3_schema = schema_inspect.run()
    bench_result = query_bench.run()
    encode_result = encode_bench.run()

    baseline = size_result["s3_gzip_original"]["bytes"]
    lines = [
        f"## Comparison run: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "### Sizes (vs. original gzipped CloudTrail JSON in S3)",
        "",
        "| source | bytes | vs S3 gzip |",
        "|---|---:|---:|",
    ]
    for name, info in size_result.items():
        b = info["bytes"]
        ratio = f"{b / baseline:.2f}x" if baseline else "n/a"
        lines.append(f"| {name} | {b:,} | {ratio} |")

    lines += ["", "### Schema differences (Tier 2 Parquet vs Tier 3 Avro)", ""]
    diffs = [
        f"- `{col}`: parquet=`{tier2_schema.get(col, 'MISSING')}` vs avro=`{tier3_schema.get(col, 'MISSING')}`"
        for col in sorted(set(tier2_schema) | set(tier3_schema))
        if tier2_schema.get(col) != tier3_schema.get(col)
    ]
    lines += diffs if diffs else ["- No column-level differences."]

    lines += ["", "### Query benchmark (identical SQL across all three sources)", ""]
    lines += ["| query | view | elapsed_ms |", "|---|---|---:|"]
    for r in bench_result:
        lines.append(f"| {r['query']} | {r['view']} | {r['elapsed_ms']:.1f} |")

    lines += [
        "",
        "### Encode benchmark (same process, same rows, deflate/gzip both sides)",
        "",
        f"`{encode_result['n_records']}` records.",
        "",
        "| format | ms | bytes | records/sec |",
        "|---|---:|---:|---:|",
    ]
    for fmt in ("parquet", "avro"):
        r = encode_result[fmt]
        lines.append(f"| {fmt} | {r['ms']:.1f} | {r['bytes']:,} | {r['records_per_sec']:.0f} |")

    return "\n".join(lines) + "\n"


def main():
    telemetry.configure()
    with logfire.span("report"):
        md = render_markdown()
    print(md)

    import logfire as lf

    lf.force_flush()


if __name__ == "__main__":
    main()
