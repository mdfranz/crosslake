"""Shared Logfire configuration for the compare tool.

Reads the write token from the LOGFIRE_TOKEN env var (see
`scripts/logfire-env.sh` and docs/observability.md).
"""

import subprocess
from pathlib import Path

import logfire


def _git_sha() -> str | None:
    """Short git commit SHA for Logfire's service_version -- lets the
    Services page compare error rate/latency across commits, which this
    project has iterated on heavily. None (omitted from the resource)
    outside a git checkout or if git isn't on PATH."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parents[3],  # repo root
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
        return result.stdout.strip()
    except Exception:  # noqa: BLE001 -- version metadata is best-effort
        return None


def configure(service_name: str = "crosslake-compare") -> None:
    logfire.configure(
        service_name=service_name,
        service_version=_git_sha(),
        environment="local",  # vs. a future "cloud" mode -- see PLAN.md
        advanced=logfire.AdvancedOptions(base_url="https://logfire-us.pydantic.dev"),
    )
