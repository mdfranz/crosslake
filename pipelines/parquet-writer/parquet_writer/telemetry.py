"""Shared Logfire configuration for the Beam pipeline.

Reads the write token from the LOGFIRE_TOKEN env var (see
`scripts/logfire-env.sh` and docs/observability.md) rather than relying on
`.logfire/logfire_credentials.json` auto-discovery, which only looks in the
current working directory -- brittle here since Beam workers and DirectRunner
invocations don't reliably run from the repo root.
"""

import logfire


def configure(service_name: str = "crosslake-parquet-writer") -> None:
    logfire.configure(
        service_name=service_name,
        advanced=logfire.AdvancedOptions(base_url="https://logfire-us.pydantic.dev"),
        # Console output is intentionally quiet; one driver span and summary
        # are exported while element-level counts stay in Beam metrics.
        console=False,
    )
