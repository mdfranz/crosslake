"""Shared Logfire configuration for the compare tool.

Reads the write token from the LOGFIRE_TOKEN env var (see
`scripts/logfire-env.sh` and docs/observability.md).
"""

import logfire


def configure(service_name: str = "crosslake-compare") -> None:
    logfire.configure(
        service_name=service_name,
        advanced=logfire.AdvancedOptions(base_url="https://logfire-us.pydantic.dev"),
    )
