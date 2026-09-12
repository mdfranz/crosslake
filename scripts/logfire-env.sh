#!/usr/bin/env bash
# Source this (don't execute it) to export Logfire/OTEL env vars for local
# dev, read from the repo-root .logfire/logfire_credentials.json minted by:
#   uvx logfire --region=us auth
#   uvx logfire --region=us projects use --org <your-org> <your-project>
#
# One token is shared by the Go poller (via OTEL_EXPORTER_OTLP_*) and the
# Python components (via LOGFIRE_TOKEN, which `logfire.configure()` reads
# automatically) -- see docs/observability.md.
#
# Usage: source scripts/logfire-env.sh

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CREDS="$REPO_ROOT/.logfire/logfire_credentials.json"

if [ ! -f "$CREDS" ]; then
    echo "logfire-env: no credentials at $CREDS" >&2
    echo "  run: uvx logfire --region=us auth" >&2
    echo "  then: uvx logfire --region=us projects use --org <your-org> <your-project>" >&2
    return 1 2>/dev/null || exit 1
fi

LOGFIRE_TOKEN="$(python3 -c "import json; print(json.load(open('$CREDS'))['token'])")"
export LOGFIRE_TOKEN
export OTEL_EXPORTER_OTLP_ENDPOINT="https://logfire-us.pydantic.dev"
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=${LOGFIRE_TOKEN}"
echo "logfire-env: LOGFIRE_TOKEN and OTEL_EXPORTER_OTLP_* exported (token not printed)" >&2
