#!/usr/bin/env bash
# ==============================================================================
# NPU Orchestrator - Pre-Flight Diagnostic & Config Verification
# ==============================================================================
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "Running NPU Orchestrator Pre-Flight Verification..."

if docker ps --format '{{.Names}}' | grep -q "^npu-orchestrator$"; then
    # Container is running: run diagnostics inside the running container
    docker exec -t npu-orchestrator python3 -m app.core.preflight
else
    # Container is not running: run in a temporary disposable container and auto-remove (--rm)
    docker compose run --rm orchestrator python3 -m app.core.preflight
fi
