#!/usr/bin/env bash
# ==============================================================================
# NPU Orchestrator - Pre-Flight Diagnostic & Config Verification
# ==============================================================================
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "Running NPU Orchestrator Verification Suite..."

if docker ps --format '{{.Names}}' | grep -q "^npu-orchestrator$"; then
    # Container is running: execute diagnostics inside active container
    docker exec npu-orchestrator python3 -m app.core.preflight
    docker exec npu-orchestrator python3 -m app.scripts.audit_proxmox
    docker exec npu-orchestrator python3 -m app.scripts.bootstrap_netbox --check
else
    # Container is not running: execute in a temporary disposable container (--rm)
    docker compose run --rm orchestrator python3 -m app.core.preflight
    docker compose run --rm orchestrator python3 -m app.scripts.audit_proxmox
    docker compose run --rm orchestrator python3 -m app.scripts.bootstrap_netbox --check
fi
