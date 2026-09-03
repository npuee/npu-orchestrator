#!/usr/bin/env bash
# ==============================================================================
# NetBox Zero-Touch Auto-Setup for NPU Orchestrator
# ==============================================================================
# Automatically populates a fresh NetBox instance with all required
# Custom Fields, Roles, Clusters, Sites, Config Contexts, Webhooks,
# and Custom Links, then auto-updates config.yml with provisioned IDs.
# ==============================================================================
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "======================================================================"
echo "          NPU Orchestrator - NetBox Auto-Configuration Setup          "
echo "======================================================================"

if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found. Please create .env first with NETBOX_URL and NETBOX_TOKEN."
    exit 1
fi

# Load .env for use by the Python script
set -a; source .env; set +a

echo "🔧 Running NetBox bootstrapper (this will create all required NetBox objects)..."
# Always run on the host so config.yml can be written (container mounts it read-only)
python3 -m app.scripts.bootstrap_netbox

echo ""
echo "📦 Triggering initial CT template sync from Proxmox to NetBox..."
if docker ps --format '{{.Names}}' | grep -q "^npu-orchestrator$"; then
    curl -s -X POST -H "X-API-Key: ${API_KEY}" http://localhost:8090/api/v1/sync/platforms | python3 -m json.tool 2>/dev/null || true
fi

echo ""
echo "Verifying overall system readiness..."
./check-config.sh

