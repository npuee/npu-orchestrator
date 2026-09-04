#!/usr/bin/env bash
# ==============================================================================
# NPU Orchestrator Appliance - Quickstart Deployment & Installer
# ==============================================================================
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "┌────────────────────────────────────────────────────────────────────┐"
echo "│              NPU Infrastructure Orchestrator Installer             │"
echo "└────────────────────────────────────────────────────────────────────┘"

# ── Step 1: Verify Prerequisites & Environment ────────────────────────────────
echo ""
echo " [1/6] Prerequisites & Environment"

if ! command -v docker >/dev/null 2>&1; then
    echo "       ✖ Error: Docker is not installed. Please install Docker first."
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "       ✖ Error: Docker Compose plugin is not installed."
    exit 1
fi

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "       ⚠️  No .env file found. Creating from .env.example..."
        cp .env.example .env
        echo ""
        echo "       ❗ ACTION REQUIRED:"
        echo "          Edit .env with your credentials, then rerun ./install.sh"
        exit 1
    else
        echo "       ✖ Error: Neither .env nor .env.example found."
        exit 1
    fi
fi

if [ ! -f "config.yml" ]; then
    if [ -f "config.example.yml" ]; then
        echo "       ⚠️  No config.yml found. Creating from config.example.yml..."
        cp config.example.yml config.yml
    else
        echo "       ✖ Error: Neither config.yml nor config.example.yml found."
        exit 1
    fi
fi

echo "       ✔ Docker runtime detected"
echo "       ✔ Environment (.env) and topology (config.yml) loaded"

# ── Step 2: Build Container Image ─────────────────────────────────────────────
echo ""
echo " [2/6] Container Image Compilation"
mkdir -p data
if docker compose build -q >/tmp/npu_build.log 2>&1; then
    echo "       ✔ Orchestrator container image compiled & ready"
else
    echo "       ✖ Container build failed. Details:"
    cat /tmp/npu_build.log
    exit 1
fi

# ── Step 3: Pre-Flight Diagnostics (Credentials & Connectivity) ───────────────
echo ""
echo " [3/6] Connectivity & Credential Pre-Flight"

if ! docker compose --progress quiet run --rm orchestrator python3 -m app.core.preflight --summary; then
    echo ""
    echo "       ✖ Pre-flight checks failed! Credentials or connectivity issue detected."
    echo "          Run './check-config.sh' for detailed diagnostics."
    exit 1
fi

# ── Step 4: Proxmox VE Resource Verification ──────────────────────────────────
echo ""
echo " [4/6] Proxmox VE Resource Verification"

if ! docker compose --progress quiet run --rm orchestrator python3 -m app.scripts.audit_proxmox --summary; then
    echo ""
    echo "       ✖ Proxmox VE resource verification failed."
    exit 1
fi

# ── Step 5: NetBox Schema Verification ────────────────────────────────────────
echo ""
echo " [5/6] NetBox Schema Verification"

if ! docker compose --progress quiet run --rm orchestrator python3 -m app.scripts.bootstrap_netbox --summary; then
    echo ""
    echo "       ✖ NetBox schema verification failed."
    exit 1
fi

# ── Step 6: Start Production Container & Verify Health ────────────────────────
echo ""
echo " [6/6] Production Launch"
docker compose --progress quiet up -d --no-build
echo "       ✔ Production container started"

HEALTHY=false
for i in {1..15}; do
    if curl -s -f http://127.0.0.1:8090/health >/dev/null 2>&1; then
        HEALTHY=true
        break
    fi
    sleep 1
done

if [ "$HEALTHY" = true ]; then
    echo "       ✔ Live health probe confirmed (HTTP 200 OK from /health)"
else
    echo "       ⚠️  Warning: Health probe did not respond within 15s."
    echo "          Check container logs: docker logs npu-orchestrator"
fi

echo ""
echo "──────────────────────────────────────────────────────────────────────"
echo "  🎉 Orchestrator is running and ready!"
echo ""
echo "  • Health Probe:    http://127.0.0.1:8090/health"
echo "  • API Docs:        http://127.0.0.1:8090/docs"
echo "  • Full 16-pt Test: ./check-config.sh"
echo "──────────────────────────────────────────────────────────────────────"
echo ""
