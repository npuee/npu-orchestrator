# NPU Infrastructure Orchestrator 🚀

> **Turn NetBox into an automated, event-driven Private Cloud Orchestrator for Proxmox VE.**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Proxmox VE](https://img.shields.io/badge/Proxmox_VE-8.x_|_9.x-E57000.svg?style=flat&logo=proxmox&logoColor=white)](https://proxmox.com)
[![NetBox](https://img.shields.io/badge/NetBox-4.x-004D40.svg?style=flat&logo=netbox&logoColor=white)](https://netboxlabs.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## ⚡ The Problem vs. The Solution

### Without NPU Orchestrator (Manual Hell)
1. Open Proxmox → manually clone a VM or container template.
2. Calculate an unused IP, configure the gateway, DNS, and bridge.
3. Boot machine → SSH in or open VNC console to inject keys, passwords, and resize disks.
4. Open your DNS server → manually create forward `A` and reverse `PTR` records.
5. Open NetBox → manually type in the VMID, RAM, vCPU, IP, and status.
6. *Did you remember to update reverse proxy routes? Did someone take an IP collision?*
⏱️ **Time lost: 20–30 minutes per machine.**

### With NPU Orchestrator (Event-Driven Cloud)
1. In NetBox, click **Add Virtual Machine**:
   - **Name**: `db-node-01`
   - **Platform**: `Ubuntu 24.04 LTS (Noble) (VMID: 9024)` *(Auto-synced from Proxmox)*
   - **VM Type**: `Medium (2C / 4GB)` *(Managed by you in NetBox)*
2. Click **Save**.
☕ **That's it.** Within ~45 seconds, the VM is cloned, Cloud-Init configured, IP allocated, DNS records registered, disk expanded, and live status updated.

---

## 🏗️ Architectural Philosophy

NPU Orchestrator adheres strictly to the **NetBox as Single Source of Truth (SSoT)** principle with complete separation of concerns:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        NetBox Source of Truth                          │
│                                                                        │
│   ┌───────────────────────────┐     ┌──────────────────────────────┐   │
│   │     Platform (The OS)     │     │      VM Type (The Flavor)    │   │
│   │  Auto-synced from Proxmox │     │    User-managed in NetBox    │   │
│   │   • Ubuntu 24.04 (9024)   │     │    • Micro:   1C / 1G / 15G  │   │
│   │   • Ubuntu 26.04 (9026)   │  +  │    • Small:   2C / 2G / 25G  │   │
│   │   • Win Server 2025 (9225)│     │    • Medium:  2C / 4G / 40G  │   │
│   │   • Debian 12 LXC         │     │    • Large:   4C / 8G / 80G  │   │
│   └─────────────┬─────────────┘     └──────────────┬───────────────┘   │
│                 │                                  │                   │
│                 └─────────────────┬────────────────┘                   │
│                                   ▼                                    │
│                    Virtual Machine: db-node-01                         │
│                    • Status: Active                                    │
│                    • Primary IP: 192.168.1.50/24                          │
│                    • Cluster: Lohusuu PVE Cluster                      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │  Webhook Event (HMAC-SHA512)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      NPU Automation Orchestrator                       │
│    FastAPI Core  •  Async Worker Engine  •  Proxmox & NetBox Drivers   │
└───────────────────┬────────────────────────────────┬───────────────────┘
                    │                                │
                    ▼                                ▼
       ┌────────────────────────┐       ┌────────────────────────┐
       │   Proxmox VE Cluster   │       │     NetBox DNS / IPAM  │
       │  • Clone VM/CT         │       │  • Forward 'A' Records │
       │  • Apply vCPU/RAM/Disk │       │  • Reverse 'PTR' Record│
       │  • Inject Cloud-Init   │       │  • IP Allocation Guard │
       │  • Start on Boot       │       │  • Telemetry Metric Sync│
       └────────────────────────┘       └────────────────────────┘
```

---

## ✨ Features at a Glance

### 🚀 Zero-Touch Automated Provisioning
- **Linux VMs (KVM/QEMU)**: Instant full/linked cloning, Cloud-Init network configuration, automated SSH authorized_keys injection, and disk expansion.
- **Windows Server VMs**: Automated cloning with Sysprep / Cloudbase-Init compatibility and Administrator credential provisioning.
- **LXC System Containers**: Instant unprivileged container creation from Proxmox `.tar.zst` storage archives with static networking and SSH key injection.

### 🔄 Bidirectional Proxmox ➔ NetBox Template Sync
- **Full Discovery**: Continuously monitors Proxmox for both **QEMU VM templates** (VMID 9000–9299) and **LXC Container archives** on all storage pools.
- **Deterministic Metadata**: Automatically registers each template as a clean `Platform` with exact reference IDs (`[Proxmox VM Template: 9024]`).
- **Safe Orphan Deletion**: When you delete a template from Proxmox:
  - If **0 VMs** are using it: Automatically deleted from NetBox.
  - If **active VMs** are using it: Automatically marked `[Deprecated]` to safeguard historical inventory data without breaking foreign keys.

### 🎛️ 100% User-Managed Hardware Sizing
- Never edit configuration files to add a new server size! Manage all flavors directly inside **NetBox UI** (`Virtualization → VM Types`).
- Define `Micro (1C/1G)`, `Standard (2C/4G)`, `Heavy (8C/16G)` or custom sizes.
- Select a VM Type, and NetBox automatically pre-fills the vCPU, Memory, and Disk sliders.
- Emergency fallbacks in `config.yml` protect against unassigned fields.

### 📊 Real-Time Telemetry & Metric Sync
- Periodically queries Proxmox VE performance counters.
- Automatically pushes **24-hour average & peak CPU usage**, **RAM allocation**, **disk consumption**, and **uptime status** back into NetBox custom fields.

### 🌐 Traefik Ingress Synchronization
- Scans Traefik reverse proxy instances (Docker socket or HTTP API).
- Automatically catalogs routers, services, domain names, TLS status, and auth middleware inside NetBox.

### 🔒 Enterprise Hardened
- **Signature Verification**: Every incoming webhook is authenticated using HMAC-SHA512 signature checking.
- **API Key RBAC**: REST endpoints are guarded by high-entropy API authentication headers.
- **Secure Sandbox**: Runs strictly as an unprivileged, non-root user (`appuser:10001`) inside lightweight Docker containers.

---

## ⚡ Quickstart Deployment

### Prerequisites
- Linux host with **Docker** & **Docker Compose v2+** installed.
- Network connectivity to your **NetBox** instance and **Proxmox VE** cluster.
- *(No local Python dependencies required on the host machine!)*

### Step 1: Clone the Repository
```bash
git clone https://github.com/npu-ee/npu-orchestrator.git
cd npu-orchestrator
```

### Step 2: Configure Secrets & Topology
```bash
# Generate your secrets configuration
cp .env.example .env
nano .env
```
Fill in your credentials:
```ini
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your_netbox_api_token
PROXMOX_HOST=192.168.1.100
PROXMOX_USER=root@pam
PROXMOX_TOKEN_NAME=automation
PROXMOX_TOKEN_VALUE=your_pve_token_secret
API_KEY=choose_a_strong_orchestrator_api_key
```

### Step 3: Run the Unified Installer
```bash
chmod +x install.sh
./install.sh
```

The unified installer executes a strict, logical **5-step pre-flight deployment**:

```text
┌────────────────────────────────────────────────────────────────────┐
│              NPU Infrastructure Orchestrator Installer             │
└────────────────────────────────────────────────────────────────────┘

 [1/5] Prerequisites & Environment
       ✔ Docker runtime detected
       ✔ Environment (.env) and topology (config.yml) loaded

 [2/5] Container Image Compilation
       ✔ Orchestrator container image compiled & ready

 [3/5] Connectivity & Credential Pre-Flight
       ✔ Credentials & Security: NetBox Token, Proxmox Token, Webhook Secret
       ✔ NetBox Topology: Site: 'Default Site', Cluster: 'Default PVE Cluster', Tenant: 'Primary Tenant'
       ✔ Hypervisor: Connected to Proxmox VE 9.2.4-9.2 on 192.168.1.100:8006 (Storage & Bridge online)
       ✔ All 16/16 diagnostic probes passed (16/16)

 [4/5] NetBox Schema Verification
       ✔ NetBox v4.6.9 connected (https://netbox.example.com)
       ✔ 13 Custom Fields verified
       ✔ Virtual Machine & LXC Container roles verified
       ✔ Webhook & 1-Click deploy blueprints active

 [5/5] Production Launch
       ✔ Production container started
       ✔ Live health probe confirmed (HTTP 200 OK from /health)

──────────────────────────────────────────────────────────────────────
  🎉 Orchestrator is running and ready!

  • Health Probe:    http://127.0.0.1:8090/health
  • API Docs:        http://127.0.0.1:8090/docs
  • Full 16-pt Test: ./check-config.sh
──────────────────────────────────────────────────────────────────────
```

---

## 🔍 Live Diagnostics (`check-config.sh`)

At any time, run the standalone diagnostics engine to audit your entire infrastructure pipeline:

```bash
./check-config.sh
```

Performs **16 live verification probes**:
- NetBox & Proxmox authentication tokens
- Default Tenant, Site, Cluster, and Role IDs
- Proxmox node reachability, storage pools, and network bridges
- NetBox DNS plugin zone availability
- Webhook endpoints and HMAC secret validation

---

## 🖥️ How to Deploy Machines via NetBox

### 1. Define your Hardware Flavors (Once)
In NetBox, navigate to: **Virtualization ➔ VM Types ➔ Add VM Type**:
- **Name**: `Small` | **vCPUs**: `2` | **RAM**: `2048 MB` | **Disk**: `20 GB`
- **Name**: `Medium` | **vCPUs**: `2` | **RAM**: `4096 MB` | **Disk**: `40 GB`
- **Name**: `Large` | **vCPUs**: `4` | **RAM**: `8192 MB` | **Disk**: `80 GB`

### 2. Deploy a Virtual Machine
Navigate to: **Virtualization ➔ Virtual Machines ➔ Add**:
1. **Name**: `web-01`
2. **Platform**: Select any template synced from Proxmox (e.g. `Ubuntu 24.04 LTS (Noble) (VMID: 9024)`)
3. **VM Type**: Select your desired flavor (e.g. `Medium`)
4. **Primary IPv4**: Enter static IP (e.g. `192.168.1.45/24`) or let DHCP assign.
5. Click **Save**.

The webhook triggers instantly. The orchestrator:
- Clones the target Proxmox template.
- Sets CPU cores, RAM, and expands root disk to match your VM Type.
- Configures Cloud-Init with network IP, gateway, and SSH keys.
- Creates DNS `A` and `PTR` records.
- Powers on the machine.

---

## ⚙️ Configuration Files

### `config.yml`
Controls cluster defaults, hardware fallbacks, and integration services. Comments and indentation are preserved automatically:

```yaml
version: "1.0"

# ── Infrastructure Defaults ───────────────────────────────────────────────────
defaults:
  tenant_id: 1            # NetBox Tenant ID
  site_id: 2              # NetBox Site ID
  cluster_id: 2           # NetBox Virtualization Cluster ID
  role_vm_id: 16          # Role ID for KVM Virtual Machines
  role_lxc_id: 15         # Role ID for LXC Containers
  storage: "zfs-storage"  # Proxmox storage pool name
  bridge: "vmbr0"         # Proxmox network bridge

# ── Hardware Fallbacks ────────────────────────────────────────────────────────
# Emergency fallbacks used only if a VM is created without a VM Type or sliders.
fallbacks:
  cores: 2                # Default vCPU cores
  memory_mb: 2048         # Default RAM in MB (2048 = 2GB)
  disk_gb: 20             # Default disk size in GB

# ── Proxmox Template Settings ─────────────────────────────────────────────────
templates:
  linux_vmid_prefix: "90"           # Linux templates live in VMID range 9000-9099
  windows_vmid_prefix: "92"         # Windows templates live in VMID range 9200-9299
  default_windows_password: "P@ssw0rdInitial!"

# ── Traefik Ingress Synchronization ───────────────────────────────────────────
traefik:
  enabled: true
  sync_interval_minutes: 15
  instances:
    - name: "traefik-main"
      netbox_vm_id: 7
      type: "docker"
      path: "/cloud/traefik"
    - name: "traefik-api"
      netbox_vm_id: 6
      type: "api"
      url: "http://192.168.1.50:8080"

# ── DNS Automation (NetBox DNS Plugin) ────────────────────────────────────────
dns:
  default_zone: "homelab.local"         # Internal DNS domain in NetBox DNS
  auto_register_a: true             # Auto-create forward 'A' records (hostname -> IP)
  auto_register_ptr: true           # Auto-create reverse 'PTR' records (IP -> hostname)
```

---

## 📡 REST API Reference

Interactive OpenAPI documentation is live at `http://<your-server-ip>:8090/docs`.

| Method | Path | Description | Auth Required |
| :---: | :--- | :--- | :---: |
| `POST` | `/api/v1/webhooks/netbox` | Handles real-time NetBox lifecycle events | HMAC Signature |
| `POST` | `/api/v1/sync/platforms` | Discovers Proxmox templates & reconciles NetBox Platforms | `X-API-Key` |
| `POST` | `/api/v1/sync/metrics` | Pulls live CPU/RAM/Disk metrics from Proxmox into NetBox | `X-API-Key` |
| `POST` | `/api/v1/sync/traefik` | Synchronizes Traefik ingress routes to NetBox services | `X-API-Key` |
| `GET` | `/api/v1/system/preflight` | Executes full 16-point infrastructure diagnostic suite | `X-API-Key` |
| `GET` | `/api/v1/jobs` | Lists historical provisioning & background task jobs | `X-API-Key` |
| `GET` | `/api/v1/jobs/{job_id}` | Streams real-time terminal execution logs for a job | `X-API-Key` |
| `GET` | `/health` | Live HTTP probe for container health monitoring | *None* |

---

## 📁 Repository Structure

```text
npu-orchestrator/
├── app/
│   ├── api/v1/              # REST API route handlers (Webhooks, Sync, Jobs)
│   ├── core/                # Configuration, pre-flight engine, HMAC security
│   ├── drivers/
│   │   ├── proxmox.py       # Proxmoxer REST client (KVM, Cloud-Init, Telemetry)
│   │   ├── netbox.py        # NetBox REST client with HTTP keep-alive pooling
│   │   ├── template_sync.py # Proxmox ➔ NetBox Platform Sync & Lifecycle Engine
│   │   ├── traefik_sync.py  # Traefik Docker/API route sync
│   │   └── notifier.py      # Signal messenger notification provider
│   ├── scripts/
│   │   └── bootstrap_netbox.py # Schema sanity audit & custom fields synchronizer
│   ├── storage/
│   │   └── db.py            # SQLite WAL database & job execution logger
│   ├── workers/             # Domain worker engine
│   │   ├── dispatcher.py    # NetBox webhook parsing, event diffing & locks
│   │   ├── provisioning.py  # Linux, Windows & LXC creation pipelines
│   │   ├── lifecycle.py     # Power, live hardware resizing, rename, decommission
│   │   ├── queue.py         # Python 3.12 task shield & job dispatcher
│   │   └── tasks.py         # Backward-compatible re-export façade
│   └── main.py              # Application lifecycle & background reconciler loop
├── config.yml               # Local topology & hardware fallbacks (git-ignored)
├── config.example.yml       # Pristine commented configuration template
├── .env                     # Secrets & tokens (git-ignored)
├── .env.example             # Secrets template
├── docker-compose.yml       # Production container deployment definition
├── Dockerfile               # Multi-stage secure container build
├── install.sh               # 5-step unified installer & health probe
└── check-config.sh          # 16-point live diagnostics CLI
```

---

## 🔒 Security Posture

- **HMAC Webhook Verification**: Incoming webhooks from NetBox are checked against `WEBHOOK_SECRET` using SHA-512 signatures.
- **Non-Root Runtime**: Docker container executes under user `appuser` (UID/GID 10001) with zero elevated host privileges.
- **Git Safety**: Secrets (`.env`) and topology overrides (`config.yml`) are strictly excluded in `.gitignore` to prevent credential leakage.

---

## 📄 License

Distributed under the **MIT License**. Free for commercial, homelab, and enterprise use.
See [LICENSE](LICENSE) for details.
