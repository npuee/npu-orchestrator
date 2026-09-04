# NPU Infrastructure Orchestrator 🚀

> **Turn NetBox into a zero-touch, event-driven Private Cloud Orchestrator for Proxmox VE.**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Proxmox VE](https://img.shields.io/badge/Proxmox_VE-8.x_|_9.x-E57000.svg?style=flat&logo=proxmox&logoColor=white)](https://proxmox.com)
[![NetBox](https://img.shields.io/badge/NetBox-4.x-004D40.svg?style=flat&logo=netbox&logoColor=white)](https://netboxlabs.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.0.3-green.svg)](app/__init__.py)

---

## ⚡ 3-Minute Quickstart

Get NPU Orchestrator running with **3 commands**:

```bash
# 1. Clone the repository
git clone https://github.com/npu-ee/npu-orchestrator.git
cd npu-orchestrator

# 2. Copy the environment template & fill in your NetBox + Proxmox credentials
cp .env.example .env
nano .env

# 3. Run the automated installer
chmod +x install.sh
./install.sh
```

The installer builds the container, verifies network connectivity, auto-configures NetBox (custom fields, roles, tags, webhooks), and starts the orchestrator on port `8090`.

---

## 📋 Initial Configuration Guide

Only two systems are required: **NetBox** and **Proxmox VE**. All other features (*DNS, Uptime Kuma, Traefik, Telemetry, Signal*) are optional modules that are completely skipped if disabled.

### Step 1: Create a Proxmox VE API Token

1. In Proxmox VE web GUI, go to: **Datacenter ➔ Permissions ➔ API Tokens ➔ Add**.
2. **User**: `root@pam` (or an administrative user).
3. **Token ID**: `orchestrator`
4. **Privilege Separation**: **Uncheck** this box (so the token inherits user permissions).
5. Click **Add** and copy the **Secret Token Value** (you won't be able to see it again).

### Step 2: Create a NetBox API Token

1. In NetBox web GUI, click your profile icon (top right) ➔ **API Tokens ➔ Add Token**.
2. **Description**: `NPU Orchestrator`
3. **Key**: Leave blank to auto-generate.
4. Set **Write enabled** (Checked).
5. Click **Create** and copy the generated token string.

### Step 3: Populate `.env`

Edit `.env` and fill in the required core credentials:

```ini
# NetBox (Source of Truth)
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your_netbox_api_token_here

# Proxmox VE Cluster
PROXMOX_HOST=192.168.1.100
PROXMOX_PORT=8006
PROXMOX_USER=root@pam
PROXMOX_TOKEN_NAME=orchestrator
PROXMOX_TOKEN_VALUE=your-proxmox-token-secret-uuid
PROXMOX_VERIFY_SSL=false

# Security (Random 32+ character strings)
API_KEY=choose_a_strong_api_key_for_orchestrator
NETBOX_WEBHOOK_SECRET=choose_a_random_webhook_secret_hex
```

> [!TIP]
> Generate secure secrets easily using: `openssl rand -hex 24`

### Step 4: Run `./install.sh`

Execute the automated installer:
```bash
./install.sh
```

The installer performs a 6-step setup:
1. **Prerequisites**: Checks Docker and Compose v2 runtime.
2. **Image Compilation**: Compiles the lightweight Python 3.12 Docker image.
3. **Pre-Flight Diagnostics**: Verifies API tokens and reachability to Proxmox and NetBox.
4. **Proxmox Resource Audit**: Queries hypervisor nodes, storage pools, bridges, and OS templates.
5. **NetBox Zero-Touch Auto-Setup**: Creates all 15 required custom fields, VM/LXC roles, infrastructure tags, and binds the provisioning webhook automatically.
6. **Production Launch**: Starts the container and verifies `GET /health` responds HTTP 200 within ~2ms.

---

## 🔍 Verification & Health Checking

At any time, run the standalone verification suite:

```bash
./check-config.sh
```

This runs a 3-part diagnostic audit:
- **Pre-Flight Report**: Tests all credentials, API keys, and service endpoints. Unconfigured optional modules are cleanly marked `[⏩ SKIP]`.
- **Proxmox Hypervisor Audit**: Verifies hypervisor nodes, storage pool capacity, network bridges, and OS templates.
- **NetBox Schema Audit**: Confirms all custom fields, roles, tags, and webhooks are active and up-to-date.

You can also check the live health endpoint directly:
```bash
curl http://127.0.0.1:8090/health
```

---

## 🎮 How to Provision Machines via NetBox

Once installed, deploying machines requires **zero Proxmox GUI access**.

```
┌────────────────────────────────────────────────────────┐
│               NetBox: Add Virtual Machine              │
│                                                        │
│  1. Name:       web-node-01                            │
│  2. Platform:   Ubuntu 24.04 LTS (Noble) (VMID: 9024)  │
│  3. VM Type:    Medium (2 vCPU / 4 GB RAM / 40 GB Disk)│
│  4. Primary IP: 192.168.1.50/24                        │
│                                                        │
│  [ Save ] ➔ Webhook triggers orchestrator              │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│           NPU Orchestrator (Automated ~45s)            │
│                                                        │
│  ✔ Clones Proxmox template (VMID 9024)                 │
│  ✔ Sets CPU cores, RAM, and expands root disk          │
│  ✔ Configures Cloud-Init (IP, Gateway, SSH keys)       │
│  ✔ Registers DNS 'A' and 'PTR' records in NetBox DNS   │
│  ✔ Boots VM & updates status to 'Active' in NetBox     │
│  ✔ Auto-registers ICMP ping monitor in Uptime Kuma     │
└────────────────────────────────────────────────────────┘
```

### 1. Define Sizing Flavors in NetBox (Once)
In NetBox, go to: **Virtualization ➔ VM Types ➔ Add VM Type**:
- **Small**: `1 vCPU`, `2048 MB RAM`, `20 GB Disk`
- **Medium**: `2 vCPU`, `4096 MB RAM`, `40 GB Disk`
- **Large**: `4 vCPU`, `8192 MB RAM`, `80 GB Disk`

### 2. Deploy a VM
In NetBox, go to: **Virtualization ➔ Virtual Machines ➔ Add**:
1. **Name**: `db-node-01`
2. **Platform**: Select any template synced from Proxmox (e.g. `Ubuntu 24.04 LTS [9024]`).
3. **VM Type**: Select your desired flavor (e.g. `Medium`).
4. **IPv4 Address**: Enter a static IP or leave blank for DHCP.
5. Click **Save**.

### 3. Lifecycle Management
- **Power On / Off**: Change NetBox status to `Active` (starts VM) or `Offline` (shuts down VM).
- **Live Hardware Resize**: Adjust vCPU or Memory sliders in NetBox; orchestrator updates Proxmox on next restart.
- **Decommission**: Change status to `Decommissioning` to quarantine/power off, or delete in NetBox to purge Proxmox disks.

---

## 🧩 Optional Pluggable Modules

NPU Orchestrator features a pluggable module system. **If a module is not configured or disabled, the orchestrator skips loading it entirely** with zero CPU or network overhead.

| Module | What It Does | How to Enable |
| :--- | :--- | :--- |
| **NetBox DNS** | Auto-creates forward `A` and reverse `PTR` records when VMs are created. | Set `dns.default_zone: "your.domain"` in `config.yml`. |
| **Uptime Kuma** | Auto-provisions ICMP ping monitors grouped by NetBox Site. | Add `UPTIME_KUMA_URL`, user, and password in `.env`. |
| **Traefik Ingress** | Scans Traefik reverse proxies and documents web services in NetBox. | Configure instance list under `traefik:` in `config.yml`. |
| **Telemetry Sync** | Queries Proxmox 24h RRD counters (CPU, RAM, Disk, Uptime) into NetBox custom fields. | Enabled by default (`telemetry.enabled: true` in `config.yml`). |
| **Template Sync** | Discovers Proxmox VM & LXC templates and registers them as NetBox Platforms. | Enabled by default (`templates.enabled: true` in `config.yml`). |
| **Signal Alerting** | Sends instant push alerts on VM creation, resize, or decommission. | Set `SIGNAL_ENABLED=true` and `SIGNAL_API_URL` in `.env`. |

---

## ⚙️ Configuration Reference

### `config.yml` (Topology & Settings)

```yaml
version: "1.0"

# ── Infrastructure Defaults ─────────────────────────────────────────
# Automatically populated by ./install.sh
defaults:
  tenant_id: 1            # NetBox Tenant ID
  site_id: 2              # NetBox Site ID
  cluster_id: 2           # NetBox Cluster ID
  role_vm_id: 16          # Role ID for KVM Virtual Machines
  role_lxc_id: 15         # Role ID for LXC Containers
  storage: "zfs-storage"  # Proxmox storage pool name
  bridge: "vmbr0"         # Proxmox network bridge name

# ── Fallback Sizing ─────────────────────────────────────────────────
# Emergency fallbacks if a VM is created without a VM Type flavor
fallbacks:
  cores: 2                # Default vCPU cores
  memory_mb: 2048         # Default RAM in MB
  disk_gb: 20             # Default disk size in GB

# ── Proxmox Template Auto-Discovery ─────────────────────────────────
templates:
  enabled: true
  sync_interval_minutes: 60
  linux_vmid_prefix: "90"     # Linux templates live in VMID 9000-9099
  windows_vmid_prefix: "92"   # Windows templates live in VMID 9200-9299
  default_windows_password: "P@ssw0rdInitial!"

# ── Optional: Uptime Kuma Ping Monitoring ───────────────────────────
uptime_kuma:
  enabled: true
  sync_interval_minutes: 30
  exclude_tags:
    - "no-monitor"

# ── Optional: DNS Auto-Registration (NetBox DNS Plugin) ─────────────
dns:
  default_zone: "homelab.local"
  auto_register_a: true
  auto_register_ptr: true
```

---

## 📡 REST API & Monitoring

Interactive OpenAPI documentation is live at `http://<your-server-ip>:8090/docs`.

| Method | Endpoint | Description |
| :---: | :--- | :--- |
| `GET` | `/health` | Instant (~2ms) diagnostic status of Core and all optional modules |
| `POST` | `/api/v1/webhooks/netbox` | Handles real-time NetBox webhook events (HMAC-authenticated) |
| `GET` | `/api/v1/jobs` | Historical provisioning jobs, status, and execution logs |
| `GET` | `/api/v1/jobs/{job_id}` | Real-time execution logs for a specific provisioning task |
| `POST` | `/api/v1/jobs/prune` | Manually prunes old historical jobs older than retention threshold |
| `POST` | `/api/v1/sync/platforms` | Discovers Proxmox templates & reconciles NetBox Platforms |
| `POST` | `/api/v1/sync/metrics` | Pulls live CPU/RAM/Disk metrics from Proxmox into NetBox |
| `POST` | `/api/v1/sync/traefik` | Synchronizes Traefik ingress routes to NetBox services |
| `POST` | `/api/v1/sync/uptime-kuma` | Synchronizes NetBox devices to Uptime Kuma monitors |

---

## ❓ Troubleshooting & FAQ

<details>
<summary><b>1. Proxmox SSL certificate errors ("certificate verify failed")</b></summary>
<br>
If your Proxmox server uses a self-signed SSL certificate, ensure <code>PROXMOX_VERIFY_SSL=false</code> is set in <code>.env</code>.
</details>

<details>
<summary><b>2. How does the orchestrator identify VM templates in Proxmox?</b></summary>
<br>
The orchestrator uses deterministic VMID prefixes configured in <code>config.yml</code>:
<ul>
  <li><b>Linux Templates</b>: VMIDs <code>9000–9099</code> (e.g. <code>9024</code> for Ubuntu 24.04).</li>
  <li><b>Windows Templates</b>: VMIDs <code>9200–9299</code> (e.g. <code>9225</code> for Windows Server 2025).</li>
  <li><b>LXC Containers</b>: Any <code>.tar.zst</code> template in your Proxmox backup/template storage.</li>
</ul>
</details>

<details>
<summary><b>3. Where are application logs stored?</b></summary>
<br>
Container logs can be viewed live via Docker:
<pre><code>docker compose logs -f orchestrator</code></pre>
Provisioning task logs are also stored in SQLite (WAL mode) and accessible via the web API at <code>http://&lt;server-ip&gt;:8090/api/v1/jobs</code>.
</details>

<details>
<summary><b>4. How to restart or apply configuration changes?</b></summary>
<br>
After modifying <code>config.yml</code> or <code>.env</code>, apply changes instantly with:
<pre><code>docker compose restart orchestrator</code></pre>
</details>

---

## 📄 License

Distributed under the **MIT License**. Free for homelab, commercial, and enterprise use.
See [LICENSE](LICENSE) for details.
