# Changelog

All notable changes to the **NPU Infrastructure Orchestrator** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Configurable Traefik Middleware Matching & Custom Fields Mapping**:
  - Added `middleware_patterns` in `config.yml` allowing user-defined middleware substrings for detecting IP whitelist and SSO protection without hardcoded middleware names.
  - Added `custom_fields` mapping in `config.yml` allowing flexible mapping between orchestrator service attributes and NetBox custom field names.
  - Added `service_tag` configuration option to assign a single clean tag (default `"traefik"`) to all discovered routes, with automatic tag verification and creation in NetBox.

### Changed
- **Streamlined Service Tagging**:
  - Replaced redundant service tags (`SSO`, `NPU Whitelist`, `Public Ingress`) with a single Traefik service tag; protection status is cleanly represented via boolean custom field checkmarks.
  - NetBox sync reconciliation automatically migrates existing services to the unified service tag.

---

## [0.0.3] - 2026-09-04

### Added
- **Bare-Metal & Device Traefik Ingress Support**:
  - `TraefikSyncDriver` now natively accepts either `netbox_device_id` (`dcim.device`) or `netbox_vm_id` (`virtualization.virtualmachine`).
  - Enables reverse proxies running directly on bare-metal hypervisor hosts or physical appliances to register their ingress routes into NetBox Application Services.
- **Normalized Service Naming Standard**:
  - Added `format_service_name` helper in `TraefikSyncDriver` to normalize application service names to all-lowercase without `"Ingress"` suffixes.
  - Automatically strips dynamic file extensions (`.yml`, `.yaml`) and redundant domain suffixes.
  - Enabled dynamic service name updating in NetBox during sync reconciliation.
- **Dynamic NetBox IPAM Next-Available-IP Allocation**:
  - Integrated atomic `/api/ipam/prefixes/{id}/available-ips/` allocation via `netbox_driver.get_or_allocate_available_ip()`, dynamically fetching free addresses directly from NetBox prefix pools.
  - Added strict Proxmox driver boundary validation to prevent VMID allocations from generating invalid IPv4 octets.
- **Pre-Existing Workload Auto-Adoption**:
  - Implemented `proxmox_driver.find_vm_by_name()` across QEMU VMs and LXC Containers.
  - Automatically detects if an existing hypervisor workload matches a newly registered NetBox hostname, adopting the existing VMID without duplicate cloning.
- **First-Class Proxmox LXC System Container Deployment**:
  - Added native LXC container provisioning pipeline (`pct create`), secure root password generation, unprivileged user namespaces, nesting features (`nesting=1`), and 1-click NetBox blueprint sizing.
- **Strict Multi-Site & Multi-Cluster Isolation**:
  - Added immediate (< 2ms) site and cluster guard in webhook dispatcher to prevent cross-site bleeding from foreign clusters, clouds, or remote branch networks.
- **Early Webhook Concurrency Lock**:
  - Provisioning lock is now acquired prior to updating NetBox VM defaults, eliminating recursive webhook race conditions that triggered duplicate cloning jobs.
- **Dynamic FastAPI Version Binding**:
  - Bound FastAPI application title and root endpoint dynamically to `app.__version__`.

### Fixed
- Fixed tenant fallback configuration default in `bootstrap_netbox.py` to use `tenant_id: 1` (`Core Infrastructure`).

---

## [0.0.2] - 2026-08-31

### Added
- **Pluggable Module Architecture**:
  - Introduced `app/core/modules.py` to decouple optional components (Traefik, Uptime Kuma, NetBox DNS, Telemetry, Templates, Signal).
  - Disabled or unconfigured modules are completely skipped during startup, background reconciliation, and CLI diagnostic audits.
- **Uptime Kuma Monitoring Integration**:
  - Added bidirectional device inventory reconciliation with auto-provisioning of ICMP Ping monitors grouped by NetBox Site.
  - Added "View in Uptime Kuma" custom links on NetBox Devices and Virtual Machines.
- **High-Performance In-Memory `/health` Endpoint**:
  - Non-blocking health probe responding in ~2ms with full diagnostic state for core pillars and optional modules.
- **Database Hardening & Historical Pruning**:
  - Configured SQLite WAL mode (`journal_mode=WAL`), connection reuse, `PRAGMA busy_timeout=10000`, and `asyncio.Lock` concurrency.
  - Added automatic orphaned in-flight job recovery on boot.
  - Automated 30-day job retention pruning in background reconciler and on-demand via `POST /api/v1/jobs/prune`.
- **Three-Tier Verification Suite**:
  - Decoupled `./check-config.sh` into Pre-Flight Diagnostics, Proxmox Hypervisor & Cluster Audit, and NetBox Schema Audit.

---

## [0.0.1] - 2026-08-25

### Added
- Initial open-source release of NPU Infrastructure Orchestrator.
- Event-driven NetBox webhook receiver for automated VM provisioning.
- Proxmox VE QEMU VM cloning from templates with Cloud-Init customization.
- Automatic forward (A) and reverse (PTR) DNS registration via NetBox DNS plugin.
- Proxmox 24-hour RRD telemetry metrics synchronization.
- Proxmox template auto-discovery and NetBox Platform reconciliation.
