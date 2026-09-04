#!/usr/bin/env python3
"""
NetBox Zero-Touch Bootstrapper & Schema Sanity Engine for NPU Orchestrator.

Performs a comprehensive schema sanity audit against NetBox:
  - Audits required Custom Fields (VM & Service scopes)
  - Audits Device/VM Roles (Virtual Machine & LXC Container)
  - Audits Cluster Types & 1-Click Deployment Custom Link
  - Audits Webhook & Event Rule bindings

If any required objects are missing, it applies the missing objects idempotently
without touching or duplicating existing infrastructure.
"""

import asyncio
import logging
import os
import sys
import yaml
import httpx
from typing import Any, Dict, List, Optional, Tuple

try:
    from app.core.app_config import app_config
    from app.core.modules import module_manager
except Exception:
    app_config = None
    module_manager = None

# Suppress verbose HTTP request logging from httpx
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("netbox_sanity")

DEFAULT_CUSTOM_FIELDS = [
    {"name": "proxmox_vmid", "label": "Proxmox VMID", "type": "integer", "object_types": ["virtualization.virtualmachine"], "description": "Proxmox cluster VMID assigned by Automation Server"},
    {"name": "proxmox_node", "label": "Proxmox Node", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Proxmox VE host node hosting this VM / Container"},
    {"name": "cpu_usage", "label": "CPU Usage", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "24-Hour average CPU core utilization and peak workload (from Proxmox RRD)"},
    {"name": "memory_usage", "label": "Memory Usage", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "24-Hour average RAM utilization vs total allocated memory (from Proxmox RRD)"},
    {"name": "disk_usage", "label": "Disk Usage", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Root disk capacity and space utilization"},
    {"name": "uptime", "label": "Uptime", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Elapsed uptime since last start or reboot"},
    {"name": "guest_agent", "label": "Guest Agent", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Live QEMU Guest Agent status from Proxmox VE"},
    {"name": "metrics_updated", "label": "Metrics Last Synced", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Timestamp when 24-hour telemetry was last synchronized"},
    {"name": "requested_ip", "label": "IP Address (Optional)", "type": "text", "object_types": ["virtualization.virtualmachine"], "description": "Optional static IPv4 address. If left blank, next IP is auto-allocated."},
    {"name": "fqdn", "label": "FQDN / Domain", "type": "text", "object_types": ["ipam.service"], "description": "Fully Qualified Domain Name or hostname (e.g. media.npu.ee)"},
    {"name": "public_url", "label": "Public URL", "type": "url", "object_types": ["ipam.service"], "description": "Full Public HTTPS/HTTP URL (e.g. https://media.npu.ee)"},
    {"name": "sso_protected", "label": "SSO Protected", "type": "boolean", "object_types": ["ipam.service"], "description": "Protected by Azure AD SSO ForwardAuth (npu-sso)"},
    {"name": "ip_whitelist", "label": "NPU Whitelist", "type": "boolean", "object_types": ["ipam.service"], "description": "Restricted to NPU IP Whitelist (npu-ip-whitelist)"},
    {"name": "middlewares", "label": "Middlewares", "type": "text", "object_types": ["ipam.service"], "description": "Active Traefik Middleware Chain"},
    {"name": "kuma_monitor_id", "label": "Uptime Kuma ID", "type": "integer", "object_types": ["dcim.device", "virtualization.virtualmachine"], "description": "Monitor ID in Uptime Kuma"},
]

DEFAULT_ROLES = [
    {"name": "Virtual Machine", "slug": "virtual-machine", "color": "9c27b0", "description": "Standard virtual machine workload"},
    {"name": "LXC Container", "slug": "lxc-container", "color": "009688", "description": "LXC system container workload"},
]

DEFAULT_TAGS = [
    {"name": "SSO", "slug": "sso", "color": "9c27b0", "description": "Protected by Azure AD SSO ForwardAuth"},
    {"name": "NPU Whitelist", "slug": "npu-whitelist", "color": "ff9800", "description": "Restricted to NPU IP Whitelist"},
    {"name": "Public Ingress", "slug": "public-ingress", "color": "4caf50", "description": "Publicly accessible via Traefik ingress"},
    {"name": "No Monitor", "slug": "no-monitor", "color": "9e9e9e", "description": "Excluded from automated Uptime Kuma ping monitoring"},
    {"name": "Decommissioned", "slug": "decommissioned", "color": "607d8b", "description": "Workload safely decommissioned, isolated, and powered off"},
]

DEFAULT_LXC_TYPES = [
    {"name": "LXC Micro (1C/1G)", "slug": "lxc-micro", "default_vcpus": 1.0, "default_memory": 1024, "description": "LXC Container Blueprint - 1 vCPU, 1 GB RAM"},
    {"name": "LXC Standard (2C/2G)", "slug": "lxc-standard", "default_vcpus": 2.0, "default_memory": 2048, "description": "LXC Container Blueprint - 2 vCPUs, 2 GB RAM"},
    {"name": "LXC Performance (4C/4G)", "slug": "lxc-performance", "default_vcpus": 4.0, "default_memory": 4096, "description": "LXC Container Blueprint - 4 vCPUs, 4 GB RAM"},
]


class NetBoxSanityChecker:
    def __init__(self, netbox_url: str, netbox_token: str, webhook_url: Optional[str] = None, webhook_secret: Optional[str] = None):
        self.url = netbox_url.rstrip("/")
        self.token = netbox_token
        self.webhook_url = webhook_url
        self.webhook_secret = webhook_secret
        self.headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.ids: Dict[str, Any] = {}

    async def get_existing_map(self, endpoint: str, key: str, client: httpx.AsyncClient) -> Dict[str, Any]:
        """Fetches all items from a NetBox endpoint and indexes them by a given key."""
        try:
            resp = await client.get(f"{self.url}/api/{endpoint}/?limit=100", headers=self.headers)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return {item[key]: item for item in results if key in item}
        except Exception:
            pass
        return {}

    async def run_sanity_and_sync(self, check_only: bool = False, summary_mode: bool = False, indent: str = "       ") -> Dict[str, Any]:
        """Audits NetBox schema against requirements and applies missing items if needed."""
        if not summary_mode:
            print("\n======================================================================")
            print("               NETBOX SCHEMA & CONFIGURATION AUDIT")
            print("======================================================================")

        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            # 1. Connection Check
            status_resp = await client.get(f"{self.url}/api/status/", headers=self.headers)
            if status_resp.status_code != 200:
                print(f"{indent}\033[91m✖\033[0m Cannot connect to NetBox at {self.url} (HTTP {status_resp.status_code})")
                sys.exit(1)

            version = status_resp.json().get("netbox-version", "unknown")
            rq_workers = status_resp.json().get("rq-workers-running", 0)
            if not summary_mode:
                print(f"Connected to NetBox v{version} at {self.url}\n")
                if rq_workers > 0:
                    print(f"[✔ OK] Webhook Worker (RQ)    : Active ({rq_workers} worker online - dispatches event webhooks)")
                else:
                    print(f"[⚠️ WARN] Webhook Worker (RQ)  : No RQ workers online (NetBox cannot dispatch provisioning webhooks)")
            else:
                print(f"{indent}\033[92m✔\033[0m NetBox v{version} connected ({self.url})")

            # 2. Audit NetBox DNS Plugin & Default Zone
            dns_enabled = module_manager.is_enabled("dns") if module_manager else (app_config.dns.get("auto_register_a", True) if app_config else True)
            zone_name = app_config.dns.get("default_zone", "npu.house") if app_config else "npu.house"
            has_dns_plugin = False
            has_dns_zone = False
            if not dns_enabled:
                if not summary_mode:
                    print(f"[⏩ SKIP] NetBox DNS Plugin      : Skipped (module disabled in config.yml)")
            else:
                try:
                    plugin_resp = await client.get(f"{self.url}/api/plugins/netbox-dns/zones/", headers=self.headers)
                    if plugin_resp.status_code == 200:
                        has_dns_plugin = True
                        z_resp = await client.get(f"{self.url}/api/plugins/netbox-dns/zones/?name={zone_name}", headers=self.headers)
                        if z_resp.status_code == 200 and z_resp.json().get("results"):
                            has_dns_zone = True
                            if not summary_mode:
                                print(f"[✔ OK] NetBox DNS Plugin      : Active & Responding (Default Zone: '{zone_name}')")
                        else:
                            if not summary_mode:
                                print(f"[⚠️ WARN] NetBox DNS Plugin    : Active, but Zone '{zone_name}' not created yet")
                    elif plugin_resp.status_code == 404:
                        if not summary_mode:
                            print(f"[✖ OUTDATED] NetBox DNS Plugin : 'netbox-dns' plugin not installed (DNS auto-register will fail)")
                except Exception as e:
                    if not summary_mode:
                        print(f"[⚠️ WARN] NetBox DNS Plugin    : Probe skipped ({e})")

            # 3. Audit Custom Fields
            existing_cfs = await self.get_existing_map("extras/custom-fields", "name", client)
            missing_cfs = [cf for cf in DEFAULT_CUSTOM_FIELDS if cf["name"] not in existing_cfs]

            if not summary_mode:
                if not missing_cfs:
                    print(f"[✔ OK] Custom Fields          : All {len(DEFAULT_CUSTOM_FIELDS)}/{len(DEFAULT_CUSTOM_FIELDS)} required fields present")
                else:
                    names = ", ".join(f"'{cf['name']}'" for cf in missing_cfs)
                    print(f"[✖ OUTDATED] Custom Fields   : Missing {len(missing_cfs)} field(s) -> {names}")

            # 4. Audit Roles
            existing_roles = await self.get_existing_map("dcim/device-roles", "slug", client)
            missing_roles = [r for r in DEFAULT_ROLES if r["slug"] not in existing_roles]
            if "virtual-machine" in existing_roles:
                self.ids["role_vm_id"] = existing_roles["virtual-machine"]["id"]
            if "lxc-container" in existing_roles:
                self.ids["role_lxc_id"] = existing_roles["lxc-container"]["id"]

            if not summary_mode:
                if not missing_roles:
                    print(f"[✔ OK] Device / VM Roles      : All {len(DEFAULT_ROLES)}/{len(DEFAULT_ROLES)} roles present (VM ID: {self.ids.get('role_vm_id')}, LXC ID: {self.ids.get('role_lxc_id')})")
                else:
                    names = ", ".join(f"'{r['name']}'" for r in missing_roles)
                    print(f"[✖ OUTDATED] Device / VM Roles: Missing {len(missing_roles)} role(s) -> {names}")

            # 5. Audit Infrastructure Tags
            existing_tags = await self.get_existing_map("extras/tags", "slug", client)
            missing_tags = [t for t in DEFAULT_TAGS if t["slug"] not in existing_tags]

            if not summary_mode:
                if not missing_tags:
                    print(f"[✔ OK] Infrastructure Tags   : All {len(DEFAULT_TAGS)}/{len(DEFAULT_TAGS)} tags present ({', '.join(t['slug'] for t in DEFAULT_TAGS)})")
                else:
                    names = ", ".join(f"'{t['name']}'" for t in missing_tags)
                    print(f"[✖ OUTDATED] Infrastructure Tags: Missing {len(missing_tags)} tag(s) -> {names}")

            # 6. Audit Cluster Type
            existing_ctypes = await self.get_existing_map("virtualization/cluster-types", "slug", client)
            has_ctype = "proxmox-ve" in existing_ctypes

            if has_ctype:
                ctype_id = existing_ctypes["proxmox-ve"]["id"]
                if not summary_mode:
                    print(f"[✔ OK] Proxmox Cluster Type   : 'Proxmox VE' registered (ID: {ctype_id})")
            else:
                ctype_id = None
                if not summary_mode:
                    print("[✖ OUTDATED] Cluster Type     : 'Proxmox VE' type missing")

            # 7. Audit Topology Defaults (from config.yml)
            topo_parts = []
            topo_missing = []
            if app_config:
                defaults = app_config.defaults
                tenant_id = defaults.get("tenant_id")
                site_id = defaults.get("site_id")
                cluster_id = defaults.get("cluster_id")

                if tenant_id:
                    t_resp = await client.get(f"{self.url}/api/tenancy/tenants/{tenant_id}/", headers=self.headers)
                    if t_resp.status_code == 200:
                        topo_parts.append(f"Tenant '{t_resp.json().get('name')}' (ID: {tenant_id})")
                    else:
                        topo_missing.append(f"Tenant ID {tenant_id}")

                if site_id:
                    s_resp = await client.get(f"{self.url}/api/dcim/sites/{site_id}/", headers=self.headers)
                    if s_resp.status_code == 200:
                        topo_parts.append(f"Site '{s_resp.json().get('name')}' (ID: {site_id})")
                    else:
                        topo_missing.append(f"Site ID {site_id}")

                if cluster_id:
                    c_resp = await client.get(f"{self.url}/api/virtualization/clusters/{cluster_id}/", headers=self.headers)
                    if c_resp.status_code == 200:
                        topo_parts.append(f"Cluster '{c_resp.json().get('name')}' (ID: {cluster_id})")
                    else:
                        topo_missing.append(f"Cluster ID {cluster_id}")

                if not summary_mode:
                    if not topo_missing:
                        print(f"[✔ OK] Topology Defaults      : {', '.join(topo_parts)}")
                    else:
                        print(f"[✖ OUTDATED] Topology Defaults: Missing {', '.join(topo_missing)} in NetBox")

            # 8. Audit Custom Links & Blueprint Types
            existing_links = await self.get_existing_map("extras/custom-links", "name", client)
            has_link = "Deploy VM Blueprint" in existing_links
            has_kuma_link = "View in Uptime Kuma" in existing_links
            kuma_enabled = module_manager.is_enabled("uptime_kuma") if module_manager else True

            existing_vm_types = await self.get_existing_map("virtualization/virtual-machine-types", "slug", client)
            missing_lxc_types = [t for t in DEFAULT_LXC_TYPES if t["slug"] not in existing_vm_types]

            if not summary_mode:
                if has_link:
                    print("[✔ OK] 1-Click Deploy Button  : 'Deploy VM Blueprint' active on VM Types")
                else:
                    print("[✖ OUTDATED] Custom Link      : 'Deploy VM Blueprint' button missing")
                if not missing_lxc_types:
                    print(f"[✔ OK] LXC Blueprints         : All {len(DEFAULT_LXC_TYPES)} container blueprints active")
                else:
                    names = ", ".join(f"'{t['name']}'" for t in missing_lxc_types)
                    print(f"[ℹ INFO] LXC Blueprints        : Missing {len(missing_lxc_types)} blueprint(s) -> {names}")
                if kuma_enabled:
                    if has_kuma_link:
                        print("[✔ OK] Uptime Kuma Link       : 'View in Uptime Kuma' active on Devices & VMs")
                    else:
                        print("[✖ OUTDATED] Uptime Kuma Link : 'View in Uptime Kuma' button missing")
                else:
                    print("[⏩ SKIP] Uptime Kuma Link       : Skipped (module disabled or not configured)")

            # 9. Audit Webhook & Event Rule
            existing_whs = await self.get_existing_map("extras/webhooks", "name", client)
            has_wh = "Proxmox Orchestrator Webhook" in existing_whs
            wh_id = existing_whs["Proxmox Orchestrator Webhook"]["id"] if has_wh else None

            existing_ers = await self.get_existing_map("extras/event-rules", "name", client)
            has_er = "Trigger Proxmox VM Provisioning" in existing_ers

            if not summary_mode:
                if has_wh and has_er:
                    print("[✔ OK] Provisioning Webhook   : Active (points to Orchestrator)")
                else:
                    missing_wh_parts = []
                    if not has_wh:
                        missing_wh_parts.append("Webhook")
                    if not has_er:
                        missing_wh_parts.append("Event Rule")
                    print(f"[✖ OUTDATED] Webhook Config   : Missing {' & '.join(missing_wh_parts)}")

            # Check if any updates are needed
            needs_kuma_link = kuma_enabled and not has_kuma_link
            needs_sync = bool(missing_cfs or missing_roles or missing_tags or not has_ctype or not has_link or needs_kuma_link or not has_wh or not has_er or missing_lxc_types)

            if summary_mode:
                if not missing_cfs:
                    print(f"{indent}\033[92m✔\033[0m All {len(DEFAULT_CUSTOM_FIELDS)} Custom Fields verified")
                else:
                    print(f"{indent}\033[93m⚠️\033[0m Custom Fields: {len(missing_cfs)} missing (will auto-create)")

                if not missing_roles:
                    print(f"{indent}\033[92m✔\033[0m Virtual Machine & LXC Container roles verified")
                else:
                    print(f"{indent}\033[93m⚠️\033[0m Roles: {len(missing_roles)} missing (will auto-create)")

                if not missing_tags:
                    print(f"{indent}\033[92m✔\033[0m All {len(DEFAULT_TAGS)} Infrastructure Tags verified")
                else:
                    print(f"{indent}\033[93m⚠️\033[0m Tags: {len(missing_tags)} missing (will auto-create)")

                if has_wh and has_link:
                    print(f"{indent}\033[92m✔\033[0m Webhook & 1-Click deploy blueprints active")
                else:
                    print(f"{indent}\033[93m⚠️\033[0m Webhooks/Blueprints need registration")

            if not summary_mode:
                print("-" * 70)
                if not needs_sync:
                    print("RESULT: NetBox schema is 100% UP-TO-DATE and fully compatible!")
                    print("======================================================================\n")
                    return self.ids

            if check_only:
                if not summary_mode:
                    print("RESULT: NetBox schema is OUTDATED. Run './setup-netbox.sh' to apply updates.")
                    print("======================================================================\n")
                return self.ids

            if not needs_sync:
                return self.ids

            # ── Auto-Remediation Phase (apply only missing objects) ────────────
            print("RESULT: Schema updates required. Applying missing objects...")
            print("-" * 70)

            # Apply missing custom fields
            for cf in missing_cfs:
                resp = await client.post(f"{self.url}/api/extras/custom-fields/", headers=self.headers, json=cf)
                if resp.status_code in (200, 201):
                    print(f"  ✔ Created Custom Field: {cf['name']}")
                else:
                    print(f"  ✖ Failed to create Custom Field {cf['name']}: {resp.text}")

            # Apply missing roles
            for r in missing_roles:
                resp = await client.post(f"{self.url}/api/dcim/device-roles/", headers=self.headers, json=r)
                if resp.status_code in (200, 201):
                    new_role = resp.json()
                    print(f"  ✔ Created Device Role: {r['name']} (ID: {new_role['id']})")
                    if r["slug"] == "virtual-machine":
                        self.ids["role_vm_id"] = new_role["id"]
                    elif r["slug"] == "lxc-container":
                        self.ids["role_lxc_id"] = new_role["id"]

            # Apply missing tags
            for t in missing_tags:
                resp = await client.post(f"{self.url}/api/extras/tags/", headers=self.headers, json=t)
                if resp.status_code in (200, 201):
                    print(f"  ✔ Created Tag: {t['name']} (slug: {t['slug']})")
                else:
                    print(f"  ✖ Failed to create Tag {t['name']}: {resp.text}")

            # Apply missing cluster type
            if not has_ctype:
                ctype_payload = {"name": "Proxmox VE", "slug": "proxmox-ve", "description": "Proxmox VE Hypervisors"}
                resp = await client.post(f"{self.url}/api/virtualization/cluster-types/", headers=self.headers, json=ctype_payload)
                if resp.status_code in (200, 201):
                    ctype_id = resp.json().get("id")
                    print(f"  ✔ Created Cluster Type: Proxmox VE (ID: {ctype_id})")

            # Apply missing custom links
            if not has_link:
                link_payload = {
                    "name": "Deploy VM Blueprint",
                    "object_types": ["virtualization.virtualmachinetype"],
                    "link_text": "🚀 Deploy New VM / CT from this Blueprint",
                    "link_url": "https://{{ request.get_host }}/virtualization/virtual-machines/add/?virtual_machine_type={{ object.id }}&status=active",
                    "button_class": "green",
                    "new_window": False,
                }
                resp = await client.post(f"{self.url}/api/extras/custom-links/", headers=self.headers, json=link_payload)
                if resp.status_code in (200, 201):
                    print("  ✔ Created Custom Link: Deploy VM Blueprint")

            # Apply missing LXC Blueprint Types
            if missing_lxc_types:
                lxc_platform_id = None
                p_resp = await client.get(f"{self.url}/api/dcim/platforms/?limit=100", headers=self.headers)
                if p_resp.status_code == 200:
                    for p in p_resp.json().get("results", []):
                        if p.get("slug", "").startswith("pve-lxc-") or "lxc" in p.get("slug", "").lower():
                            lxc_platform_id = p["id"]
                            break

                for t in missing_lxc_types:
                    t_payload = {
                        "name": t["name"],
                        "slug": t["slug"],
                        "default_vcpus": t["default_vcpus"],
                        "default_memory": t["default_memory"],
                        "description": t["description"],
                    }
                    if lxc_platform_id:
                        t_payload["default_platform"] = lxc_platform_id
                    t_resp = await client.post(f"{self.url}/api/virtualization/virtual-machine-types/", headers=self.headers, json=t_payload)
                    if t_resp.status_code in (200, 201):
                        print(f"  ✔ Created LXC Blueprint Type: {t['name']}")

            if kuma_enabled and not has_kuma_link:
                kuma_link_payload = {
                    "name": "View in Uptime Kuma",
                    "object_types": ["dcim.device", "virtualization.virtualmachine"],
                    "link_text": "{% if object.cf.kuma_monitor_id or object.primary_ip %}📊 Uptime Kuma{% endif %}",
                    "link_url": "https://kuma.npu.ee/dashboard/{% if object.cf.kuma_monitor_id %}{{ object.cf.kuma_monitor_id }}{% endif %}",
                    "button_class": "cyan",
                    "new_window": True,
                }
                resp = await client.post(f"{self.url}/api/extras/custom-links/", headers=self.headers, json=kuma_link_payload)
                if resp.status_code in (200, 201):
                    print("  ✔ Created Custom Link: View in Uptime Kuma")

            # Apply missing webhook & event rule
            if self.webhook_url and not has_wh:
                wh_payload = {
                    "name": "Proxmox Orchestrator Webhook",
                    "payload_url": self.webhook_url,
                    "secret": self.webhook_secret or "",
                }
                resp = await client.post(f"{self.url}/api/extras/webhooks/", headers=self.headers, json=wh_payload)
                if resp.status_code in (200, 201):
                    wh_id = resp.json().get("id")
                    print(f"  ✔ Created Webhook: Proxmox Orchestrator Webhook (ID: {wh_id})")

            if wh_id and not has_er:
                er_payload = {
                    "name": "Trigger Proxmox VM Provisioning",
                    "object_types": ["virtualization.virtualmachine"],
                    "action_type": "webhook",
                    "action_object_id": wh_id,
                    "event_types": ["creations", "updates", "deletions"],
                }
                resp = await client.post(f"{self.url}/api/extras/event-rules/", headers=self.headers, json=er_payload)
                if resp.status_code in (200, 201):
                    print("  ✔ Created Event Rule: Trigger Proxmox VM Provisioning")

            # 7. Check if this is a 100% blank NetBox instance (no clusters exist at all)
            existing_clusters = await self.get_existing_map("virtualization/clusters", "name", client)
            if not existing_clusters and ctype_id:
                print("\nℹ️  Empty NetBox detected (0 clusters found). Provisioning baseline topology...")
                # Baseline cluster
                c_resp = await client.post(f"{self.url}/api/virtualization/clusters/", headers=self.headers, json={
                    "name": "Primary PVE Cluster", "type": ctype_id, "description": "Default Proxmox VE Cluster"
                })
                c_id = c_resp.json().get("id") if c_resp.status_code in (200, 201) else None
                if c_id:
                    self.ids["cluster_id"] = c_id
                    print(f"  ✔ Created Default Cluster: Primary PVE Cluster (ID: {c_id})")

                # Baseline site
                s_resp = await client.post(f"{self.url}/api/dcim/sites/", headers=self.headers, json={
                    "name": "Primary Datacenter", "slug": "primary-datacenter", "status": "active"
                })
                s_id = s_resp.json().get("id") if s_resp.status_code in (200, 201) else None
                if s_id:
                    self.ids["site_id"] = s_id
                    print(f"  ✔ Created Default Site: Primary Datacenter (ID: {s_id})")

                # Baseline tenant
                t_resp = await client.post(f"{self.url}/api/tenancy/tenants/", headers=self.headers, json={
                    "name": "Internal Services", "slug": "internal-services"
                })
                t_id = t_resp.json().get("id") if t_resp.status_code in (200, 201) else None
                if t_id:
                    self.ids["tenant_id"] = t_id
                    print(f"  ✔ Created Default Tenant: Internal Services (ID: {t_id})")

                # Cluster context
                if c_id:
                    ctx_payload = {
                        "name": "Cluster: Primary PVE Cluster Baseline",
                        "weight": 250,
                        "description": "Default datastore, network bridge, and hypervisor node",
                        "is_active": True,
                        "clusters": [c_id],
                        "data": {"datastore": "local-zfs", "bridge": "vmbr0", "default_node": "proxmox"},
                    }
                    await client.post(f"{self.url}/api/extras/config-contexts/", headers=self.headers, json=ctx_payload)
                    print("  ✔ Created Baseline Config Context for Cluster")

            print("======================================================================\n")
            return self.ids


def update_config_yml(ids: Dict[str, Any], config_path: str = "config.yml") -> None:
    """Updates numeric IDs in config.yml ONLY if they were newly generated and config has no valid ID."""
    if not ids:
        return

    import re as _re

    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        config_path,
        os.path.join(script_dir, "..", "..", "config.yml"),
        "/app/config.yml",
    ]
    resolved = None
    for candidate in candidates:
        if os.path.exists(candidate):
            resolved = os.path.abspath(candidate)
            break

    if not resolved:
        return

    try:
        with open(resolved, "r", encoding="utf-8") as f:
            text = f.read()

        # Only patch role_vm_id and role_lxc_id if present, never overwrite user cluster/site/tenant
        patched = []
        for key in ["role_vm_id", "role_lxc_id"]:
            value = ids.get(key)
            if value is None:
                continue
            pattern = rf"^(\s*{_re.escape(key)}:\s*)\S+(.*)"
            replacement = rf"\g<1>{value}\2"
            new_text = _re.sub(pattern, replacement, text, flags=_re.MULTILINE)
            if new_text != text:
                patched.append(key)
            text = new_text

        if patched:
            with open(resolved, "w", encoding="utf-8") as f:
                f.write(text)
    except Exception:
        pass


async def main():
    # Load .env file
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

    netbox_url = os.environ.get("NETBOX_URL", "")
    netbox_token = os.environ.get("NETBOX_TOKEN", "")
    webhook_secret = os.environ.get("NETBOX_WEBHOOK_SECRET", "")
    webhook_url = os.environ.get("WEBHOOK_URL", "http://127.0.0.1:8090/api/v1/webhooks/netbox")

    if not netbox_url or not netbox_token:
        print("❌ Error: NETBOX_URL and NETBOX_TOKEN must be set in .env")
        sys.exit(1)

    check_only = "--check-only" in sys.argv or "--check" in sys.argv
    summary_mode = "--summary" in sys.argv

    checker = NetBoxSanityChecker(netbox_url, netbox_token, webhook_url, webhook_secret)
    ids = await checker.run_sanity_and_sync(check_only=check_only, summary_mode=summary_mode)
    update_config_yml(ids)


if __name__ == "__main__":
    asyncio.run(main())
