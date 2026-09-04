#!/usr/bin/env python3
"""
Proxmox VE Hypervisor & Resource Audit for NPU Orchestrator.

Audits the Proxmox VE infrastructure environment:
  - Hypervisor node status, CPU/RAM sizing, and uptime
  - Default storage pool presence, content types, and free disk capacity
  - Secondary/backup storage pool health
  - Default network bridge presence, active state, and CIDR/gateway
  - Discovered OS blueprint VM templates
  - Next available VMID allocator
"""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

try:
    from app.core.config import settings
except Exception:
    class _FallbackSettings:
        def __init__(self):
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
            if os.path.exists(env_path):
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, _, v = line.partition("=")
                            os.environ.setdefault(k.strip(), v.strip())
            self.PROXMOX_HOST = os.environ.get("PROXMOX_HOST", "")
            self.PROXMOX_PORT = int(os.environ.get("PROXMOX_PORT", "8006"))
            self.PROXMOX_USER = os.environ.get("PROXMOX_USER", "root@pam")
            self.PROXMOX_TOKEN_NAME = os.environ.get("PROXMOX_TOKEN_NAME", "")
            self.PROXMOX_TOKEN_VALUE = os.environ.get("PROXMOX_TOKEN_VALUE", "")
            self.PROXMOX_PASSWORD = os.environ.get("PROXMOX_PASSWORD", "")
            self.PROXMOX_VERIFY_SSL = os.environ.get("PROXMOX_VERIFY_SSL", "false").lower() == "true"
            self.PROXMOX_DEFAULT_NODE = os.environ.get("PROXMOX_DEFAULT_NODE", "")
    settings = _FallbackSettings()

try:
    from app.core.app_config import app_config
except Exception:
    class _FallbackAppConfig:
        defaults = {"storage": "zfs-storage", "bridge": "vmbr0"}
        templates = {"enabled": True, "linux_vmid_prefix": "90", "windows_vmid_prefix": "92"}
    app_config = _FallbackAppConfig()

# Suppress driver & library debug messages from polluting audit output
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("orchestrator.proxmox").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger("proxmox_audit")

from app.drivers.proxmox import proxmox_driver


class ProxmoxAuditor:
    def __init__(self):
        self.host = settings.PROXMOX_HOST
        self.port = settings.PROXMOX_PORT
        self.user = settings.PROXMOX_USER

    async def run_audit(self, summary_mode: bool = False, indent: str = "       ") -> Dict[str, Any]:
        """Runs complete Proxmox VE hypervisor and resource audit."""
        if not summary_mode:
            print("\n======================================================================")
            print("                PROXMOX VE HYPERVISOR & CLUSTER AUDIT")
            print("======================================================================")

        loop = asyncio.get_running_loop()

        def _perform_probes():
            pve = proxmox_driver.get_client()
            version_data = pve.version.get()
            node = proxmox_driver.resolve_node(None)
            node_status = pve.nodes(node).status.get()
            storages = pve.nodes(node).storage.get()
            networks = pve.nodes(node).network.get()
            templates = proxmox_driver.list_templates(node)
            next_vmid = proxmox_driver.get_next_vmid()
            return {
                "version": version_data,
                "node": node,
                "status": node_status,
                "storages": storages,
                "networks": networks,
                "templates": templates,
                "next_vmid": next_vmid,
            }

        try:
            data = await loop.run_in_executor(None, _perform_probes)
        except Exception as e:
            if summary_mode:
                print(f"{indent}\033[91m✖\033[0m Proxmox VE Resource Audit failed: {e}")
            else:
                print(f"[\033[91m✖ FAIL\033[0m] Proxmox VE Connectivity         : Failed to probe: {e}")
                print("=" * 70 + "\n")
            return {"overall_status": "fail", "error": str(e), "checks": []}

        ver_str = f"{data['version'].get('version', '')}-{data['version'].get('release', '')}"
        kernel_str = data['status'].get("current-kernel", {}).get("release") or data['status'].get("kversion", "unknown")
        if " " in kernel_str:
            kernel_str = kernel_str.split()[0]

        if not summary_mode:
            print(f"Connected to Proxmox VE {ver_str} (kernel {kernel_str}) on {self.host}:{self.port}\n")

        node_name = data["node"]
        status = data["status"]
        uptime_sec = status.get("uptime", 0)
        uptime_days = uptime_sec // 86400
        uptime_hours = (uptime_sec % 86400) // 3600
        uptime_str = f"{uptime_days}d {uptime_hours}h" if uptime_days > 0 else f"{uptime_hours}h"

        cpus = status.get("cpuinfo", {}).get("cpus", "unknown")
        mem_total_gb = round(status.get("memory", {}).get("total", 0) / (1024 ** 3), 1)

        checks: List[Dict[str, Any]] = []

        # 1. Hypervisor Node
        checks.append({
            "name": f"Hypervisor Node '{node_name}'",
            "status": "pass",
            "detail": f"Online ({cpus} vCPUs, {mem_total_gb} GB RAM, uptime: {uptime_str})",
            "summary": f"Node '{node_name}' Online ({cpus} vCPUs, {mem_total_gb} GB RAM, uptime: {uptime_str})",
        })

        # 2. Target Storage Pools
        target_storage = app_config.defaults.get("storage", "zfs-storage")
        storage_map = {s.get("storage"): s for s in data["storages"]}

        if target_storage in storage_map:
            st = storage_map[target_storage]
            avail_gb = round(st.get("avail", 0) / (1024 ** 3), 1)
            total_gb = round(st.get("total", 0) / (1024 ** 3), 1)
            st_type = st.get("type", "storage")
            st_content = st.get("content", "all")
            st_active = st.get("active", 1) == 1
            st_status = "pass" if st_active else "warn"
            checks.append({
                "name": f"Storage Pool '{target_storage}'",
                "status": st_status,
                "detail": f"Available ({st_type}, {avail_gb} GB free / {total_gb} GB, content: {st_content})",
                "summary": f"Storage '{target_storage}' ({avail_gb} GB free / {total_gb} GB)",
            })
        else:
            checks.append({
                "name": f"Storage Pool '{target_storage}'",
                "status": "fail",
                "detail": f"Configured default storage '{target_storage}' not found on node. Available: {list(storage_map.keys())}",
                "summary": f"Default storage '{target_storage}' NOT found",
            })

        # Check secondary / backup storage if present
        for s_name, st in storage_map.items():
            if s_name != target_storage and st.get("active") == 1 and st.get("enabled", 1) == 1:
                s_avail_gb = round(st.get("avail", 0) / (1024 ** 3), 1)
                s_content = st.get("content", "")
                if "backup" in s_content or "vztmpl" in s_content or "iso" in s_content:
                    checks.append({
                        "name": f"Auxiliary Storage '{s_name}'",
                        "status": "pass",
                        "detail": f"Available ({st.get('type')}, {s_avail_gb} GB free, content: {s_content})",
                        "summary": f"Backup Storage '{s_name}' ({s_avail_gb} GB free)",
                    })
                    break

        # 3. Target Network Bridge
        target_bridge = app_config.defaults.get("bridge", "vmbr0")
        network_map = {n.get("iface"): n for n in data["networks"]}

        if target_bridge in network_map:
            br = network_map[target_bridge]
            br_active = br.get("active", 1) == 1
            cidr = br.get("cidr") or (f"{br.get('address')}/{br.get('netmask')}" if br.get("address") else "No IP")
            gateway = br.get("gateway", "")
            gw_str = f", gateway: {gateway}" if gateway else ""
            ports = br.get("bridge_ports", "")
            port_str = f", ports: {ports}" if ports else ""
            checks.append({
                "name": f"Network Bridge '{target_bridge}'",
                "status": "pass" if br_active else "warn",
                "detail": f"Active & UP ({cidr}{gw_str}{port_str})",
                "summary": f"Bridge '{target_bridge}' Active ({cidr})",
            })
        else:
            checks.append({
                "name": f"Network Bridge '{target_bridge}'",
                "status": "warn",
                "detail": f"Bridge '{target_bridge}' not found in node network list. Available: {list(network_map.keys())}",
                "summary": f"Bridge '{target_bridge}' not found",
            })

        # 4. OS Blueprint Templates
        templates = data["templates"]
        if templates:
            tmpl_desc = ", ".join(f"{t['name']} [{t['vmid']}]" for t in templates[:3])
            if len(templates) > 3:
                tmpl_desc += f", +{len(templates) - 3} more"
            checks.append({
                "name": "OS Blueprint Templates",
                "status": "pass",
                "detail": f"{len(templates)} found ({tmpl_desc})",
                "summary": f"Templates: {len(templates)} Blueprints available ({tmpl_desc})",
            })
        else:
            checks.append({
                "name": "OS Blueprint Templates",
                "status": "warn",
                "detail": "No VM templates detected in 90xx/92xx ranges or marked as template",
                "summary": "No VM templates detected",
            })

        # 5. Cluster VMID Allocator
        next_vmid = data["next_vmid"]
        checks.append({
            "name": "Cluster VMID Allocator",
            "status": "pass",
            "detail": f"Next available VMID is {next_vmid}",
            "summary": f"Cluster VMID Allocator: Next available VMID is {next_vmid}",
        })

        passed = sum(1 for c in checks if c["status"] == "pass")
        failed = sum(1 for c in checks if c["status"] == "fail")
        warns = sum(1 for c in checks if c["status"] == "warn")
        overall = "fail" if failed > 0 else ("warn" if warns > 0 else "pass")

        # Formatting Output
        if summary_mode:
            for c in checks:
                icon = "\033[92m✔\033[0m" if c["status"] == "pass" else ("\033[93m⚠️\033[0m" if c["status"] == "warn" else "\033[91m✖\033[0m")
                print(f"{indent}{icon} {c['summary']}")
        else:
            for c in checks:
                icon = "\033[92m✔ OK\033[0m" if c["status"] == "pass" else ("\033[93m⚠️ WARN\033[0m" if c["status"] == "warn" else "\033[91m✖ FAIL\033[0m")
                print(f"[{icon}] {c['name']:<31} : {c['detail']}")

            print("-" * 70)
            if overall == "pass":
                print(f"RESULT: Proxmox VE hypervisor & resources 100% READY! ({passed}/{len(checks)} verified)")
            elif overall == "warn":
                print(f"RESULT: Proxmox VE operational with warnings ({passed}/{len(checks)} passed, {warns} warnings)")
            else:
                print(f"RESULT: Proxmox VE audit FAILED ({failed} critical issues)")
            print("======================================================================\n")

        return {
            "overall_status": overall,
            "total_checks": len(checks),
            "passed": passed,
            "failed": failed,
            "warnings": warns,
            "checks": checks,
            "node": node_name,
            "version": ver_str,
            "kernel": kernel_str,
        }


proxmox_auditor = ProxmoxAuditor()

if __name__ == "__main__":
    summary_flag = "--summary" in sys.argv
    result = asyncio.run(proxmox_auditor.run_audit(summary_mode=summary_flag))
    if result["overall_status"] == "fail":
        sys.exit(1)
    sys.exit(0)
