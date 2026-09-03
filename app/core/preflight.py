import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional
import httpx

try:
    from app.core.config import settings
except Exception:
    # Standalone fallback: load from environment and .env without pydantic_settings
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
            self.NETBOX_URL = os.environ.get("NETBOX_URL", "")
            self.NETBOX_TOKEN = os.environ.get("NETBOX_TOKEN", "")
            self.PROXMOX_HOST = os.environ.get("PROXMOX_HOST", "")
            self.PROXMOX_PORT = int(os.environ.get("PROXMOX_PORT", "8006"))
            self.PROXMOX_USER = os.environ.get("PROXMOX_USER", "root@pam")
            self.PROXMOX_TOKEN_NAME = os.environ.get("PROXMOX_TOKEN_NAME", "")
            self.PROXMOX_TOKEN_VALUE = os.environ.get("PROXMOX_TOKEN_VALUE", "")
            self.PROXMOX_VERIFY_SSL = os.environ.get("PROXMOX_VERIFY_SSL", "false").lower() == "true"
            self.API_KEY = os.environ.get("API_KEY", "")
            self.NETBOX_WEBHOOK_SECRET = os.environ.get("NETBOX_WEBHOOK_SECRET", "")
    settings = _FallbackSettings()

from app.core.app_config import app_config

logger = logging.getLogger("orchestrator.preflight")


class PreflightChecker:
    """
    Automated pre-flight diagnostic validator for npu-orchestrator.
    Verifies secrets in .env, configuration in config.yml, and live API
    connectivity to both NetBox and Proxmox VE before workloads run.
    """

    def __init__(self):
        self.netbox_url = settings.NETBOX_URL.rstrip("/") if settings.NETBOX_URL else None
        self.netbox_token = settings.NETBOX_TOKEN
        self.proxmox_host = settings.PROXMOX_HOST
        self.proxmox_port = settings.PROXMOX_PORT
        self.proxmox_user = settings.PROXMOX_USER
        self.proxmox_token_name = settings.PROXMOX_TOKEN_NAME
        self.proxmox_token_value = settings.PROXMOX_TOKEN_VALUE
        self.proxmox_verify_ssl = settings.PROXMOX_VERIFY_SSL

    async def check_env_secrets(self) -> List[Dict[str, Any]]:
        """Checks essential secrets in .env."""
        checks = []
        
        # NetBox URL & Token
        if self.netbox_url and self.netbox_token:
            checks.append({"name": "NetBox Credentials", "status": "pass", "detail": f"Configured for {self.netbox_url}"})
        else:
            checks.append({"name": "NetBox Credentials", "status": "fail", "detail": "NETBOX_URL or NETBOX_TOKEN missing in .env"})

        # Proxmox API Credentials
        if self.proxmox_host and self.proxmox_user and self.proxmox_token_value:
            checks.append({
                "name": "Proxmox Credentials",
                "status": "pass",
                "detail": f"Configured for {self.proxmox_user}!{self.proxmox_token_name}@{self.proxmox_host}:{self.proxmox_port}",
            })
        else:
            checks.append({"name": "Proxmox Credentials", "status": "fail", "detail": "PROXMOX_HOST, USER, or TOKEN missing in .env"})

        # Internal API Key
        if settings.API_KEY:
            checks.append({"name": "API Key Protection", "status": "pass", "detail": "API_KEY configured for internal protection"})
        else:
            checks.append({"name": "API Key Protection", "status": "warn", "detail": "API_KEY is not set (endpoints unprotected)"})

        # NetBox Webhook Secret
        if settings.NETBOX_WEBHOOK_SECRET:
            checks.append({"name": "Webhook HMAC Secret", "status": "pass", "detail": "NETBOX_WEBHOOK_SECRET configured"})
        else:
            checks.append({"name": "Webhook HMAC Secret", "status": "warn", "detail": "NETBOX_WEBHOOK_SECRET not set (signatures unverified)"})

        return checks

    async def check_netbox_live(self) -> List[Dict[str, Any]]:
        """Validates NetBox connectivity and existence of configured default IDs."""
        checks = []
        if not self.netbox_url or not self.netbox_token:
            checks.append({"name": "NetBox Live Probe", "status": "fail", "detail": "Skipped: Missing NetBox credentials"})
            return checks

        headers = {
            "Authorization": f"Token {self.netbox_token}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=8.0, verify=False) as client:
            # 1. Test basic connectivity & auth
            try:
                resp = await client.get(f"{self.netbox_url}/api/status/", headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    nb_ver = data.get("netbox-version", "unknown")
                    checks.append({"name": "NetBox API Connectivity", "status": "pass", "detail": f"Connected (NetBox v{nb_ver})"})
                else:
                    checks.append({"name": "NetBox API Connectivity", "status": "fail", "detail": f"HTTP {resp.status_code}: {resp.text[:100]}"})
                    return checks
            except Exception as e:
                checks.append({"name": "NetBox API Connectivity", "status": "fail", "detail": f"Connection failed: {e}"})
                return checks

            defaults = app_config.defaults

            # 2. Check Tenant ID
            tenant_id = defaults.get("tenant_id")
            if tenant_id:
                try:
                    t_resp = await client.get(f"{self.netbox_url}/api/tenancy/tenants/{tenant_id}/", headers=headers)
                    if t_resp.status_code == 200:
                        t_name = t_resp.json().get("name", f"ID {tenant_id}")
                        checks.append({"name": f"Default Tenant (ID: {tenant_id})", "status": "pass", "detail": f"Found: '{t_name}'"})
                    else:
                        checks.append({"name": f"Default Tenant (ID: {tenant_id})", "status": "fail", "detail": f"Tenant ID {tenant_id} not found in NetBox"})
                except Exception as e:
                    checks.append({"name": f"Default Tenant (ID: {tenant_id})", "status": "fail", "detail": str(e)})

            # 3. Check Site ID
            site_id = defaults.get("site_id")
            if site_id:
                try:
                    s_resp = await client.get(f"{self.netbox_url}/api/dcim/sites/{site_id}/", headers=headers)
                    if s_resp.status_code == 200:
                        s_name = s_resp.json().get("name", f"ID {site_id}")
                        checks.append({"name": f"Default Site (ID: {site_id})", "status": "pass", "detail": f"Found: '{s_name}'"})
                    else:
                        checks.append({"name": f"Default Site (ID: {site_id})", "status": "fail", "detail": f"Site ID {site_id} not found in NetBox"})
                except Exception as e:
                    checks.append({"name": f"Default Site (ID: {site_id})", "status": "fail", "detail": str(e)})

            # 4. Check Cluster ID
            cluster_id = defaults.get("cluster_id")
            if cluster_id:
                try:
                    c_resp = await client.get(f"{self.netbox_url}/api/virtualization/clusters/{cluster_id}/", headers=headers)
                    if c_resp.status_code == 200:
                        c_name = c_resp.json().get("name", f"ID {cluster_id}")
                        checks.append({"name": f"Default Cluster (ID: {cluster_id})", "status": "pass", "detail": f"Found: '{c_name}'"})
                    else:
                        checks.append({"name": f"Default Cluster (ID: {cluster_id})", "status": "fail", "detail": f"Cluster ID {cluster_id} not found in NetBox"})
                except Exception as e:
                    checks.append({"name": f"Default Cluster (ID: {cluster_id})", "status": "fail", "detail": str(e)})

            # 5. Check VM Role ID
            role_vm_id = defaults.get("role_vm_id")
            if role_vm_id:
                try:
                    r_resp = await client.get(f"{self.netbox_url}/api/dcim/device-roles/{role_vm_id}/", headers=headers)
                    if r_resp.status_code == 200:
                        r_name = r_resp.json().get("name", f"ID {role_vm_id}")
                        checks.append({"name": f"Default VM Role (ID: {role_vm_id})", "status": "pass", "detail": f"Found: '{r_name}'"})
                    else:
                        checks.append({"name": f"Default VM Role (ID: {role_vm_id})", "status": "fail", "detail": f"Role ID {role_vm_id} not found in NetBox"})
                except Exception as e:
                    checks.append({"name": f"Default VM Role (ID: {role_vm_id})", "status": "fail", "detail": str(e)})

            # 6. Check LXC Role ID
            role_lxc_id = defaults.get("role_lxc_id")
            if role_lxc_id:
                try:
                    r_resp = await client.get(f"{self.netbox_url}/api/dcim/device-roles/{role_lxc_id}/", headers=headers)
                    if r_resp.status_code == 200:
                        r_name = r_resp.json().get("name", f"ID {role_lxc_id}")
                        checks.append({"name": f"Default LXC Role (ID: {role_lxc_id})", "status": "pass", "detail": f"Found: '{r_name}'"})
                    else:
                        checks.append({"name": f"Default LXC Role (ID: {role_lxc_id})", "status": "fail", "detail": f"Role ID {role_lxc_id} not found in NetBox"})
                except Exception as e:
                    checks.append({"name": f"Default LXC Role (ID: {role_lxc_id})", "status": "fail", "detail": str(e)})

            # 7. Check NetBox DNS Plugin installed, then check zone
            zone_name = app_config.dns.get("default_zone", "homelab.local")
            dns_enabled = app_config.dns.get("auto_register_a", True)
            try:
                # First probe: does the netbox-dns plugin respond at all?
                plugin_resp = await client.get(f"{self.netbox_url}/api/plugins/netbox-dns/zones/", headers=headers)
                if plugin_resp.status_code == 404:
                    if dns_enabled:
                        checks.append({
                            "name": "NetBox DNS Plugin",
                            "status": "warn",
                            "detail": "Plugin 'netbox-dns' is not installed. DNS auto-registration is enabled in config.yml but will fail. Install netbox-dns or set dns.auto_register_a: false",
                        })
                    else:
                        checks.append({
                            "name": "NetBox DNS Plugin",
                            "status": "pass",
                            "detail": "Not installed (OK — dns.auto_register_a is disabled in config.yml)",
                        })
                elif plugin_resp.status_code == 200:
                    checks.append({"name": "NetBox DNS Plugin", "status": "pass", "detail": "Installed and responding"})
                    # Second probe: does the configured zone exist?
                    z_resp = await client.get(f"{self.netbox_url}/api/plugins/netbox-dns/zones/?name={zone_name}", headers=headers)
                    if z_resp.status_code == 200 and z_resp.json().get("results"):
                        checks.append({"name": f"DNS Zone '{zone_name}'", "status": "pass", "detail": "Zone exists in NetBox DNS"})
                    else:
                        checks.append({
                            "name": f"DNS Zone '{zone_name}'",
                            "status": "warn",
                            "detail": f"Zone '{zone_name}' not found. Create it in NetBox → Plugins → DNS → Zones, or update dns.default_zone in config.yml",
                        })
                else:
                    checks.append({"name": "NetBox DNS Plugin", "status": "warn", "detail": f"Unexpected response HTTP {plugin_resp.status_code}"})
            except Exception as e:
                checks.append({"name": "NetBox DNS Plugin", "status": "warn", "detail": f"DNS plugin check skipped: {e}"})

        return checks

    async def check_proxmox_live(self) -> List[Dict[str, Any]]:
        """Validates Proxmox VE API connectivity and presence of target node, storage, and bridge."""
        checks = []
        if not self.proxmox_host or not self.proxmox_user or not self.proxmox_token_value:
            checks.append({"name": "Proxmox Live Probe", "status": "fail", "detail": "Skipped: Missing Proxmox credentials"})
            return checks

        from app.drivers.proxmox import proxmox_driver
        loop = asyncio.get_running_loop()

        def _probe_proxmox():
            pve = proxmox_driver.get_client()
            version_data = pve.version.get()
            node = proxmox_driver.resolve_node(None)
            node_status = pve.nodes(node).status.get()
            storages = [s.get("storage") for s in pve.nodes(node).storage.get()]
            networks = [n.get("iface") for n in pve.nodes(node).network.get()]
            return {
                "version": version_data.get("version"),
                "release": version_data.get("release"),
                "node": node,
                "uptime": node_status.get("uptime", 0),
                "storages": storages,
                "networks": networks,
            }

        try:
            pve_info = await loop.run_in_executor(None, _probe_proxmox)
            checks.append({
                "name": "Proxmox VE Connectivity",
                "status": "pass",
                "detail": f"Connected to Proxmox VE {pve_info['version']}-{pve_info['release']} on {self.proxmox_host}:{self.proxmox_port}",
            })
            checks.append({
                "name": f"Default Node '{pve_info['node']}'",
                "status": "pass",
                "detail": f"Online (uptime: {pve_info['uptime'] // 86400}d)",
            })

            # Check default storage
            default_storage = app_config.defaults.get("storage", "zfs-storage")
            if default_storage in pve_info["storages"]:
                checks.append({
                    "name": f"Default Storage '{default_storage}'",
                    "status": "pass",
                    "detail": f"Available on node '{pve_info['node']}'",
                })
            else:
                checks.append({
                    "name": f"Default Storage '{default_storage}'",
                    "status": "fail",
                    "detail": f"Storage '{default_storage}' not found on node. Available: {pve_info['storages']}",
                })

            # Check default bridge
            default_bridge = app_config.defaults.get("bridge", "vmbr0")
            if default_bridge in pve_info["networks"]:
                checks.append({
                    "name": f"Default Network Bridge '{default_bridge}'",
                    "status": "pass",
                    "detail": f"Available on node '{pve_info['node']}'",
                })
            else:
                checks.append({
                    "name": f"Default Network Bridge '{default_bridge}'",
                    "status": "warn",
                    "detail": f"Bridge '{default_bridge}' not found in node network list",
                })

        except Exception as e:
            checks.append({"name": "Proxmox VE Connectivity", "status": "fail", "detail": f"Failed to connect: {e}"})

        return checks

    async def run_all_checks(self) -> Dict[str, Any]:
        """Runs the complete suite of pre-flight checks."""
        env_checks = await self.check_env_secrets()
        netbox_checks = await self.check_netbox_live()
        proxmox_checks = await self.check_proxmox_live()

        all_checks = env_checks + netbox_checks + proxmox_checks
        has_fail = any(c["status"] == "fail" for c in all_checks)
        has_warn = any(c["status"] == "warn" for c in all_checks)

        overall = "fail" if has_fail else ("warn" if has_warn else "pass")

        return {
            "overall_status": overall,
            "total_checks": len(all_checks),
            "passed": sum(1 for c in all_checks if c["status"] == "pass"),
            "failed": sum(1 for c in all_checks if c["status"] == "fail"),
            "warnings": sum(1 for c in all_checks if c["status"] == "warn"),
            "checks": all_checks,
        }

    @staticmethod
    def print_terminal_report(report: Dict[str, Any]) -> None:
        """Prints a human-readable CLI diagnostic summary."""
        print("\n" + "=" * 70)
        print("        NPU ORCHESTRATOR PRE-FLIGHT DIAGNOSTIC REPORT")
        print("=" * 70)
        
        status_icons = {
            "pass": "\033[92m✔ PASS\033[0m",
            "warn": "\033[93m⚠️ WARN\033[0m",
            "fail": "\033[91m✖ FAIL\033[0m",
        }

        for c in report["checks"]:
            badge = status_icons.get(c["status"], c["status"].upper())
            print(f"[{badge}] {c['name']:<35} : {c['detail']}")

        print("-" * 70)
        overall_badge = status_icons.get(report["overall_status"], report["overall_status"].upper())
        print(f"OVERALL STATUS: [{overall_badge}]  (Passed: {report['passed']}, Failed: {report['failed']}, Warnings: {report['warnings']})")
        print("=" * 70 + "\n")


    @staticmethod
    def print_summary_report(report: Dict[str, Any], indent: str = "       ") -> None:
        """Prints a concise 4-line summary report for installer integration."""
        checks = {c["name"]: c for c in report["checks"]}

        # 1. Credentials
        auth_ok = all(checks.get(k, {}).get("status") == "pass" for k in [
            "NetBox Credentials", "Proxmox Credentials", "API Key Protection", "Webhook HMAC Secret"
        ])
        if auth_ok:
            print(f"{indent}\033[92m✔\033[0m Credentials & Security: NetBox Token, Proxmox Token, Webhook Secret")
        else:
            print(f"{indent}\033[91m✖\033[0m Credentials & Security: Authentication failed")

        # 2. NetBox Topology
        site_match = [c for k, c in checks.items() if "Site" in k]
        cluster_match = [c for k, c in checks.items() if "Cluster" in k]
        tenant_match = [c for k, c in checks.items() if "Tenant" in k]

        site_name = site_match[0]["detail"].replace("Found: ", "Site: ") if site_match else ""
        cluster_name = cluster_match[0]["detail"].replace("Found: ", "Cluster: ") if cluster_match else ""
        tenant_name = tenant_match[0]["detail"].replace("Found: ", "Tenant: ") if tenant_match else ""

        nb_ok = not any(c["status"] == "fail" for k, c in checks.items() if any(t in k for t in ["Tenant", "Site", "Cluster", "Role", "DNS", "NetBox API"]))
        if nb_ok:
            print(f"{indent}\033[92m✔\033[0m NetBox Topology: {site_name}, {cluster_name}, {tenant_name}")
        else:
            print(f"{indent}\033[91m✖\033[0m NetBox Topology: Resource lookup failed")

        # 3. Hypervisor
        pve_conn = [c for k, c in checks.items() if "Proxmox VE Connectivity" in k]
        pve_detail = pve_conn[0]["detail"] if pve_conn else "Proxmox VE"

        pve_ok = not any(c["status"] == "fail" for k, c in checks.items() if any(t in k for t in ["Proxmox VE", "Node", "Storage", "Bridge"]))
        if pve_ok:
            print(f"{indent}\033[92m✔\033[0m Hypervisor: {pve_detail} (Storage & Bridge online)")
        else:
            print(f"{indent}\033[91m✖\033[0m Hypervisor: Node or storage check failed")

        # 4. Overall tally
        passed = report["passed"]
        total = report["total_checks"]
        if report["overall_status"] == "pass":
            print(f"{indent}\033[92m✔\033[0m All {passed}/{total} diagnostic probes passed ({passed}/{total})")
        else:
            print(f"{indent}\033[91m✖\033[0m Diagnostic check failed: {report['failed']} probe(s) failed")
            for c in report["checks"]:
                if c["status"] == "fail":
                    print(f"{indent}   \033[91m✖ {c['name']}: {c['detail']}\033[0m")


preflight_checker = PreflightChecker()


if __name__ == "__main__":
    rep = asyncio.run(preflight_checker.run_all_checks())
    if "--summary" in sys.argv:
        preflight_checker.print_summary_report(rep)
    else:
        preflight_checker.print_terminal_report(rep)
    if rep["overall_status"] == "fail":
        sys.exit(1)
    sys.exit(0)
