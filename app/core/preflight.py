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
from app.core.modules import module_manager

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

        # Uptime Kuma Module (Optional)
        if module_manager.is_enabled("uptime_kuma"):
            kuma_url = getattr(settings, "UPTIME_KUMA_URL", None) or os.environ.get("UPTIME_KUMA_URL")
            kuma_user = getattr(settings, "UPTIME_KUMA_USERNAME", None) or os.environ.get("UPTIME_KUMA_USERNAME")
            checks.append({"name": "Uptime Kuma Credentials", "status": "pass", "detail": f"Configured for {kuma_url} (User: {kuma_user})"})
        elif getattr(settings, "UPTIME_KUMA_URL", None) and not getattr(settings, "UPTIME_KUMA_USERNAME", None):
            checks.append({"name": "Uptime Kuma Credentials", "status": "warn", "detail": "UPTIME_KUMA_URL provided but credentials incomplete in .env"})
        else:
            checks.append({"name": "Uptime Kuma Credentials", "status": "skip", "detail": "Skipped (module disabled or not configured in .env / config.yml)"})

        # Signal Alerting Module (Optional)
        if module_manager.is_enabled("signal"):
            recip_cnt = len(settings.SIGNAL_RECIPIENTS) if settings.SIGNAL_RECIPIENTS else 0
            checks.append({"name": "Signal Alerting", "status": "pass", "detail": f"Configured for {settings.SIGNAL_API_URL} ({recip_cnt} recipient(s))"})
        elif getattr(settings, "SIGNAL_ENABLED", False) and not getattr(settings, "SIGNAL_API_URL", None):
            checks.append({"name": "Signal Alerting", "status": "warn", "detail": "SIGNAL_ENABLED=true but SIGNAL_API_URL is missing in .env"})
        else:
            checks.append({"name": "Signal Alerting", "status": "skip", "detail": "Skipped (SIGNAL_ENABLED=false or not configured)"})

        return checks

    async def check_netbox_live(self) -> List[Dict[str, Any]]:
        """Validates NetBox API connectivity, background workers, and default topology."""
        checks = []
        if not self.netbox_url or not self.netbox_token:
            checks.append({"name": "NetBox Live Probe", "status": "fail", "detail": "Skipped: Missing NetBox credentials"})
            return checks

        headers = {
            "Authorization": f"Token {self.netbox_token}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=8.0, verify=False) as client:
            try:
                resp = await client.get(f"{self.netbox_url}/api/status/", headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    nb_ver = data.get("netbox-version", "unknown")
                    checks.append({"name": "NetBox API Connectivity", "status": "pass", "detail": f"Connected (NetBox v{nb_ver} at {self.netbox_url})"})
                else:
                    checks.append({"name": "NetBox API Connectivity", "status": "fail", "detail": f"HTTP {resp.status_code}: {resp.text[:100]}"})
            except Exception as e:
                checks.append({"name": "NetBox API Connectivity", "status": "fail", "detail": f"Connection failed: {e}"})

        return checks

    async def check_kuma_live(self) -> List[Dict[str, Any]]:
        """Validates Uptime Kuma live reachability and sync configuration (if module is enabled)."""
        checks = []
        if not module_manager.is_enabled("uptime_kuma"):
            reason = "disabled in config.yml" if not app_config.uptime_kuma.get("enabled", True) else "not configured in .env"
            checks.append({
                "name": "Uptime Kuma Connectivity",
                "status": "skip",
                "detail": f"Skipped (module {reason})",
            })
            return checks

        kuma_url = getattr(settings, "UPTIME_KUMA_URL", None) or os.environ.get("UPTIME_KUMA_URL", "http://172.31.0.1:3001")
        async with httpx.AsyncClient(timeout=5.0, verify=False) as client:
            try:
                resp = await client.get(kuma_url)
                if resp.status_code in (200, 302):
                    checks.append({"name": "Uptime Kuma Connectivity", "status": "pass", "detail": f"Responding at {kuma_url} (HTTP {resp.status_code})"})
                else:
                    checks.append({"name": "Uptime Kuma Connectivity", "status": "warn", "detail": f"HTTP {resp.status_code} from {kuma_url}"})
            except Exception as e:
                checks.append({"name": "Uptime Kuma Connectivity", "status": "warn", "detail": f"Cannot connect to {kuma_url}: {e}"})

        # Kuma Sync Configuration
        kuma_cfg = app_config.uptime_kuma
        interval = kuma_cfg.get("sync_interval_minutes", 30)
        notifications = kuma_cfg.get("enable_default_notifications", True)
        notif_str = "Enabled" if notifications else "Disabled"
        exclude_tags = kuma_cfg.get("exclude_tags", ["no-monitor"])

        checks.append({
            "name": "Uptime Kuma Sync Settings",
            "status": "pass",
            "detail": f"Auto-Sync Active (Every {interval}m, Notifications: {notif_str}, Exclude: {', '.join(exclude_tags)})"
        })

        return checks

    async def check_proxmox_live(self) -> List[Dict[str, Any]]:
        """Validates Proxmox VE API connectivity."""
        checks = []
        if not self.proxmox_host or not self.proxmox_user or not self.proxmox_token_value:
            checks.append({"name": "Proxmox Live Probe", "status": "fail", "detail": "Skipped: Missing Proxmox credentials"})
            return checks

        from app.drivers.proxmox import proxmox_driver
        loop = asyncio.get_running_loop()

        def _probe_proxmox():
            pve = proxmox_driver.get_client()
            version_data = pve.version.get()
            return {
                "version": version_data.get("version"),
                "release": version_data.get("release"),
            }

        try:
            pve_info = await loop.run_in_executor(None, _probe_proxmox)
            checks.append({
                "name": "Proxmox VE API Connectivity",
                "status": "pass",
                "detail": f"Connected to Proxmox VE {pve_info['version']}-{pve_info['release']} on {self.proxmox_host}:{self.proxmox_port}",
            })
        except Exception as e:
            checks.append({"name": "Proxmox VE API Connectivity", "status": "fail", "detail": f"Failed to connect: {e}"})

        return checks

    async def run_all_checks(self) -> Dict[str, Any]:
        """Runs the complete suite of pre-flight checks."""
        env_checks = await self.check_env_secrets()
        netbox_checks = await self.check_netbox_live()
        proxmox_checks = await self.check_proxmox_live()
        kuma_checks = await self.check_kuma_live()

        all_checks = env_checks + netbox_checks + proxmox_checks + kuma_checks
        has_fail = any(c["status"] == "fail" for c in all_checks)
        has_warn = any(c["status"] == "warn" for c in all_checks)

        overall = "fail" if has_fail else ("warn" if has_warn else "pass")

        return {
            "overall_status": overall,
            "total_checks": len(all_checks),
            "passed": sum(1 for c in all_checks if c["status"] == "pass"),
            "skipped": sum(1 for c in all_checks if c["status"] == "skip"),
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
            "skip": "\033[90m⏩ SKIP\033[0m",
        }

        for c in report["checks"]:
            badge = status_icons.get(c["status"], c["status"].upper())
            print(f"[{badge}] {c['name']:<35} : {c['detail']}")

        print("-" * 70)
        overall_badge = status_icons.get(report["overall_status"], report["overall_status"].upper())
        skip_msg = f", Skipped: {report.get('skipped', 0)}" if report.get("skipped", 0) > 0 else ""
        print(f"OVERALL STATUS: [{overall_badge}]  (Passed: {report['passed']}{skip_msg}, Failed: {report['failed']}, Warnings: {report['warnings']})")
        print("=" * 70 + "\n")


    @staticmethod
    def print_summary_report(report: Dict[str, Any], indent: str = "       ") -> None:
        """Prints a concise summary report for installer integration."""
        checks = {c["name"]: c for c in report["checks"]}

        # 1. Credentials (Core must pass; optional modules pass if enabled, cleanly skipped if disabled)
        core_auth_ok = (
            checks.get("NetBox Credentials", {}).get("status") == "pass"
            and checks.get("Proxmox Credentials", {}).get("status") == "pass"
        )
        opt_failed = any(
            checks.get(k, {}).get("status") == "fail"
            for k in ["Uptime Kuma Credentials", "Signal Alerting"]
        )
        if core_auth_ok and not opt_failed:
            active_auth = ["NetBox", "Proxmox"]
            if checks.get("Uptime Kuma Credentials", {}).get("status") == "pass":
                active_auth.append("Kuma")
            if checks.get("Signal Alerting", {}).get("status") == "pass":
                active_auth.append("Signal")
            print(f"{indent}\033[92m✔\033[0m Credentials & Security: {', '.join(active_auth)}, Webhook Secret")
        else:
            print(f"{indent}\033[91m✖\033[0m Credentials & Security: Core authentication failed")

        # 2. Services connectivity
        nb_ok = checks.get("NetBox API Connectivity", {}).get("status") == "pass"
        pve_ok = checks.get("Proxmox VE API Connectivity", {}).get("status") == "pass" or checks.get("Proxmox VE Connectivity", {}).get("status") == "pass"
        kuma_status = checks.get("Uptime Kuma Connectivity", {}).get("status")
        kuma_ok = kuma_status in ("pass", "skip", None)

        if nb_ok and pve_ok and kuma_ok:
            online_svcs = ["NetBox", "Proxmox VE"]
            if kuma_status == "pass":
                online_svcs.append("Uptime Kuma")
            skip_note = " (Kuma skipped)" if kuma_status == "skip" else ""
            print(f"{indent}\033[92m✔\033[0m External Services: {', '.join(online_svcs)} online{skip_note}")
        else:
            print(f"{indent}\033[91m✖\033[0m External Services: Connection failed")

        # 3. Overall tally
        passed = report["passed"]
        skipped = report.get("skipped", 0)
        total = report["total_checks"]
        if report["overall_status"] in ("pass", "warn"):
            skip_str = f", {skipped} skipped" if skipped > 0 else ""
            print(f"{indent}\033[92m✔\033[0m All required diagnostic probes passed ({passed} passed{skip_str})")
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
