"""
Module Lifecycle & Health State Manager for NPU Orchestrator.

Architecture:
  - Core Foundation (Mandatory):
      1. Proxmox VE (Compute / Hypervisor)
      2. NetBox     (Source of Truth / DCIM / IPAM)

  - Optional Integration Modules (Pluggable via config.yml & .env):
      1. Uptime Kuma   (Automated ICMP Ping monitoring)
      2. Traefik       (Ingress service discovery & SSO tagging)
      3. DNS           (Forward A & reverse PTR record management)
      4. Telemetry     (24h time-averaged Proxmox RRD metrics sync)
      5. Templates     (Proxmox templates -> NetBox Platforms sync)
      6. Signal        (Instant operator alerting)

If an optional module is not configured or disabled, the orchestrator
skips loading, connecting, or running background syncs for it entirely.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from app.core.config import settings
except Exception:
    import os
    from pathlib import Path

    class _FallbackSettings:
        def __init__(self):
            env_path = Path(__file__).resolve().parent.parent.parent / ".env"
            if env_path.exists():
                with open(env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, _, v = line.partition("=")
                            os.environ.setdefault(k.strip(), v.strip())
            self.APP_NAME = os.environ.get("APP_NAME", "NPU Orchestrator")
            self.NETBOX_URL = os.environ.get("NETBOX_URL", "")
            self.NETBOX_TOKEN = os.environ.get("NETBOX_TOKEN", "")
            self.PROXMOX_HOST = os.environ.get("PROXMOX_HOST", "")
            self.PROXMOX_PORT = int(os.environ.get("PROXMOX_PORT", "8006"))
            self.PROXMOX_USER = os.environ.get("PROXMOX_USER", "root@pam")
            self.PROXMOX_TOKEN_NAME = os.environ.get("PROXMOX_TOKEN_NAME", "")
            self.PROXMOX_TOKEN_VALUE = os.environ.get("PROXMOX_TOKEN_VALUE", "")
            self.PROXMOX_PASSWORD = os.environ.get("PROXMOX_PASSWORD", "")
            self.PROXMOX_DEFAULT_NODE = os.environ.get("PROXMOX_DEFAULT_NODE", "proxmox")
            self.UPTIME_KUMA_URL = os.environ.get("UPTIME_KUMA_URL", "")
            self.UPTIME_KUMA_USERNAME = os.environ.get("UPTIME_KUMA_USERNAME", "")
            self.UPTIME_KUMA_PASSWORD = os.environ.get("UPTIME_KUMA_PASSWORD", "")
            self.TRAEFIK_SYNC_ENABLED = os.environ.get("TRAEFIK_SYNC_ENABLED", "true").lower() == "true"
            self.SIGNAL_ENABLED = os.environ.get("SIGNAL_ENABLED", "false").lower() == "true"
            self.SIGNAL_API_URL = os.environ.get("SIGNAL_API_URL", "")
            self.SIGNAL_SENDER = os.environ.get("SIGNAL_SENDER", "")
            recips = os.environ.get("SIGNAL_RECIPIENTS", "")
            self.SIGNAL_RECIPIENTS = [r.strip() for r in recips.split(",") if r.strip()] if recips else []
    settings = _FallbackSettings()

from app.core.app_config import app_config

logger = logging.getLogger("orchestrator.modules")


from dataclasses import dataclass
from typing import Callable

@dataclass
class ModuleDescriptor:
    name: str
    is_enabled_fn: Callable[[], bool]
    is_configured_fn: Callable[[], bool]
    details_fn: Optional[Callable[[], Dict[str, Any]]] = None
    disabled_reason_fn: Optional[Callable[[], str]] = None


class ModuleManager:
    """
    Central Extensible Module Lifecycle & Diagnostics Registry.

    To add a new integration module in the future:
      1. Add its configuration in config.yml (e.g. `vault: enabled: true`).
      2. Call `module_manager.register_module("vault", is_enabled=..., is_configured=...)`
         or simply rely on standard config.yml parsing.
      3. Use `if module_manager.is_enabled("vault"):` to guard workers/tasks.
    """

    def __init__(self):
        self._core_status: Dict[str, Dict[str, Any]] = {
            "netbox": {
                "configured": bool(settings.NETBOX_URL and settings.NETBOX_TOKEN),
                "status": "ready",
                "url": settings.NETBOX_URL,
                "last_check": None,
            },
            "proxmox": {
                "configured": bool(settings.PROXMOX_HOST and settings.PROXMOX_USER and (settings.PROXMOX_TOKEN_VALUE or settings.PROXMOX_PASSWORD)),
                "status": "ready",
                "host": f"{settings.PROXMOX_HOST}:{settings.PROXMOX_PORT}",
                "default_node": settings.PROXMOX_DEFAULT_NODE or "proxmox",
                "last_check": None,
            },
        }

        self._modules_status: Dict[str, Dict[str, Any]] = {}
        self._registry: Dict[str, ModuleDescriptor] = {}
        self._init_builtin_modules()

    def _init_builtin_modules(self):
        """Registers the 6 standard built-in optional integration modules."""
        # 1. Uptime Kuma
        self.register_module(
            name="uptime_kuma",
            is_enabled=lambda: (
                app_config.uptime_kuma.get("enabled", True)
                and bool(settings.UPTIME_KUMA_URL and settings.UPTIME_KUMA_USERNAME and settings.UPTIME_KUMA_PASSWORD)
            ),
            is_configured=lambda: bool(settings.UPTIME_KUMA_URL and settings.UPTIME_KUMA_USERNAME and settings.UPTIME_KUMA_PASSWORD),
            details=lambda: {"url": settings.UPTIME_KUMA_URL if settings.UPTIME_KUMA_URL else None},
            disabled_reason=lambda: (
                "Disabled in config.yml"
                if not app_config.uptime_kuma.get("enabled", True)
                else "Missing UPTIME_KUMA_URL or credentials in .env"
            ),
        )

        # 2. Traefik Ingress
        self.register_module(
            name="traefik",
            is_enabled=lambda: (
                getattr(settings, "TRAEFIK_SYNC_ENABLED", True)
                and app_config.traefik.get("enabled", True)
                and len(app_config.traefik.get("instances", [])) > 0
            ),
            is_configured=lambda: len(app_config.traefik.get("instances", [])) > 0,
            details=lambda: {"instances": [i.get("name") for i in app_config.traefik.get("instances", [])]},
            disabled_reason=lambda: (
                "No instances configured in config.yml"
                if not app_config.traefik.get("instances")
                else "Disabled in config.yml or .env"
            ),
        )

        # 3. DNS (Forward A & Reverse PTR)
        self.register_module(
            name="dns",
            is_enabled=lambda: bool(app_config.dns.get("default_zone")) and (
                app_config.dns.get("auto_register_a", True) or app_config.dns.get("auto_register_ptr", True)
            ),
            is_configured=lambda: bool(app_config.dns.get("default_zone")),
            details=lambda: {
                "default_zone": app_config.dns.get("default_zone"),
                "auto_register_a": app_config.dns.get("auto_register_a", True),
                "auto_register_ptr": app_config.dns.get("auto_register_ptr", True),
            },
            disabled_reason=lambda: "default_zone is empty or auto registration disabled",
        )

        # 4. Proxmox Telemetry
        self.register_module(
            name="telemetry",
            is_enabled=lambda: app_config.telemetry.get("enabled", True),
            is_configured=lambda: True,
            disabled_reason=lambda: "Disabled in config.yml",
        )

        # 5. OS Templates
        self.register_module(
            name="templates",
            is_enabled=lambda: app_config.templates.get("enabled", True),
            is_configured=lambda: True,
            disabled_reason=lambda: "Disabled in config.yml",
        )

        # 6. Signal Alerting
        self.register_module(
            name="signal",
            is_enabled=lambda: getattr(settings, "SIGNAL_ENABLED", False) and bool(getattr(settings, "SIGNAL_API_URL", None)),
            is_configured=lambda: bool(getattr(settings, "SIGNAL_API_URL", None)),
            details=lambda: {
                "sender": settings.SIGNAL_SENDER if getattr(settings, "SIGNAL_ENABLED", False) else None,
                "recipients_count": len(settings.SIGNAL_RECIPIENTS) if getattr(settings, "SIGNAL_RECIPIENTS", None) else 0,
            },
            disabled_reason=lambda: "SIGNAL_ENABLED=false or missing SIGNAL_API_URL in .env",
        )

    def register_module(
        self,
        name: str,
        is_enabled: Callable[[], bool],
        is_configured: Optional[Callable[[], bool]] = None,
        details: Optional[Callable[[], Dict[str, Any]]] = None,
        disabled_reason: Optional[Callable[[], str]] = None,
    ):
        """Registers a new pluggable module with the orchestrator."""
        desc = ModuleDescriptor(
            name=name,
            is_enabled_fn=is_enabled,
            is_configured_fn=is_configured or (lambda: bool(app_config.get(name))),
            details_fn=details,
            disabled_reason_fn=disabled_reason,
        )
        self._registry[name] = desc
        if name not in self._modules_status:
            self._modules_status[name] = {"status": "uninitialized"}

    # ── Configuration Checks ─────────────────────────────────────────────────

    def is_enabled(self, module_name: str) -> bool:
        """
        Returns True ONLY if the module is enabled in config.yml AND all
        required credentials or environment variables are present.
        If False, orchestrator avoids loading or calling the module at all.
        """
        if module_name in self._registry:
            return self._registry[module_name].is_enabled_fn()

        # Dynamic fallback: check directly against config.yml
        mod_cfg = app_config.get(module_name)
        if isinstance(mod_cfg, dict):
            return mod_cfg.get("enabled", True)
        return False

    def is_configured(self, module_name: str) -> bool:
        """Returns True if configuration or credentials exist on system."""
        if module_name in self._registry:
            return self._registry[module_name].is_configured_fn()

        # Dynamic fallback: check if config section is populated
        mod_cfg = app_config.get(module_name)
        return isinstance(mod_cfg, dict) and len(mod_cfg) > 0

    # ── State Mutators ───────────────────────────────────────────────────────

    def set_core_status(self, service: str, status: str, details: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        """Updates live status for core mandatory services (NetBox, Proxmox)."""
        if service not in self._core_status:
            self._core_status[service] = {}
        entry = self._core_status[service]
        entry["status"] = status
        entry["last_check"] = datetime.now(timezone.utc).isoformat()
        if details:
            entry.update(details)
        if error:
            entry["error"] = error
        elif "error" in entry:
            del entry["error"]

    def set_module_status(self, module_name: str, status: str, details: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        """Updates live status for an optional integration module."""
        if module_name not in self._modules_status:
            self._modules_status[module_name] = {}
        entry = self._modules_status[module_name]
        entry["status"] = status
        entry["last_sync"] = datetime.now(timezone.utc).isoformat()
        if details:
            entry.update(details)
        if error:
            entry["error"] = error
        elif "error" in entry:
            del entry["error"]

    # ── Health Status Assembler ──────────────────────────────────────────────

    def get_health_report(self) -> Dict[str, Any]:
        """
        Generates a non-blocking cached health diagnostic report including core services
        and all registered / dynamic optional integration modules.
        """
        now = datetime.now(timezone.utc).isoformat()
        core_healthy = all(s.get("status") in ("ready", "connected", "ok") for s in self._core_status.values())

        modules_report: Dict[str, Any] = {}

        # 1. Process all registered modules
        for name, desc in self._registry.items():
            enabled = desc.is_enabled_fn()
            configured = desc.is_configured_fn()
            cached_state = dict(self._modules_status.get(name, {}))

            cached_status = cached_state.get("status")
            if not cached_status or cached_status == "uninitialized":
                default_status = "connected" if name in ("uptime_kuma", "traefik") else ("configured" if name == "signal" else "active")
                curr_status = default_status if enabled else "disabled"
            else:
                curr_status = cached_status if enabled else "disabled"

            entry: Dict[str, Any] = {
                "configured": configured,
                "enabled": enabled,
                "status": curr_status,
            }
            if desc.details_fn:
                entry.update(desc.details_fn())

            # Merge cached runtime status and telemetry
            for k, v in cached_state.items():
                if k != "status":
                    entry[k] = v

            if not enabled:
                entry["status"] = "disabled"
                if desc.disabled_reason_fn:
                    entry["reason"] = desc.disabled_reason_fn()
                else:
                    entry["reason"] = "Disabled in config.yml"

            modules_report[name] = entry

        # 2. Include any dynamically reported modules not explicitly registered
        for name, cached_state in self._modules_status.items():
            if name not in modules_report:
                enabled = self.is_enabled(name)
                configured = self.is_configured(name)
                entry = {
                    "configured": configured,
                    "enabled": enabled,
                    "status": cached_state.get("status", "active" if enabled else "disabled"),
                }
                for k, v in cached_state.items():
                    if k != "status":
                        entry[k] = v
                if not enabled:
                    entry["status"] = "disabled"
                    entry["reason"] = "Disabled in config.yml"
                modules_report[name] = entry

        return {
            "status": "healthy" if core_healthy else "degraded",
            "service": settings.APP_NAME,
            "timestamp": now,
            "core": self._core_status,
            "modules": modules_report,
        }


module_manager = ModuleManager()
