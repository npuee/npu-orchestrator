import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

logger = logging.getLogger("orchestrator.app_config")

DEFAULT_CONFIG: Dict[str, Any] = {
    "version": "1.0",
    "defaults": {
        "tenant_id": 6,
        "site_id": 2,
        "cluster_id": 2,
        "role_vm_id": 16,
        "role_lxc_id": 15,
        "storage": "zfs-storage",
        "bridge": "vmbr0",
    },
    "fallbacks": {
        "cores": 2,
        "memory_mb": 2048,
        "disk_gb": 20,
    },
    "templates": {
        "enabled": True,
        "sync_interval_minutes": 60,
        "linux_vmid_prefix": "90",
        "windows_vmid_prefix": "92",
        "default_windows_password": "P@ssw0rdInitial!",
    },
    "traefik": {
        "enabled": True,
        "sync_interval_minutes": 15,
        "service_tags": ["traefik"],
        "service_tag": ["traefik"],
        "middlewares": {
            "ip_whitelist": {
                "netbox_field": "ip_whitelist",
                "patterns": ["whitelist", "allowlist", "npu-ip-whitelist"],
            },
            "sso": {
                "netbox_field": "sso_protected",
                "patterns": ["sso", "forward-auth", "authelia", "authentik", "npu-sso"],
            },
        },
        "service_fields": {
            "public_url": "public_url",
            "fqdn": "fqdn",
            "middlewares": "middlewares",
        },
        "instances": [
            {
                "name": "traefik-local",
                "netbox_vm_id": 1,
                "type": "docker",
                "path": "/etc/traefik",
                "conf_dir": "/etc/traefik/conf",
            },
            {
                "name": "traefik-remote",
                "netbox_device_id": 1,
                "type": "api",
                "api_url": "http://192.168.1.50:8080",
            },
        ],
    },
    "telemetry": {
        "enabled": True,
        "sync_interval_minutes": 15,
    },
    "dns": {
        "default_zone": "homelab.local",
        "auto_register_a": True,
        "auto_register_ptr": True,
    },
    "uptime_kuma": {
        "enabled": True,
        "public_url": "http://localhost:3001",
        "sync_interval_minutes": 30,
        "sync_on_startup": True,
        "enable_default_notifications": True,
        "exclude_tags": ["no-monitor"],
        "group_by_site": True,
        "ping_interval": 60,
        "ping_retry_interval": 60,
        "max_retries": 3,
    },
    "database": {
        "retention_days": 30,
        "prune_interval_hours": 24,
    },
}


class AppConfig:
    """Loads and holds appliance settings from config.yml with automatic defaults."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._detect_config_path()
        self._data: Dict[str, Any] = {}
        self.reload()

    def _detect_config_path(self) -> str:
        candidates = [
            "/app/config.yml",
            "/app/config.yaml",
            "config.yml",
            "config.yaml",
            "/data/config.yml",
        ]
        for c in candidates:
            if os.path.exists(c):
                return c
        return "config.yml"

    def reload(self) -> None:
        """Loads or reloads config.yml from disk."""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    loaded = yaml.safe_load(f)
                    if isinstance(loaded, dict):
                        self._data = loaded
                        logger.info("Loaded configuration from %s", self.config_path)
                        return
            except Exception as e:
                logger.warning("Failed to parse %s, falling back to defaults: %s", self.config_path, e)
        else:
            logger.info("Config file '%s' not found, using built-in defaults.", self.config_path)
        self._data = DEFAULT_CONFIG

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    @property
    def defaults(self) -> Dict[str, Any]:
        return self._data.get("defaults", DEFAULT_CONFIG["defaults"])

    @property
    def fallbacks(self) -> Dict[str, Any]:
        return self._data.get("fallbacks", DEFAULT_CONFIG["fallbacks"])

    @property
    def templates(self) -> Dict[str, Any]:
        return self._data.get("templates", DEFAULT_CONFIG["templates"])

    @property
    def traefik(self) -> Dict[str, Any]:
        return self._data.get("traefik", DEFAULT_CONFIG["traefik"])

    @property
    def telemetry(self) -> Dict[str, Any]:
        return self._data.get("telemetry", DEFAULT_CONFIG["telemetry"])

    @property
    def dns(self) -> Dict[str, Any]:
        return self._data.get("dns", DEFAULT_CONFIG["dns"])

    @property
    def uptime_kuma(self) -> Dict[str, Any]:
        return self._data.get("uptime_kuma", DEFAULT_CONFIG["uptime_kuma"])

    @property
    def database(self) -> Dict[str, Any]:
        return self._data.get("database", DEFAULT_CONFIG["database"])


app_config = AppConfig()
