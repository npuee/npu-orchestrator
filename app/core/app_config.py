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
        "linux_vmid_prefix": "90",
        "windows_vmid_prefix": "92",
        "default_windows_password": "P@ssw0rdInitial!",
    },
    "traefik": {
        "enabled": True,
        "sync_interval_minutes": 15,
        "instances": [
            {
                "name": "traefik-oracle",
                "netbox_vm_id": 7,
                "type": "docker",
                "path": "/cloud/traefik",
                "conf_dir": "/cloud/traefik/conf",
            },
            {
                "name": "traefik-lohusuu",
                "netbox_vm_id": 6,
                "type": "api",
                "api_url": "http://192.168.1.50:8080",
            },
        ],
    },
    "dns": {
        "default_zone": "homelab.local",
        "auto_register_a": True,
        "auto_register_ptr": True,
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
    def dns(self) -> Dict[str, Any]:
        return self._data.get("dns", DEFAULT_CONFIG["dns"])


app_config = AppConfig()
