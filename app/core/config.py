from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List
from pathlib import Path


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "NPU Orchestrator"
    APP_ENV: str = "production"
    DEBUG: bool = False
    PORT: int = 8090
    HOST: str = "0.0.0.0"
    API_KEY: Optional[str] = None  # If set, protects manual endpoints via X-API-Key

    # Proxmox VE Settings
    PROXMOX_HOST: str = "192.168.1.100"
    PROXMOX_PORT: int = 8006
    PROXMOX_USER: str = "root@pam"
    PROXMOX_TOKEN_NAME: Optional[str] = None
    PROXMOX_TOKEN_VALUE: Optional[str] = None
    PROXMOX_PASSWORD: Optional[str] = None  # Fallback if using password auth
    PROXMOX_VERIFY_SSL: bool = False
    PROXMOX_DEFAULT_NODE: str = "pve"
    PROXMOX_DEFAULT_STORAGE: str = "zfs-storage"
    
    # Infrastructure Defaults
    DEFAULT_DNS_DOMAIN: str = "homelab.local"
    DEFAULT_DNS_SERVER: str = "192.168.1.1"
    DEFAULT_GATEWAY: str = "192.168.1.1"
    DEFAULT_BRIDGE: str = "vmbr0"
    DEFAULT_SSH_KEY: Optional[str] = None  # Raw public key string (e.g. ssh-ed25519 AAAAC3...)
    DEFAULT_SSH_KEY_FILE: Optional[str] = "/root/scripts/keys/main.pub"

    # NetBox Integration
    NETBOX_URL: Optional[str] = None
    NETBOX_TOKEN: Optional[str] = None
    NETBOX_WEBHOOK_SECRET: Optional[str] = None  # For HMAC validation

    # Signal Notifications
    SIGNAL_ENABLED: bool = True
    SIGNAL_API_URL: Optional[str] = None  # e.g., http://api.example.com/signal/send
    SIGNAL_SENDER: Optional[str] = None
    SIGNAL_RECIPIENTS: List[str] = []

    # Uptime Kuma Settings
    UPTIME_KUMA_URL: str = "http://172.31.0.1:3001"
    UPTIME_KUMA_USERNAME: Optional[str] = None
    UPTIME_KUMA_PASSWORD: Optional[str] = None

    # Traefik Sync Settings
    TRAEFIK_SYNC_ENABLED: bool = True
    TRAEFIK_SYNC_INTERVAL_MINUTES: int = 15
    TRAEFIK_SYNC_TARGET_VM_ID: int = 7

    # Persistent Storage
    SQLITE_DB_PATH: str = "/data/orchestrator.db"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def database_path(self) -> Path:
        p = Path(self.SQLITE_DB_PATH)
        # Fallback to local ./data if /data is not writable/present
        if not p.parent.exists():
            try:
                p.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                local_dir = Path(__file__).resolve().parent.parent.parent / "data"
                local_dir.mkdir(parents=True, exist_ok=True)
                if not (local_dir / "orchestrator.db").exists() and (local_dir / "automation.db").exists():
                    return local_dir / "automation.db"
                return local_dir / "orchestrator.db"
        # If orchestrator.db does not exist yet but automation.db exists, preserve legacy db
        if not p.exists() and p.name == "orchestrator.db":
            legacy = p.parent / "automation.db"
            if legacy.exists():
                return legacy
        return p


settings = Settings()
