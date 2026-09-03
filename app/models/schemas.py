from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class LinuxProvisionRequest(BaseModel):
    hostname: str = Field(..., description="Hostname for the new VM (e.g. web-app-01)")
    template_id: Optional[int] = Field(None, description="Source Template ID (9000-9999). If omitted, discovers automatically")
    node: Optional[str] = Field(None, description="Proxmox node name (e.g. pve)")
    vmid: Optional[int] = Field(None, description="Target VMID. If omitted, fetches next available ID from cluster")
    ip_address: Optional[str] = Field(None, description="Static IPv4 address with or without CIDR (e.g. 192.168.1.105)")
    gateway: Optional[str] = Field(None, description="Default gateway IPv4")
    dns_server: Optional[str] = Field(None, description="Primary DNS server")
    dns_domain: Optional[str] = Field(None, description="Search domain / DNS domain")
    disk_size_gb: int = Field(20, ge=5, le=2000, description="Disk size in GB")
    ssh_key: Optional[str] = Field(None, description="Public SSH Key string. Defaults to system configured key")
    ci_user: str = Field("root", description="Cloud-init default user")
    storage: Optional[str] = Field(None, description="Target Proxmox storage pool (default: zfs-storage)")
    start_on_create: bool = Field(True, description="Automatically start VM after provisioning")


class WindowsProvisionRequest(BaseModel):
    hostname: str = Field(..., description="Hostname for the new Windows VM")
    admin_password: str = Field(..., min_length=6, description="Initial Administrator password")
    template_id: Optional[int] = Field(None, description="Source Template ID (9200-9299). If omitted, discovers automatically")
    node: Optional[str] = Field(None, description="Proxmox node name (e.g. pve)")
    vmid: Optional[int] = Field(None, description="Target VMID. If omitted, fetches next available ID")
    ip_address: Optional[str] = Field(None, description="Static IPv4 address (e.g. 192.168.1.106)")
    gateway: Optional[str] = Field(None, description="Default gateway")
    dns_server: Optional[str] = Field(None, description="Primary DNS server")
    dns_domain: Optional[str] = Field(None, description="Search domain")
    disk_size_gb: int = Field(32, ge=20, le=4000, description="Disk size in GB")
    cores: int = Field(4, ge=1, le=64, description="CPU Cores")
    memory_mb: int = Field(8192, ge=1024, le=131072, description="RAM in MB")
    balloon_mb: int = Field(512, ge=0, description="Memory ballooning in MB")
    storage: Optional[str] = Field(None, description="Target Proxmox storage pool (default: zfs-storage)")
    start_on_create: bool = Field(True, description="Automatically start VM after provisioning")


class JobResponse(BaseModel):
    job_id: str
    status: str
    action: str
    vmid: Optional[int] = None
    hostname: Optional[str] = None
    ip_address: Optional[str] = None
    created_at: str
    updated_at: Optional[str] = None
    logs: List[str] = []
    metadata: Dict[str, Any] = {}
    error: Optional[str] = None


class TemplateInfo(BaseModel):
    vmid: int
    name: str
    node: str
    category: str  # 'linux' or 'windows'
    status: str
    cores: Optional[int] = None
    memory_mb: Optional[int] = None
