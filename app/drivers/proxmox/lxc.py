import logging
import urllib.parse
from typing import Optional, Dict, Any, Tuple, Callable

from app.core.config import settings
from app.drivers.proxmox.client import ProxmoxClientManager
from app.drivers.proxmox.templates import ProxmoxTemplateManager
from app.drivers.proxmox.qemu import ProxmoxQemuManager

logger = logging.getLogger("orchestrator.proxmox.lxc")


class ProxmoxLxcManager:
    """Manages Proxmox LXC System Container creation, network configuration, and SSH provisioning."""

    def __init__(
        self,
        client_mgr: ProxmoxClientManager,
        template_mgr: ProxmoxTemplateManager,
        qemu_mgr: ProxmoxQemuManager,
    ):
        self.client_mgr = client_mgr
        self.template_mgr = template_mgr
        self.qemu_mgr = qemu_mgr

    def calculate_ip_and_gateway(
        self,
        vmid: int,
        ip_address: Optional[str] = None,
        gateway: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Calculates IP CIDR (e.g. 192.168.1.50/24) and default gateway."""
        target_gw = gateway or settings.DEFAULT_GATEWAY
        if not ip_address:
            if vmid and vmid <= 254:
                gw_base = target_gw.rsplit(".", 1)[0]
                ip_address = f"{gw_base}.{vmid}"
            else:
                raise ValueError(f"No IP address provided and VMID {vmid} exceeds /24 host boundary (1-254)")
        ip_cidr = ip_address if "/" in ip_address else f"{ip_address}/24"
        return ip_cidr, target_gw

    def create_lxc_container(
        self,
        hostname: str,
        template_volid: Optional[str] = None,
        node: Optional[str] = None,
        vmid: Optional[int] = None,
        ip_address: Optional[str] = None,
        gateway: Optional[str] = None,
        dns_server: Optional[str] = None,
        dns_domain: Optional[str] = None,
        disk_size_gb: int = 20,
        cores: Optional[int] = 2,
        memory_mb: Optional[int] = 2048,
        swap_mb: int = 512,
        onboot: bool = True,
        ssh_key: Optional[str] = None,
        password: Optional[str] = None,
        storage: Optional[str] = None,
        bridge: Optional[str] = None,
        unprivileged: bool = True,
        features: str = "nesting=1",
        start_on_create: bool = True,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Creates, configures network/SSH, and boots a Proxmox LXC System Container (CT).
        """
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)
        target_storage = storage or settings.PROXMOX_DEFAULT_STORAGE

        # 1. Allocate VMID
        if not vmid:
            vmid = int(pve.cluster.nextid.get())
            if log_callback:
                log_callback(f"Allocated next available CT ID: {vmid}")

        # 2. Resolve template volid
        if not template_volid:
            template_volid = self.template_mgr.find_lxc_template(target_node)
            if log_callback:
                log_callback(f"Resolved LXC template: '{template_volid}'")

        # 3. Calculate IP & Network settings
        ip_cidr, target_gw = self.calculate_ip_and_gateway(vmid, ip_address, gateway)
        target_dns = dns_server or settings.DEFAULT_DNS_SERVER
        target_domain = dns_domain or settings.DEFAULT_DNS_DOMAIN

        # 4. Create LXC Container
        if log_callback:
            log_callback(f"Creating LXC Container {vmid} ('{hostname}') on {target_storage} with {cores or 2} cores, {memory_mb or 2048}MB RAM, {disk_size_gb}GB disk...")
        if progress_callback:
            progress_callback(f"🚀 Proxmox LXC creation started: Building CT {vmid} ('{hostname}') from '{template_volid}' on node '{target_node}'...")

        resolved_ssh = self.qemu_mgr._resolve_ssh_key(ssh_key)

        lxc_params = {
            "vmid": vmid,
            "hostname": hostname,
            "ostemplate": template_volid,
            "rootfs": f"{target_storage}:{disk_size_gb}",
            "cores": cores or 2,
            "memory": memory_mb or 2048,
            "swap": swap_mb,
            "net0": f"name=eth0,bridge={bridge or settings.DEFAULT_BRIDGE},ip={ip_cidr},gw={target_gw},type=veth",
            "nameserver": target_dns,
            "searchdomain": target_domain,
            "onboot": 1 if onboot else 0,
            "unprivileged": 1 if unprivileged else 0,
            "features": features,
            "start": 1 if start_on_create else 0,
        }
        if password:
            lxc_params["password"] = password
            if log_callback:
                log_callback("Configured root password for container console access.")

        if resolved_ssh:
            lxc_params["ssh-public-keys"] = urllib.parse.quote(resolved_ssh, safe="")
            if log_callback:
                log_callback("Attached SSH public key(s) to LXC container.")

        upid = pve.nodes(target_node).lxc.post(**lxc_params)
        self.client_mgr.wait_for_task(
            target_node,
            upid,
            timeout=300,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

        if log_callback:
            log_callback(f"LXC Container {vmid} is now running!")
        if progress_callback:
            progress_callback(f"⚡ CT {vmid} created successfully! Container is now active.")

        return {
            "vmid": vmid,
            "hostname": hostname,
            "ip_address": ip_cidr.split("/")[0],
            "gateway": target_gw,
            "dns_server": target_dns,
            "dns_domain": target_domain,
            "disk_size_gb": disk_size_gb,
            "cores": cores or 2,
            "memory_mb": memory_mb or 2048,
            "node": target_node,
            "status": "running" if start_on_create else "stopped",
            "category": "lxc",
        }
