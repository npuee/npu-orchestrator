import logging
import urllib.parse
import re
from typing import Optional, Dict, Any, Callable
from pathlib import Path

from app.core.config import settings
from app.core.app_config import app_config
from app.core.security import sanitize_ssh_public_keys
from app.drivers.proxmox.client import ProxmoxClientManager
from app.drivers.proxmox.templates import ProxmoxTemplateManager

logger = logging.getLogger("orchestrator.proxmox.qemu")


class ProxmoxQemuManager:
    """Manages QEMU Linux VM cloning, Cloud-Init provisioning, and disk sizing."""

    def __init__(self, client_mgr: ProxmoxClientManager, template_mgr: ProxmoxTemplateManager):
        self.client_mgr = client_mgr
        self.template_mgr = template_mgr

    def _resolve_ssh_key(self, provided_key: Optional[str] = None) -> Optional[str]:
        """Resolves, deduplicates, and sanitizes OpenSSH public keys from parameter or settings."""
        raw = provided_key or settings.DEFAULT_SSH_KEY
        return sanitize_ssh_public_keys(raw)

    def clone_linux_vm(
        self,
        hostname: str,
        template_id: Optional[int] = None,
        node: Optional[str] = None,
        vmid: Optional[int] = None,
        ip_address: Optional[str] = None,
        gateway: Optional[str] = None,
        dns_server: Optional[str] = None,
        dns_domain: Optional[str] = None,
        disk_size_gb: int = 20,
        cores: Optional[int] = None,
        memory_mb: Optional[int] = None,
        ssh_key: Optional[str] = None,
        ci_user: str = "root",
        storage: Optional[str] = None,
        bridge: Optional[str] = None,
        start_on_create: bool = True,
        onboot: bool = True,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Clones a Linux/Ubuntu template, applies cloud-init, disk resize, and starts the VM.
        Replaces clone-ubuntu.sh with native REST API calls.
        """
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)
        target_storage = storage or settings.PROXMOX_DEFAULT_STORAGE

        # 1. Resolve Template ID
        if not template_id:
            template_id, tpl_name = self.template_mgr.find_default_template("linux", target_node)
            if log_callback:
                log_callback(f"Auto-selected Linux template {tpl_name} (ID: {template_id})")

        # 2. Resolve Target VMID
        allocated_vmid = False
        if not vmid:
            vmid = self.client_mgr.get_next_vmid()
            allocated_vmid = True
            if log_callback:
                log_callback(f"Allocated next available VMID: {vmid}")

        try:
            # 3. Resolve IP and Network Defaults
            target_gw = gateway or settings.DEFAULT_GATEWAY
            target_dns = dns_server or settings.DEFAULT_DNS_SERVER
            target_domain = dns_domain or settings.DEFAULT_DNS_DOMAIN

            if not ip_address:
                if vmid and vmid <= 254:
                    gw_base = target_gw.rsplit(".", 1)[0]
                    ip_address = f"{gw_base}.{vmid}"
                else:
                    raise ValueError(f"No IP address provided and VMID {vmid} exceeds /24 host boundary (1-254)")
            ip_cidr = ip_address if "/" in ip_address else f"{ip_address}/24"

            # 4. Clone the Template
            if log_callback:
                log_callback(f"Cloning template {template_id} to VMID {vmid} ('{hostname}') on {target_storage}...")
            if progress_callback:
                progress_callback(f"🚀 Proxmox clone started: Cloning Linux template {template_id} to VMID {vmid} ('{hostname}') on storage '{target_storage}'.")

            clone_upid = pve.nodes(target_node).qemu(template_id).clone.post(
                newid=vmid,
                name=hostname,
                full=1,
                storage=target_storage,
            )
            clone_timeout = int(app_config.templates.get("clone_timeout_seconds", 2400))
            self.client_mgr.wait_for_task(
                target_node,
                clone_upid,
                timeout=clone_timeout,
                log_callback=log_callback,
                progress_callback=progress_callback,
            )
            if log_callback:
                log_callback("Template clone completed successfully.")
            if progress_callback:
                progress_callback(f"⚡ Clone completed! Configuring cloud-init & starting VM {vmid}...")

            # Unlock VM if needed
            try:
                pve.nodes(target_node).qemu(vmid).unlink.post(force=1)
            except Exception:
                pass

            # 5. Enforce cache=none on SCSI disk and hardware resources (Cores, RAM)
            try:
                hw_params = {}
                if cores:
                    hw_params["cores"] = cores
                if memory_mb:
                    hw_params["memory"] = memory_mb

                config = pve.nodes(target_node).qemu(vmid).config.get()
                scsi0 = config.get("scsi0", "")
                if scsi0:
                    cleaned_scsi0 = re.sub(r",?cache=[^,]*", "", scsi0)
                    hw_params["scsi0"] = f"{cleaned_scsi0},cache=none"

                if hw_params:
                    pve.nodes(target_node).qemu(vmid).config.post(**hw_params)
                    if log_callback:
                        log_callback(f"Configured hardware: {cores or 'default'} cores, {memory_mb or 'default'} MB RAM, SCSI0 cache=none.")
            except Exception as exc:
                logger.warning("Could not set hardware config on VM %d: %s", vmid, exc)

            # 6. Resize Disk if requested > template default
            try:
                config = pve.nodes(target_node).qemu(vmid).config.get()
                current_size_match = re.search(r"size=([0-9]+)([GM])", config.get("scsi0", ""))
                current_gb = 0
                if current_size_match:
                    val, unit = int(current_size_match.group(1)), current_size_match.group(2)
                    current_gb = val if unit == "G" else val // 1024

                if disk_size_gb > current_gb and current_gb > 0:
                    if log_callback:
                        log_callback(f"Resizing disk from {current_gb}G to {disk_size_gb}G...")
                    pve.nodes(target_node).qemu(vmid).resize.put(disk="scsi0", size=f"{disk_size_gb}G")
            except Exception as exc:
                logger.warning("Could not resize disk for VM %d: %s", vmid, exc)

            # 7. Configure Cloud-Init & Start on Boot
            if log_callback:
                log_callback(f"Configuring cloud-init and setting Start on Boot: {'yes' if onboot else 'no'}...")

            current_config = {}
            try:
                current_config = pve.nodes(target_node).qemu(vmid).config.get()
            except Exception:
                pass

            has_cloudinit_drive = any("cloudinit" in str(v) for v in current_config.values()) or "ide2" in current_config

            cloudinit_params = {
                "ciuser": ci_user,
                "ipconfig0": f"ip={ip_cidr},gw={target_gw}",
                "nameserver": target_dns,
                "searchdomain": target_domain,
                "onboot": 1 if onboot else 0,
            }
            if not has_cloudinit_drive:
                cloudinit_params["ide2"] = f"{target_storage}:cloudinit"
            if bridge:
                cloudinit_params["net0"] = f"virtio,bridge={bridge}"

            resolved_ssh = self._resolve_ssh_key(ssh_key)
            if resolved_ssh:
                cloudinit_params["sshkeys"] = urllib.parse.quote(resolved_ssh, safe="")
                if log_callback:
                    log_callback("Attached SSH public key to cloud-init.")

            pve.nodes(target_node).qemu(vmid).config.post(**cloudinit_params)

            # 8. Start VM
            if start_on_create:
                if log_callback:
                    log_callback(f"Starting VM {vmid}...")
                start_upid = pve.nodes(target_node).qemu(vmid).status.start.post()
                self.client_mgr.wait_for_task(target_node, start_upid, timeout=60)
                if log_callback:
                    log_callback(f"VM {vmid} is now running.")

            return {
                "vmid": vmid,
                "hostname": hostname,
                "ip_address": ip_address,
                "gateway": target_gw,
                "dns_server": target_dns,
                "dns_domain": target_domain,
                "disk_size_gb": disk_size_gb,
                "node": target_node,
                "status": "running" if start_on_create else "stopped",
                "category": "linux",
            }
        finally:
            if allocated_vmid:
                self.client_mgr.release_vmid(vmid)
