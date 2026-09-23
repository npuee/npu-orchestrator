import logging
from typing import Optional, List, Dict, Any, Tuple, Callable
from proxmoxer import ProxmoxAPI

from app.drivers.proxmox.client import ProxmoxClientManager
from app.drivers.proxmox.templates import ProxmoxTemplateManager
from app.drivers.proxmox.qemu import ProxmoxQemuManager
from app.drivers.proxmox.windows import ProxmoxWindowsManager
from app.drivers.proxmox.lxc import ProxmoxLxcManager
from app.drivers.proxmox.lifecycle import ProxmoxLifecycleManager

logger = logging.getLogger("orchestrator.proxmox")


class ProxmoxDriver:
    """
    Unified Proxmox VE driver facade.
    Deconstructs the monolithic driver into focused domain sub-drivers (client,
    templates, qemu, windows, lxc, lifecycle) while providing 100% backward
    compatibility across all orchestrator modules.
    """

    def __init__(self):
        self.client_mgr = ProxmoxClientManager()
        self.template_mgr = ProxmoxTemplateManager(self.client_mgr)
        self.qemu_mgr = ProxmoxQemuManager(self.client_mgr, self.template_mgr)
        self.windows_mgr = ProxmoxWindowsManager(self.client_mgr, self.template_mgr)
        self.lxc_mgr = ProxmoxLxcManager(self.client_mgr, self.template_mgr, self.qemu_mgr)
        self.lifecycle_mgr = ProxmoxLifecycleManager(self.client_mgr)

    # --- Client & Node Resolution ---
    def get_client(self) -> ProxmoxAPI:
        return self.client_mgr.get_client()

    def wait_for_task(
        self,
        node: str,
        upid: str,
        timeout: int = 1800,
        poll_interval: float = 2.0,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        return self.client_mgr.wait_for_task(
            node=node,
            upid=upid,
            timeout=timeout,
            poll_interval=poll_interval,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

    def get_next_vmid(self) -> int:
        return self.client_mgr.get_next_vmid()

    def release_vmid(self, vmid: Optional[int]):
        return self.client_mgr.release_vmid(vmid)

    def reserve_vmid(self, vmid: Optional[int] = None):
        return self.client_mgr.reserve_vmid(vmid)

    def get_online_nodes(self) -> List[str]:
        return self.client_mgr.get_online_nodes()

    def resolve_node(self, requested_node: Optional[str] = None) -> str:
        return self.client_mgr.resolve_node(requested_node)

    def find_vm_by_name(self, hostname: str) -> Optional[Dict[str, Any]]:
        return self.client_mgr.find_vm_by_name(hostname)

    @staticmethod
    def _set_config(client_obj, is_lxc: bool, **params):
        return ProxmoxClientManager._set_config(client_obj, is_lxc, **params)

    # --- Template Management ---
    def list_templates(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.template_mgr.list_templates(node)

    def find_default_template(self, category: str = "linux", node: Optional[str] = None) -> Tuple[int, str]:
        return self.template_mgr.find_default_template(category, node)

    def resolve_template_for_platform(
        self,
        platform_slug: Optional[str] = None,
        platform_name: Optional[str] = None,
        platform_description: Optional[str] = None,
        requested_template_id: Optional[int] = None,
        node: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        return self.template_mgr.resolve_template_for_platform(
            platform_slug=platform_slug,
            platform_name=platform_name,
            platform_description=platform_description,
            requested_template_id=requested_template_id,
            node=node,
        )

    def find_lxc_template(self, node: str) -> str:
        return self.template_mgr.find_lxc_template(node)

    # --- QEMU Linux VM Provisioning ---
    def _resolve_ssh_key(self, provided_key: Optional[str] = None) -> Optional[str]:
        return self.qemu_mgr._resolve_ssh_key(provided_key)

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
        return self.qemu_mgr.clone_linux_vm(
            hostname=hostname,
            template_id=template_id,
            node=node,
            vmid=vmid,
            ip_address=ip_address,
            gateway=gateway,
            dns_server=dns_server,
            dns_domain=dns_domain,
            disk_size_gb=disk_size_gb,
            cores=cores,
            memory_mb=memory_mb,
            ssh_key=ssh_key,
            ci_user=ci_user,
            storage=storage,
            bridge=bridge,
            start_on_create=start_on_create,
            onboot=onboot,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

    # --- Windows VM Provisioning ---
    def clone_windows_vm(
        self,
        hostname: str,
        admin_password: str,
        template_id: Optional[int] = None,
        node: Optional[str] = None,
        vmid: Optional[int] = None,
        ip_address: Optional[str] = None,
        gateway: Optional[str] = None,
        dns_server: Optional[str] = None,
        dns_domain: Optional[str] = None,
        disk_size_gb: int = 32,
        cores: int = 4,
        memory_mb: int = 8192,
        balloon_mb: int = 512,
        onboot: bool = True,
        storage: Optional[str] = None,
        bridge: Optional[str] = None,
        start_on_create: bool = True,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        return self.windows_mgr.clone_windows_vm(
            hostname=hostname,
            admin_password=admin_password,
            template_id=template_id,
            node=node,
            vmid=vmid,
            ip_address=ip_address,
            gateway=gateway,
            dns_server=dns_server,
            dns_domain=dns_domain,
            disk_size_gb=disk_size_gb,
            cores=cores,
            memory_mb=memory_mb,
            balloon_mb=balloon_mb,
            onboot=onboot,
            storage=storage,
            bridge=bridge,
            start_on_create=start_on_create,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

    # --- LXC Container Provisioning ---
    def calculate_ip_and_gateway(
        self,
        vmid: int,
        ip_address: Optional[str] = None,
        gateway: Optional[str] = None,
    ) -> Tuple[str, str]:
        return self.lxc_mgr.calculate_ip_and_gateway(vmid, ip_address, gateway)

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
        return self.lxc_mgr.create_lxc_container(
            hostname=hostname,
            template_volid=template_volid,
            node=node,
            vmid=vmid,
            ip_address=ip_address,
            gateway=gateway,
            dns_server=dns_server,
            dns_domain=dns_domain,
            disk_size_gb=disk_size_gb,
            cores=cores,
            memory_mb=memory_mb,
            swap_mb=swap_mb,
            onboot=onboot,
            ssh_key=ssh_key,
            password=password,
            storage=storage,
            bridge=bridge,
            unprivileged=unprivileged,
            features=features,
            start_on_create=start_on_create,
            log_callback=log_callback,
            progress_callback=progress_callback,
        )

    # --- Lifecycle Operations ---
    def delete_vm(
        self,
        vmid: int,
        node: Optional[str] = None,
        purge: bool = True,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        return self.lifecycle_mgr.delete_vm(
            vmid=vmid,
            node=node,
            purge=purge,
            log_callback=log_callback,
        )

    def update_vm_config(
        self,
        vmid: int,
        node: Optional[str] = None,
        name: Optional[str] = None,
        onboot: Optional[bool] = None,
        cores: Optional[int] = None,
        memory_mb: Optional[int] = None,
        disk_size_gb: Optional[int] = None,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        return self.lifecycle_mgr.update_vm_config(
            vmid=vmid,
            node=node,
            name=name,
            onboot=onboot,
            cores=cores,
            memory_mb=memory_mb,
            disk_size_gb=disk_size_gb,
            log_callback=log_callback,
        )

    def set_vm_power_state(
        self,
        vmid: int,
        target_state: str,
        node: Optional[str] = None,
        desired_onboot: Optional[bool] = None,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        return self.lifecycle_mgr.set_vm_power_state(
            vmid=vmid,
            target_state=target_state,
            node=node,
            desired_onboot=desired_onboot,
            log_callback=log_callback,
        )

    def quarantine_vm(
        self,
        vmid: int,
        node: Optional[str] = None,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        return self.lifecycle_mgr.quarantine_vm(
            vmid=vmid,
            node=node,
            log_callback=log_callback,
        )


proxmox_driver = ProxmoxDriver()

__all__ = [
    "ProxmoxDriver",
    "proxmox_driver",
    "ProxmoxClientManager",
    "ProxmoxTemplateManager",
    "ProxmoxQemuManager",
    "ProxmoxWindowsManager",
    "ProxmoxLxcManager",
    "ProxmoxLifecycleManager",
]
