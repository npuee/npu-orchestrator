import time
import logging
import urllib.parse
import re
from typing import Optional, List, Dict, Any, Tuple, Callable
from pathlib import Path
from proxmoxer import ProxmoxAPI
from app.core.config import settings

logger = logging.getLogger("orchestrator.proxmox")


class ProxmoxDriver:
    def __init__(self):
        self._pve: Optional[ProxmoxAPI] = None

    def get_client(self) -> ProxmoxAPI:
        """Lazily initialize and return ProxmoxAPI client."""
        if self._pve is None:
            auth_kwargs = {
                "host": settings.PROXMOX_HOST,
                "port": settings.PROXMOX_PORT,
                "user": settings.PROXMOX_USER,
                "verify_ssl": settings.PROXMOX_VERIFY_SSL,
                "timeout": 30,
            }
            if settings.PROXMOX_TOKEN_NAME and settings.PROXMOX_TOKEN_VALUE:
                auth_kwargs["token_name"] = settings.PROXMOX_TOKEN_NAME
                auth_kwargs["token_value"] = settings.PROXMOX_TOKEN_VALUE
            elif settings.PROXMOX_PASSWORD:
                auth_kwargs["password"] = settings.PROXMOX_PASSWORD
            else:
                logger.warning("No Proxmox token or password provided; calls may fail authentication")

            self._pve = ProxmoxAPI(**auth_kwargs)
        return self._pve

    def wait_for_task(self, node: str, upid: str, timeout: int = 600, poll_interval: float = 2.0) -> bool:
        """
        Polls a Proxmox task UPID until completed.
        Raises RuntimeError if task exits with non-OK status.
        """
        pve = self.get_client()
        start = time.time()
        logger.info("Waiting for Proxmox task UPID: %s on node %s", upid, node)
        
        while time.time() - start < timeout:
            task = pve.nodes(node).tasks(upid).status.get()
            if task.get("status") == "stopped":
                exit_status = task.get("exitstatus", "OK")
                if exit_status == "OK":
                    logger.info("Task %s completed successfully", upid)
                    return True
                raise RuntimeError(f"Proxmox task {upid} failed with exitstatus: {exit_status}")
            time.sleep(poll_interval)
            
        raise TimeoutError(f"Proxmox task {upid} timed out after {timeout} seconds")

    def get_next_vmid(self) -> int:
        """Fetches the next available VMID in the cluster."""
        pve = self.get_client()
        next_id = int(pve.cluster.nextid.get())
        logger.info("Fetched next available VMID: %d", next_id)
        return next_id

    def get_online_nodes(self) -> List[str]:
        """Returns list of online node names in the cluster."""
        pve = self.get_client()
        try:
            nodes_data = pve.nodes.get()
            return [n["node"] for n in nodes_data if n.get("status") == "online"]
        except Exception as e:
            logger.warning("Could not query /nodes endpoint: %s", e)
            return []

    def resolve_node(self, requested_node: Optional[str] = None) -> str:
        """
        Resolves the actual node name to use.
        If requested_node is provided and exists, uses it.
        Otherwise falls back to PROXMOX_DEFAULT_NODE if it exists, or auto-selects the first online cluster node.
        """
        online_nodes = self.get_online_nodes()
        if requested_node:
            if not online_nodes or requested_node in online_nodes:
                return requested_node
            logger.warning("Requested node '%s' not in online nodes %s; selecting available node", requested_node, online_nodes)
            
        if settings.PROXMOX_DEFAULT_NODE and settings.PROXMOX_DEFAULT_NODE in online_nodes:
            return settings.PROXMOX_DEFAULT_NODE

        if online_nodes:
            logger.info("Auto-selected active Proxmox node: '%s'", online_nodes[0])
            return online_nodes[0]

        return settings.PROXMOX_DEFAULT_NODE or "pve"

    def find_vm_by_name(self, hostname: str) -> Optional[Dict[str, Any]]:
        """
        Searches the Proxmox VE cluster for any existing QEMU VM or LXC container
        with a matching name (case-insensitive).
        Returns a dictionary with vmid, name, node, type, and status if found, else None.
        """
        pve = self.get_client()
        target_name = hostname.strip().lower()
        try:
            resources = pve.cluster.resources.get(type="vm")
            for r in resources:
                r_name = str(r.get("name") or "").strip().lower()
                if r_name == target_name:
                    return {
                        "vmid": int(r["vmid"]),
                        "name": r.get("name"),
                        "node": r.get("node"),
                        "type": r.get("type", "qemu"),
                        "status": r.get("status", "unknown"),
                    }
        except Exception as e:
            logger.warning("Could not query cluster resources in find_vm_by_name: %s", e)
        return None

    def list_templates(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Discovers all template/source VMs on the cluster or a specific node, categorized into Linux and Windows.
        Convention:
        - Linux templates start with 90 (e.g. 9000-9099 / 90xx)
        - Windows templates start with 92 (e.g. 9200-9299 / 92xx)
        """
        pve = self.get_client()
        target_nodes = [self.resolve_node(node)] if node else self.get_online_nodes()
        if not target_nodes:
            target_nodes = [self.resolve_node(None)]
        
        templates = []
        for target_node in target_nodes:
            try:
                vms = pve.nodes(target_node).qemu.get()
                for vm in vms:
                    vmid = int(vm.get("vmid", 0))
                    vmid_str = str(vmid)
                    name = vm.get("name", f"vm-{vmid}")
                    is_template_flag = vm.get("template") == 1
                    
                    is_linux_range = vmid_str.startswith("90")
                    is_win_range = vmid_str.startswith("92")

                    if is_template_flag or is_linux_range or is_win_range:
                        if is_win_range or "win" in name.lower() or "windows" in name.lower():
                            category = "windows"
                        else:
                            category = "linux"

                        templates.append({
                            "vmid": vmid,
                            "name": name,
                            "node": target_node,
                            "category": category,
                            "status": vm.get("status", "unknown"),
                            "cores": vm.get("cpus"),
                            "memory_mb": int(vm.get("maxmem", 0)) // (1024 * 1024) if vm.get("maxmem") else None,
                        })
            except Exception as e:
                logger.error("Error querying VMs on node %s: %s", target_node, e)
                
        return sorted(templates, key=lambda x: x["vmid"])

    def find_default_template(self, category: str = "linux", node: Optional[str] = None) -> Tuple[int, str]:
        """
        Finds the default/latest template for the category.
        Linux: starts with 90 (e.g. 9024, 9026)
        Windows: starts with 92 (e.g. 9225)
        """
        templates = self.list_templates(node)
        
        if category == "linux":
            linux_matches = [t for t in templates if str(t["vmid"]).startswith("90") or t["category"] == "linux"]
            if linux_matches:
                chosen = linux_matches[-1]
                return chosen["vmid"], chosen["name"]
            raise ValueError("No Linux templates starting with 90 found on Proxmox node")
            
        elif category == "windows":
            win_matches = [t for t in templates if str(t["vmid"]).startswith("92") or t["category"] == "windows"]
            if win_matches:
                chosen = win_matches[-1]
                return chosen["vmid"], chosen["name"]
            logger.warning("No Windows templates starting with 92 found, falling back to 9225")
            return 9225, "Default Windows Template"

        raise ValueError(f"Unknown template category '{category}'")

    def resolve_template_for_platform(
        self,
        platform_slug: Optional[str] = None,
        platform_name: Optional[str] = None,
        platform_description: Optional[str] = None,
        requested_template_id: Optional[int] = None,
        node: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        """
        Correlates a NetBox Platform with the corresponding Proxmox template.
        Checks for deterministic metadata [Proxmox VM Template: <vmid>] before falling back to heuristics.
        Returns: (template_id, template_name, category)
        """
        # If user explicitly specified a template_id, check and return it
        if requested_template_id:
            templates = self.list_templates(node)
            for t in templates:
                if t["vmid"] == requested_template_id:
                    return t["vmid"], t["name"], t["category"]
            category = "windows" if str(requested_template_id).startswith("92") else "linux"
            return requested_template_id, f"template-{requested_template_id}", category

        # 0. Deterministic Match for LXC Platform
        desc_info = f"{platform_description or ''} {platform_slug or ''}"
        if "[Proxmox LXC Template:" in desc_info or (platform_slug and platform_slug.startswith("pve-lxc-")):
            m_lxc = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", desc_info)
            volid_name = m_lxc.group(1).strip() if m_lxc else (platform_name or "LXC Template")
            return 0, volid_name, "lxc"

        # 1. Deterministic Match from Platform Description or Slug for VM
        m_vmid = re.search(r"\[Proxmox VM Template:\s*(\d+)\]", desc_info)
        if not m_vmid:
            m_vmid = re.search(r"pve-vm-(\d+)-", desc_info)
        if m_vmid:
            target_vmid = int(m_vmid.group(1))
            templates = self.list_templates(node)
            for t in templates:
                if t["vmid"] == target_vmid:
                    return t["vmid"], t["name"], t["category"]
            cat = "windows" if str(target_vmid).startswith("92") else "linux"
            return target_vmid, f"template-{target_vmid}", cat

        combined_info = f"{platform_slug or ''} {platform_name or ''}".lower()
        templates = self.list_templates(node)

        # 1. Exact / Substring Version Match
        if "24" in combined_info or "noble" in combined_info:
            for t in templates:
                if "24" in t["name"] or t["vmid"] == 9024:
                    return t["vmid"], t["name"], "linux"

        if "26" in combined_info or "resolute" in combined_info:
            for t in templates:
                if "26" in t["name"] or t["vmid"] == 9026:
                    return t["vmid"], t["name"], "linux"

        if "2025" in combined_info or "win" in combined_info or "windows" in combined_info:
            for t in templates:
                if "2025" in t["name"] or t["vmid"] == 9225 or t["category"] == "windows":
                    return t["vmid"], t["name"], "windows"

        # 2. General Category Fallback
        if any(term in combined_info for term in ["ubuntu", "debian", "linux"]):
            tpl_id, tpl_name = self.find_default_template("linux", node)
            return tpl_id, tpl_name, "linux"
        elif any(term in combined_info for term in ["windows", "win", "server"]):
            tpl_id, tpl_name = self.find_default_template("windows", node)
            return tpl_id, tpl_name, "windows"

        # Default fallback to latest Linux template
        tpl_id, tpl_name = self.find_default_template("linux", node)
        return tpl_id, tpl_name, "linux"

    def _resolve_ssh_key(self, provided_key: Optional[str] = None) -> Optional[str]:
        """Resolves SSH key string from parameter, file, or default configuration."""
        key_list = []
        if provided_key and provided_key.strip():
            key_list.extend(line.strip() for line in provided_key.strip().splitlines() if line.strip())
        elif settings.DEFAULT_SSH_KEY and settings.DEFAULT_SSH_KEY.strip():
            key_list.extend(line.strip() for line in settings.DEFAULT_SSH_KEY.strip().splitlines() if line.strip())

        # Check configured key file if provided
        if settings.DEFAULT_SSH_KEY_FILE:
            try:
                p = Path(settings.DEFAULT_SSH_KEY_FILE)
                if p.exists() and p.is_file():
                    content = p.read_text(encoding="utf-8")
                    key_list.extend(line.strip() for line in content.splitlines() if line.strip() and not line.strip().startswith("#"))
            except Exception as e:
                logger.debug("Could not read SSH key file %s: %s", settings.DEFAULT_SSH_KEY_FILE, e)

        # Deduplicate keys while preserving order
        seen = set()
        unique_keys = []
        for k in key_list:
            if k not in seen:
                seen.add(k)
                unique_keys.append(k)

        if unique_keys:
            return "\n".join(unique_keys)
        return None

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
        log_callback=None,
        progress_callback=None,
    ) -> Dict[str, Any]:
        """
        Clones a Linux/Ubuntu template, applies cloud-init, disk resize, and starts the VM.
        Replaces clone-ubuntu.sh with native REST API calls.
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)
        target_storage = storage or settings.PROXMOX_DEFAULT_STORAGE

        # 1. Resolve Template ID
        if not template_id:
            template_id, tpl_name = self.find_default_template("linux", target_node)
            if log_callback:
                log_callback(f"Auto-selected Linux template {tpl_name} (ID: {template_id})")
        
        # 2. Resolve Target VMID
        if not vmid:
            vmid = self.get_next_vmid()
            if log_callback:
                log_callback(f"Allocated next available VMID: {vmid}")

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
        self.wait_for_task(target_node, clone_upid)
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
            self.wait_for_task(target_node, start_upid, timeout=60)
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
        storage: Optional[str] = None,
        bridge: Optional[str] = None,
        start_on_create: bool = True,
        log_callback=None,
        progress_callback=None,
    ) -> Dict[str, Any]:
        """
        Clones a Windows Server template, applies ConfigDrive2 cloud-init, hardware resources, and starts the VM.
        Replaces clone-windows.sh with native REST API calls.
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)
        target_storage = storage or settings.PROXMOX_DEFAULT_STORAGE

        # 1. Resolve Template ID
        if not template_id:
            template_id, tpl_name = self.find_default_template("windows", target_node)
            if log_callback:
                log_callback(f"Auto-selected Windows template {tpl_name} (ID: {template_id})")

        # 2. Resolve Target VMID
        if not vmid:
            vmid = self.get_next_vmid()
            if log_callback:
                log_callback(f"Allocated next available VMID: {vmid}")

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

        # 4. Clone Template
        if log_callback:
            log_callback(f"Cloning Windows template {template_id} to VMID {vmid} ('{hostname}') on {target_storage}...")
        if progress_callback:
            progress_callback(f"🚀 Proxmox clone started: Cloning Windows template {template_id} to VMID {vmid} ('{hostname}') on storage '{target_storage}'.")

        clone_upid = pve.nodes(target_node).qemu(template_id).clone.post(
            newid=vmid,
            name=hostname,
            full=1,
            storage=target_storage,
        )
        self.wait_for_task(target_node, clone_upid)
        if log_callback:
            log_callback("Windows template clone completed successfully.")
        if progress_callback:
            progress_callback(f"⚡ Clone completed! Configuring hardware specs ({cores} cores, {memory_mb} MB RAM) & starting VM {vmid}...")

        # 5. Set Hardware Resources and ZFS disk cache
        if log_callback:
            log_callback(f"Setting VM resources: {cores} cores, {memory_mb} MB RAM, {balloon_mb} MB balloon...")
        
        resource_params = {
            "cores": cores,
            "memory": memory_mb,
            "balloon": balloon_mb,
        }
        
        try:
            config = pve.nodes(target_node).qemu(vmid).config.get()
            scsi0 = config.get("scsi0", "")
            if scsi0:
                cleaned_scsi0 = re.sub(r",?cache=[^,]*", "", scsi0)
                resource_params["scsi0"] = f"{cleaned_scsi0},cache=none"
        except Exception as exc:
            logger.warning("Could not tune SCSI disk for Windows VM %d: %s", vmid, exc)

        pve.nodes(target_node).qemu(vmid).config.post(**resource_params)

        # 6. Configure Windows Cloud-Init (ConfigDrive2 on sata1)
        if log_callback:
            log_callback("Configuring Windows Cloud-Init (ConfigDrive2, Administrator password, Network)...")

        current_config = {}
        try:
            current_config = pve.nodes(target_node).qemu(vmid).config.get()
        except Exception:
            pass

        has_cloudinit_drive = any("cloudinit" in str(v) for v in current_config.values()) or "sata1" in current_config or "ide2" in current_config

        cloudinit_params = {
            "citype": "configdrive2",
            "ciuser": "Administrator",
            "cipassword": admin_password,
            "ipconfig0": f"ip={ip_cidr},gw={target_gw}",
            "nameserver": target_dns,
            "searchdomain": target_domain,
            "onboot": 1 if onboot else 0,
        }
        if not has_cloudinit_drive:
            cloudinit_params["sata1"] = f"{target_storage}:cloudinit"
        if bridge:
            cloudinit_params["net0"] = f"virtio,bridge={bridge}"

        pve.nodes(target_node).qemu(vmid).config.post(**cloudinit_params)

        # 7. Resize Disk
        try:
            config = pve.nodes(target_node).qemu(vmid).config.get()
            current_size_match = re.search(r"size=([0-9]+)([GM])", config.get("scsi0", ""))
            current_gb = 0
            if current_size_match:
                val, unit = int(current_size_match.group(1)), current_size_match.group(2)
                current_gb = val if unit == "G" else val // 1024

            if disk_size_gb > current_gb and current_gb > 0:
                if log_callback:
                    log_callback(f"Resizing primary disk from {current_gb}G to {disk_size_gb}G...")
                pve.nodes(target_node).qemu(vmid).resize.put(disk="scsi0", size=f"{disk_size_gb}G")
        except Exception as exc:
            logger.warning("Could not resize disk for Windows VM %d: %s", vmid, exc)

        # 8. Start VM
        if start_on_create:
            if log_callback:
                log_callback(f"Starting Windows VM {vmid}...")
            start_upid = pve.nodes(target_node).qemu(vmid).status.start.post()
            self.wait_for_task(target_node, start_upid, timeout=60)
            if log_callback:
                log_callback(f"Windows VM {vmid} is now running.")

        return {
            "vmid": vmid,
            "hostname": hostname,
            "ip_address": ip_address,
            "gateway": target_gw,
            "dns_server": target_dns,
            "dns_domain": target_domain,
            "disk_size_gb": disk_size_gb,
            "cores": cores,
            "memory_mb": memory_mb,
            "node": target_node,
            "status": "running" if start_on_create else "stopped",
            "category": "windows",
        }

    def find_lxc_template(self, node: str) -> str:
        """Find an available LXC OS template on storage (e.g. backups, local, zfs-storage)."""
        pve = self.get_client()
        target_node = self.resolve_node(node)
        for s in ["backups", "local", "zfs-storage"]:
            try:
                r = pve.nodes(target_node).storage(s).content.get(content="vztmpl")
                for item in r:
                    volid = item.get("volid", "")
                    if "ubuntu" in volid.lower():
                        return volid
                if r:
                    return r[0]["volid"]
            except Exception:
                continue
        return "backups:vztmpl/ubuntu-24.04-standard_24.04-2_amd64.tar.zst"

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
        pve = self.get_client()
        target_node = self.resolve_node(node)
        target_storage = storage or settings.PROXMOX_DEFAULT_STORAGE

        # 1. Allocate VMID
        if not vmid:
            vmid = int(pve.cluster.nextid.get())
            if log_callback:
                log_callback(f"Allocated next available CT ID: {vmid}")

        # 2. Resolve template volid
        if not template_volid:
            template_volid = self.find_lxc_template(target_node)
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

        resolved_ssh = self._resolve_ssh_key(ssh_key)

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
        self.wait_for_task(target_node, upid, timeout=120)

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

    def delete_vm(
        self,
        vmid: int,
        node: Optional[str] = None,
        purge: bool = True,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Stops and purges a VM or LXC Container and all associated storage from Proxmox VE.
        Idempotent: If the object does not exist, returns status='already_deleted'.
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)

        # 1. Discover whether VM/LXC exists and what node it resides on
        is_lxc = False
        try:
            resources = pve.cluster.resources.get(type="vm")
            matching = [r for r in resources if r.get("vmid") == vmid]
            if not matching:
                if log_callback:
                    log_callback(f"Object {vmid} does not exist in Proxmox cluster (already purged).")
                return {
                    "vmid": vmid,
                    "node": target_node,
                    "status": "already_deleted",
                }
            res = matching[0]
            target_node = res.get("node", target_node)
            is_lxc = (res.get("type") == "lxc")
        except Exception as e:
            logger.warning("Could not query cluster resources for VMID %d: %s. Falling back to direct node query.", vmid, e)
            try:
                pve.nodes(target_node).lxc(vmid).status.current.get()
                is_lxc = True
            except Exception:
                pass

        if is_lxc:
            # Stop LXC if running
            try:
                status_data = pve.nodes(target_node).lxc(vmid).status.current.get()
                if status_data.get("status") == "running":
                    if log_callback:
                        log_callback(f"Stopping running LXC CT {vmid} on node '{target_node}'...")
                    stop_upid = pve.nodes(target_node).lxc(vmid).status.stop.post()
                    self.wait_for_task(target_node, stop_upid, timeout=60)
            except Exception as e:
                if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                    if log_callback:
                        log_callback(f"LXC CT {vmid} does not exist on Proxmox (already deleted).")
                    return {"vmid": vmid, "node": target_node, "status": "already_deleted"}
                logger.warning("Could not stop LXC CT %d: %s", vmid, e)

            # Delete and purge LXC
            try:
                if log_callback:
                    log_callback(f"Deleting and purging LXC CT {vmid} from node '{target_node}' (purge={purge})...")
                del_upid = pve.nodes(target_node).lxc(vmid).delete(purge=1 if purge else 0)
                self.wait_for_task(target_node, del_upid, timeout=90)
            except Exception as e:
                if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                    if log_callback:
                        log_callback(f"LXC CT {vmid} does not exist on Proxmox (already deleted).")
                    return {"vmid": vmid, "node": target_node, "status": "already_deleted"}
                raise
        else:
            # Stop QEMU VM if running
            try:
                status_data = pve.nodes(target_node).qemu(vmid).status.current.get()
                if status_data.get("status") == "running":
                    if log_callback:
                        log_callback(f"Stopping running VM {vmid} on node '{target_node}'...")
                    stop_upid = pve.nodes(target_node).qemu(vmid).status.stop.post()
                    self.wait_for_task(target_node, stop_upid, timeout=60)
                    if log_callback:
                        log_callback(f"VM {vmid} stopped.")
            except Exception as e:
                if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                    if log_callback:
                        log_callback(f"VM {vmid} does not exist on Proxmox (already deleted).")
                    return {"vmid": vmid, "node": target_node, "status": "already_deleted"}
                logger.warning("Could not check/stop VM %d before deletion: %s", vmid, e)

            # Delete and purge VM
            try:
                if log_callback:
                    log_callback(f"Deleting and purging VM {vmid} from node '{target_node}' (purge={purge})...")
                del_upid = pve.nodes(target_node).qemu(vmid).delete(purge=1 if purge else 0)
                self.wait_for_task(target_node, del_upid, timeout=90)
            except Exception as e:
                if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                    if log_callback:
                        log_callback(f"VM {vmid} does not exist on Proxmox (already deleted).")
                    return {"vmid": vmid, "node": target_node, "status": "already_deleted"}
                raise

        if log_callback:
            log_callback(f"Object {vmid} successfully purged from Proxmox.")

        return {
            "vmid": vmid,
            "node": target_node,
            "status": "deleted",
        }

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
        """
        Dynamically updates hardware specifications, name, and options (onboot, cores, RAM, disk) on an existing Proxmox VM or CT.
        Diff-aware: Only sends updates to Proxmox if a configuration value actually changed.
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)
        updates = {}
        diff_summary = []

        # Determine whether this is an LXC container or a QEMU VM
        is_lxc = False
        try:
            pve.nodes(target_node).lxc(vmid).status.current.get()
            is_lxc = True
        except Exception:
            pass

        client_obj = pve.nodes(target_node).lxc(vmid) if is_lxc else pve.nodes(target_node).qemu(vmid)

        # 1. Fetch current configuration from Proxmox to detect real drift
        try:
            current_config = client_obj.config.get()
        except Exception as e:
            logger.warning("Could not read current config for VM %d: %s. Proceeding with blind update.", vmid, e)
            current_config = {}

        # Check Name (hostname)
        old_name = current_config.get("name") if not is_lxc else current_config.get("hostname")
        name_changed = False
        if name and old_name and name != old_name:
            if is_lxc:
                updates["hostname"] = name
            else:
                updates["name"] = name
            diff_summary.append(f"name: '{old_name}' -> '{name}'")
            name_changed = True
            if log_callback:
                log_callback(f"Renaming VM: '{old_name}' -> '{name}'")

        # 2. Check Onboot
        if onboot is not None:
            target_onboot = 1 if onboot else 0
            curr_onboot_val = current_config.get("onboot", 0)
            try:
                curr_onboot = int(curr_onboot_val) if curr_onboot_val is not None else 0
            except (ValueError, TypeError):
                curr_onboot = 0
            if target_onboot != curr_onboot:
                updates["onboot"] = target_onboot
                diff_summary.append(f"onboot: {curr_onboot} -> {target_onboot}")
                if log_callback:
                    log_callback(f"Changing Start on Boot: {curr_onboot} -> {target_onboot}")

        # 3. Check Cores
        if cores is not None:
            curr_cores_val = current_config.get("cores", 1)
            try:
                curr_cores = int(curr_cores_val) if curr_cores_val is not None else 1
            except (ValueError, TypeError):
                curr_cores = 1
            if cores != curr_cores:
                updates["cores"] = cores
                diff_summary.append(f"cores: {curr_cores} -> {cores}")
                if log_callback:
                    log_callback(f"Changing CPU cores: {curr_cores} -> {cores}")

        # 4. Check Memory
        if memory_mb is not None:
            curr_mem_val = current_config.get("memory", 512)
            try:
                curr_mem = int(curr_mem_val) if curr_mem_val is not None else 512
            except (ValueError, TypeError):
                curr_mem = 512
            if memory_mb != curr_mem:
                updates["memory"] = memory_mb
                diff_summary.append(f"memory: {curr_mem}MB -> {memory_mb}MB")
                if log_callback:
                    log_callback(f"Changing Memory: {curr_mem}MB -> {memory_mb}MB")

        # 5. Check Disk Size
        disk_resized = False
        if disk_size_gb:
            try:
                current_gb = 0
                if is_lxc:
                    rootfs_str = current_config.get("rootfs", "")
                    size_match = re.search(r"size=([0-9]+)([GM])", rootfs_str)
                    if size_match:
                        val, unit = int(size_match.group(1)), size_match.group(2)
                        current_gb = val if unit == "G" else val // 1024
                    if disk_size_gb > current_gb and current_gb > 0:
                        if log_callback:
                            log_callback(f"Resizing LXC rootfs from {current_gb}G to {disk_size_gb}G...")
                        client_obj.resize.put(disk="rootfs", size=f"{disk_size_gb}G")
                        diff_summary.append(f"rootfs: {current_gb}G -> {disk_size_gb}G")
                        disk_resized = True
                else:
                    scsi_str = current_config.get("scsi0", "")
                    size_match = re.search(r"size=([0-9]+)([GM])", scsi_str)
                    if size_match:
                        val, unit = int(size_match.group(1)), size_match.group(2)
                        current_gb = val if unit == "G" else val // 1024
                    if disk_size_gb > current_gb and current_gb > 0:
                        if log_callback:
                            log_callback(f"Resizing primary disk from {current_gb}G to {disk_size_gb}G...")
                        client_obj.resize.put(disk="scsi0", size=f"{disk_size_gb}G")
                        diff_summary.append(f"disk: {current_gb}G -> {disk_size_gb}G")
                        disk_resized = True
            except Exception as exc:
                logger.warning("Could not resize disk for VM %d: %s", vmid, exc)

        # 6. Apply updates if any changes detected
        if updates:
            client_obj.config.post(**updates)

        has_changes = bool(updates or disk_resized)
        return {
            "vmid": vmid,
            "node": target_node,
            "updates": updates,
            "diff_summary": diff_summary,
            "changed": has_changes,
            "name_changed": name_changed,
            "old_name": old_name if name_changed else None,
        }

    def set_vm_power_state(
        self,
        vmid: int,
        target_state: str,
        node: Optional[str] = None,
        desired_onboot: Optional[bool] = None,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Safely synchronizes VM or LXC container power state to 'running' (start) or 'stopped' (stop).
        Also manages 'onboot': disables onboot when stopping, enables/restores onboot when starting.
        Returns a dict with vmid, node, previous_status, new_status, and action ('started', 'stopped', 'noop').
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)

        # 1. Discover node and type
        is_lxc = False
        try:
            resources = pve.cluster.resources.get(type="vm")
            matching = [r for r in resources if r.get("vmid") == vmid]
            if not matching:
                if log_callback:
                    log_callback(f"Object {vmid} does not exist in Proxmox cluster.")
                return {"vmid": vmid, "node": target_node, "status": "not_found", "action": "noop"}
            res = matching[0]
            target_node = res.get("node", target_node)
            is_lxc = (res.get("type") == "lxc")
        except Exception as e:
            logger.warning("Could not query cluster resources for VMID %d: %s", vmid, e)

        client_obj = pve.nodes(target_node).lxc(vmid) if is_lxc else pve.nodes(target_node).qemu(vmid)
        status_data = client_obj.status.current.get()
        current_status = status_data.get("status", "unknown")

        target_state = target_state.lower()
        if target_state in ("start", "running", "on", "active"):
            # Update onboot on start
            target_onboot = 1 if desired_onboot is not False else 0
            try:
                client_obj.config.post(onboot=target_onboot)
                if log_callback:
                    log_callback(f"Start on Boot set to {'enabled (1)' if target_onboot == 1 else 'disabled (0)'}.")
            except Exception as exc:
                logger.warning("Could not update onboot for VM %d: %s", vmid, exc)

            if current_status == "running":
                if log_callback:
                    log_callback(f"VM/CT {vmid} is already running on '{target_node}'. No power state change needed.")
                return {
                    "vmid": vmid,
                    "node": target_node,
                    "previous_status": current_status,
                    "new_status": "running",
                    "action": "noop",
                }

            if log_callback:
                log_callback(f"Starting VM/CT {vmid} on node '{target_node}'...")
            start_upid = client_obj.status.start.post()
            self.wait_for_task(target_node, start_upid, timeout=90)
            if log_callback:
                log_callback(f"VM/CT {vmid} successfully started.")
            return {
                "vmid": vmid,
                "node": target_node,
                "previous_status": current_status,
                "new_status": "running",
                "action": "started",
            }

        elif target_state in ("stop", "stopped", "off", "shutdown", "offline"):
            if current_status == "stopped":
                # Ensure onboot=0 is set even if already stopped
                try:
                    client_obj.config.post(onboot=0)
                except Exception:
                    pass
                if log_callback:
                    log_callback(f"VM/CT {vmid} is already stopped on '{target_node}'. (onboot=0 ensured).")
                return {
                    "vmid": vmid,
                    "node": target_node,
                    "previous_status": current_status,
                    "new_status": "stopped",
                    "action": "noop",
                }

            if log_callback:
                log_callback(f"Shutting down VM/CT {vmid} on node '{target_node}' (with automatic forceStop fallback)...")
            try:
                if not is_lxc:
                    # Pass forceStop=1 and timeout so Proxmox handles graceful ACPI with fallback to hard stop in a single task
                    shut_upid = client_obj.status.shutdown.post(timeout=25, forceStop=1)
                else:
                    shut_upid = client_obj.status.shutdown.post(timeout=25)
                self.wait_for_task(target_node, shut_upid, timeout=60)
            except Exception as e:
                if log_callback:
                    log_callback(f"Shutdown task returned: {e}. Issuing stop with overruleShutdown...")
                time.sleep(2)
                try:
                    stop_upid = client_obj.status.stop.post(overruleShutdown=1)
                    self.wait_for_task(target_node, stop_upid, timeout=45)
                except Exception as stop_err:
                    logger.warning("Stop attempt error for VM %d: %s", vmid, stop_err)

            # Set onboot=0 now that the VM is stopped and unlocked
            try:
                time.sleep(1)
                client_obj.config.post(onboot=0)
                if log_callback:
                    log_callback("Disabled Start on Boot (onboot=0) to ensure VM remains off across host reboots.")
            except Exception as exc:
                logger.warning("Could not disable onboot for VM %d: %s", vmid, exc)

            if log_callback:
                log_callback(f"VM/CT {vmid} successfully stopped.")
            return {
                "vmid": vmid,
                "node": target_node,
                "previous_status": current_status,
                "new_status": "stopped",
                "action": "stopped",
            }

        else:
            raise ValueError(f"Unknown target power state '{target_state}'")

    def quarantine_vm(
        self,
        vmid: int,
        node: Optional[str] = None,
        log_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Safely decommissions and quarantines a VM/LXC Container without deleting its storage/disks.
        - Shuts down / stops the instance
        - Disables Start on Boot (onboot=0)
        - Isolates networking (link_down=1 on all network devices)
        - Sets tag: 'decommissioned'
        - Appends audit note to VM description
        """
        pve = self.get_client()
        target_node = self.resolve_node(node)

        # 1. Discover node and type
        is_lxc = False
        try:
            resources = pve.cluster.resources.get(type="vm")
            matching = [r for r in resources if r.get("vmid") == vmid]
            if not matching:
                if log_callback:
                    log_callback(f"Object {vmid} does not exist in Proxmox cluster (already gone).")
                return {"vmid": vmid, "node": target_node, "status": "already_deleted", "quarantined": False}
            res = matching[0]
            target_node = res.get("node", target_node)
            is_lxc = (res.get("type") == "lxc")
        except Exception as e:
            logger.warning("Could not query cluster resources for VMID %d: %s", vmid, e)

        client_obj = pve.nodes(target_node).lxc(vmid) if is_lxc else pve.nodes(target_node).qemu(vmid)

        # 2. Stop if running
        try:
            status_data = client_obj.status.current.get()
            if status_data.get("status") == "running":
                if log_callback:
                    log_callback(f"Stopping running VM/CT {vmid} on node '{target_node}'...")
                try:
                    shut_upid = client_obj.status.shutdown.post()
                    self.wait_for_task(target_node, shut_upid, timeout=40)
                except Exception:
                    stop_upid = client_obj.status.stop.post()
                    self.wait_for_task(target_node, stop_upid, timeout=30)
                if log_callback:
                    log_callback(f"VM/CT {vmid} stopped.")
        except Exception as e:
            logger.warning("Could not stop VM %d during quarantine: %s", vmid, e)

        # 3. Modify configuration to isolate network and disable boot
        try:
            config = client_obj.config.get()
            updates = {
                "onboot": 0,
            }
            if log_callback:
                log_callback(f"Disabled Start on Boot (onboot=0) for VM/CT {vmid}")

            # Network isolation: set link_down=1 on all netX interfaces
            if not is_lxc:
                for key, val in config.items():
                    if key.startswith("net") and isinstance(val, str) and "link_down=1" not in val:
                        updates[key] = f"{val},link_down=1"
                if updates:
                    if log_callback:
                        log_callback(f"Isolated network interfaces on VM {vmid}: link_down=1")

            # Tags: add 'decommissioned'
            existing_tags = config.get("tags", "") or ""
            tag_list = [t.strip() for t in existing_tags.split(";") if t.strip()] if ";" in existing_tags else [t.strip() for t in existing_tags.split(",") if t.strip()]
            if "decommissioned" not in tag_list:
                tag_list.append("decommissioned")
                updates["tags"] = ",".join(tag_list)
                if log_callback:
                    log_callback(f"Tagged VM {vmid} as 'decommissioned'")

            # Description: append audit trail
            existing_desc = config.get("description", "") or ""
            timestamp_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
            audit_note = f"\n\n[QUARANTINED / DECOMMISSIONED by Orchestrator on {timestamp_str}]\nDisks preserved. Networking disabled."
            updates["description"] = existing_desc + audit_note

            client_obj.config.post(**updates)
            if log_callback:
                log_callback(f"Successfully quarantined VM/CT {vmid} on node '{target_node}'. Disks and storage remain intact.")

        except Exception as e:
            logger.error("Failed to update config during quarantine of VM %d: %s", vmid, e)
            if log_callback:
                log_callback(f"Warning: Could not fully apply quarantine config: {e}")

        return {
            "vmid": vmid,
            "node": target_node,
            "status": "quarantined",
            "quarantined": True,
        }


proxmox_driver = ProxmoxDriver()
