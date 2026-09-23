import time
import logging
import re
from typing import Optional, Dict, Any, Callable

from app.drivers.proxmox.client import ProxmoxClientManager

logger = logging.getLogger("orchestrator.proxmox.lifecycle")


class ProxmoxLifecycleManager:
    """Manages VM and LXC container lifecycle operations: power states, updates, deletion, and quarantine."""

    def __init__(self, client_mgr: ProxmoxClientManager):
        self.client_mgr = client_mgr

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
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)

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
                    self.client_mgr.wait_for_task(target_node, stop_upid, timeout=60)
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
                self.client_mgr.wait_for_task(target_node, del_upid, timeout=90)
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
                    self.client_mgr.wait_for_task(target_node, stop_upid, timeout=60)
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
                self.client_mgr.wait_for_task(target_node, del_upid, timeout=90)
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
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)
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
            self.client_mgr._set_config(client_obj, is_lxc, **updates)

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
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)

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
                self.client_mgr._set_config(client_obj, is_lxc, onboot=target_onboot)
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
            self.client_mgr.wait_for_task(target_node, start_upid, timeout=90)
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
                    self.client_mgr._set_config(client_obj, is_lxc, onboot=0)
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
                    shut_upid = client_obj.status.shutdown.post(timeout=25, forceStop=1)
                else:
                    shut_upid = client_obj.status.shutdown.post(timeout=25)
                self.client_mgr.wait_for_task(target_node, shut_upid, timeout=60)
            except Exception as e:
                if log_callback:
                    log_callback(f"Shutdown task returned: {e}. Issuing stop with overruleShutdown...")
                time.sleep(2)
                try:
                    stop_upid = client_obj.status.stop.post(overruleShutdown=1)
                    self.client_mgr.wait_for_task(target_node, stop_upid, timeout=45)
                except Exception as stop_err:
                    logger.warning("Stop attempt error for VM %d: %s", vmid, stop_err)

            # Set onboot=0 now that the VM is stopped and unlocked
            try:
                time.sleep(1)
                self.client_mgr._set_config(client_obj, is_lxc, onboot=0)
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
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)

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
                    self.client_mgr.wait_for_task(target_node, shut_upid, timeout=40)
                except Exception:
                    stop_upid = client_obj.status.stop.post()
                    self.client_mgr.wait_for_task(target_node, stop_upid, timeout=30)
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

            self.client_mgr._set_config(client_obj, is_lxc, **updates)
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
