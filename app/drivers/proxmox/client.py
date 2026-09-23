import time
import logging
import re
import threading
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Callable
from proxmoxer import ProxmoxAPI
from app.core.config import settings
from app.core.exceptions import ProxmoxTaskFailedError, ProxmoxTaskTimeoutError

logger = logging.getLogger("orchestrator.proxmox.client")


class ProxmoxClientManager:
    """Manages Proxmox API client pooling, authentication, task monitoring, and node resolution."""

    def __init__(self):
        self._pve: Optional[ProxmoxAPI] = None
        self._vmid_lock = threading.Lock()
        self._reserved_vmids = set()

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

    def wait_for_task(
        self,
        node: str,
        upid: str,
        timeout: int = 1800,
        poll_interval: float = 2.0,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """
        Polls a Proxmox task UPID until completed.
        Optionally streams task progress logs via log_callback and progress_callback.
        Raises RuntimeError if task exits with non-OK status.
        """
        pve = self.get_client()
        start = time.time()
        last_log_check = 0.0
        last_reported_pct = -1
        last_line_read = 0
        logger.info("Waiting for Proxmox task UPID: %s on node %s (timeout: %ds)", upid, node, timeout)

        while time.time() - start < timeout:
            task = pve.nodes(node).tasks(upid).status.get()
            if task.get("status") == "stopped":
                exit_status = task.get("exitstatus", "OK")
                if exit_status == "OK" or (isinstance(exit_status, str) and exit_status.startswith("WARNINGS")):
                    logger.info("Task %s completed successfully (status: %s)", upid, exit_status)
                    return True
                raise ProxmoxTaskFailedError(upid=upid, exit_status=str(exit_status), node=node)

            now = time.time()
            if (log_callback or progress_callback) and (now - last_log_check >= 5.0):
                last_log_check = now
                try:
                    log_entries = pve.nodes(node).tasks(upid).log.get(start=last_line_read, limit=50)
                    if log_entries:
                        for entry in log_entries:
                            line_num = entry.get("n", last_line_read + 1)
                            if line_num > last_line_read:
                                last_line_read = line_num
                            text = entry.get("t", "")
                            m = re.search(r"transferred .*?\((\d+(?:\.\d+)?%)\)", text)
                            if m:
                                pct_str = m.group(1)
                                try:
                                    pct_val = int(float(pct_str.rstrip("%")))
                                except Exception:
                                    pct_val = -1
                                if pct_val >= 0 and (pct_val >= last_reported_pct + 10 or pct_val == 100):
                                    last_reported_pct = (pct_val // 10) * 10
                                    msg = f"Cloning progress: {text.strip()}"
                                    if log_callback:
                                        log_callback(msg)
                                    if progress_callback:
                                        progress_callback(f"⏳ {msg}")
                except Exception as log_err:
                    logger.debug("Could not query task log for UPID %s: %s", upid, log_err)

            time.sleep(poll_interval)

        raise ProxmoxTaskTimeoutError(upid=upid, timeout_seconds=timeout, node=node)

    def get_next_vmid(self) -> int:
        """Fetches the next available VMID in the cluster, synchronized with in-memory reservation."""
        pve = self.get_client()
        with self._vmid_lock:
            next_id = int(pve.cluster.nextid.get())
            while next_id in self._reserved_vmids:
                next_id += 1
            self._reserved_vmids.add(next_id)
            logger.info("Allocated & reserved next available VMID: %d (active reservations: %s)", next_id, list(self._reserved_vmids))
            return next_id

    def release_vmid(self, vmid: Optional[int]):
        """Releases an in-memory VMID reservation once Proxmox registration is completed or failed."""
        if not vmid:
            return
        with self._vmid_lock:
            self._reserved_vmids.discard(vmid)
            logger.debug("Released VMID reservation for %d", vmid)

    @contextmanager
    def reserve_vmid(self, vmid: Optional[int] = None):
        """Context manager to safely reserve a VMID during clone/creation tasks."""
        allocated_id = vmid
        if not allocated_id:
            allocated_id = self.get_next_vmid()
        else:
            with self._vmid_lock:
                self._reserved_vmids.add(allocated_id)
        try:
            yield allocated_id
        finally:
            self.release_vmid(allocated_id)

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

    @staticmethod
    def _set_config(client_obj, is_lxc: bool, **params):
        """
        Updates VM/CT configuration.
        Proxmox VE API uses PUT for LXC containers (/nodes/{node}/lxc/{vmid}/config)
        and POST for QEMU VMs (/nodes/{node}/qemu/{vmid}/config). Calling POST on LXC returns 501 Not Implemented.
        """
        if is_lxc:
            return client_obj.config.put(**params)
        else:
            return client_obj.config.post(**params)

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
