import asyncio
import datetime
import logging
from typing import Any, Dict, List, Optional, Tuple
import httpx

from app.core.config import settings
from app.drivers.proxmox import proxmox_driver
from app.drivers.netbox import netbox_driver

logger = logging.getLogger("orchestrator.metrics_sync")


class MetricsSyncDriver:
    """
    Synchronizes 24-Hour Time-Averaged Telemetry & Health Metrics (with Peak Values)
    from Proxmox VE RRD history into NetBox Virtual Machines.
    """

    @staticmethod
    def _format_uptime(uptime_seconds: int) -> str:
        if not uptime_seconds or uptime_seconds <= 0:
            return "Offline"
        days = uptime_seconds // 86400
        hours = (uptime_seconds % 86400) // 3600
        minutes = (uptime_seconds % 3600) // 60
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    @staticmethod
    def _format_bytes(bytes_val: int) -> str:
        if not bytes_val or bytes_val <= 0:
            return "0 GB"
        gb = bytes_val / (1024 ** 3)
        return f"{gb:.2f} GB"

    def _get_vm_rrd_stats(self, pve: Any, node: str, vmid: int, vtype: str, max_mem_allocated: int) -> Tuple[str, str]:
        """
        Queries Proxmox RRD 24-hour time series for a VM/LXC to compute:
        1. 24h Average CPU with Peak CPU: e.g. '4.9% (Peak: 25.1%)'
        2. 24h Average RAM vs Allocated RAM: e.g. '2.32 GB / 8.00 GB (29.1%)'
        """
        try:
            if vtype == "lxc":
                rrd = pve.nodes(node).lxc(vmid).rrddata.get(timeframe="day")
            else:
                rrd = pve.nodes(node).qemu(vmid).rrddata.get(timeframe="day")

            valid_cpu = [pt["cpu"] for pt in rrd if pt.get("cpu") is not None]
            valid_mem = [pt["mem"] for pt in rrd if pt.get("mem") is not None]
            max_mem_rrd = max([pt["maxmem"] for pt in rrd if pt.get("maxmem") is not None], default=0)
            max_mem = max_mem_rrd if max_mem_rrd > 0 else max_mem_allocated

            if valid_cpu:
                avg_cpu = (sum(valid_cpu) / len(valid_cpu)) * 100
                peak_cpu = max(valid_cpu) * 100
                cpu_str = f"{avg_cpu:.1f}% (Peak: {peak_cpu:.1f}%)"
            else:
                cpu_str = "0.0%"

            if valid_mem and max_mem > 0:
                avg_mem = sum(valid_mem) / len(valid_mem)
                avg_mem_pct = (avg_mem / max_mem) * 100
                mem_str = f"{self._format_bytes(int(avg_mem))} / {self._format_bytes(max_mem)} ({avg_mem_pct:.1f}%)"
            elif max_mem > 0:
                mem_str = f"0 GB / {self._format_bytes(max_mem)} (0%)"
            else:
                mem_str = "N/A"

            return cpu_str, mem_str
        except Exception as e:
            logger.warning("Could not fetch RRD stats for VMID %d: %s; falling back to live stats", vmid, e)
            return "", ""

    @staticmethod
    def _get_guest_agent_status(pve: Any, node: str, vmid: int, vtype: str, vm_status: str) -> str:
        """
        Queries Proxmox QEMU Guest Agent health and returns a descriptive status string.
        """
        if vtype == "lxc":
            return "Native (LXC)"

        if vm_status != "running":
            return "Offline (VM Stopped)"

        try:
            cfg = pve.nodes(node).qemu(vmid).config.get()
            agent_cfg = str(cfg.get("agent", "0"))
            if agent_cfg in ("0", "false", ""):
                return "Not Enabled in Proxmox"

            info = pve.nodes(node).qemu(vmid).agent.info.get()
            version = None
            if isinstance(info, dict):
                version = info.get("result", {}).get("version") or info.get("version")
            if version:
                return f"Running (v{version})"
            return "Running"
        except Exception as exc:
            err = str(exc).lower()
            if "not running" in err:
                return "Not Running in Guest"
            elif "not configured" in err:
                return "Not Enabled in Proxmox"
            elif "is not running" in err:
                return "Offline (VM Stopped)"
            return "Unreachable"

    def fetch_proxmox_telemetry(self) -> List[Dict[str, Any]]:
        """Queries Proxmox cluster resources & RRD history to extract 24h averaged metrics."""
        pve = proxmox_driver.get_client()
        try:
            raw_vms = pve.cluster.resources.get(type="vm")
            telemetry_list = []
            for v in raw_vms:
                vmid = int(v.get("vmid", 0))
                # Skip templates
                if v.get("template") == 1 or str(vmid).startswith("90") or str(vmid).startswith("92"):
                    continue

                name = v.get("name", f"vm-{vmid}")
                node = v.get("node", settings.PROXMOX_DEFAULT_NODE)
                status = v.get("status", "stopped")
                vtype = v.get("type", "qemu")  # 'qemu' or 'lxc'
                max_mem = v.get("maxmem", 0) or 0
                max_disk = v.get("maxdisk", 0) or 0
                disk_used = v.get("disk", 0) or 0
                uptime_sec = v.get("uptime", 0) or 0

                if status == "running":
                    # Fetch 24-Hour RRD time-averaged CPU and Memory
                    cpu_str, mem_str = self._get_vm_rrd_stats(pve, node, vmid, vtype, max_mem)

                    # Fallback to live snapshot if RRD empty
                    if not cpu_str:
                        cpu_raw = v.get("cpu", 0) or 0
                        cpu_str = f"{round(cpu_raw * 100, 1):.1f}%"
                    if not mem_str and max_mem > 0:
                        mem_used = v.get("mem", 0) or 0
                        mem_pct = round((mem_used / max_mem) * 100, 1)
                        mem_str = f"{self._format_bytes(mem_used)} / {self._format_bytes(max_mem)} ({mem_pct}%)"

                    uptime_str = self._format_uptime(uptime_sec)
                else:
                    cpu_str = "0.0% (Stopped)"
                    mem_str = f"0 GB / {self._format_bytes(max_mem)} (0%)" if max_mem > 0 else "N/A"
                    uptime_str = "Offline"

                # Disk Usage
                if max_disk > 0 and disk_used > 0:
                    disk_pct = round((disk_used / max_disk) * 100, 1)
                    disk_str = f"{self._format_bytes(disk_used)} / {self._format_bytes(max_disk)} ({disk_pct}%)"
                elif max_disk > 0:
                    disk_str = f"Cap: {self._format_bytes(max_disk)}"
                else:
                    disk_str = "N/A"

                guest_agent_str = self._get_guest_agent_status(pve, node, vmid, vtype, status)

                telemetry_list.append({
                    "vmid": vmid,
                    "name": name,
                    "node": node,
                    "type": vtype,
                    "status": status,  # 'running' or 'stopped'
                    "cpu_usage": cpu_str,
                    "memory_usage": mem_str,
                    "disk_usage": disk_str,
                    "uptime": uptime_str,
                    "guest_agent": guest_agent_str,
                })
            return telemetry_list
        except Exception as e:
            logger.exception("Error fetching Proxmox VM telemetry: %s", e)
            raise

    async def sync_metrics_to_netbox(self) -> Dict[str, Any]:
        """
        Fetches 24-hour averaged Proxmox telemetry and updates NetBox VirtualMachine records.
        """
        telemetry_items = await asyncio.to_thread(self.fetch_proxmox_telemetry)
        if not netbox_driver.is_configured():
            return {"status": "skipped", "reason": "NetBox not configured", "count": len(telemetry_items)}

        headers = {
            "Authorization": f"Token {netbox_driver.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        updated = []
        skipped = []
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            client = netbox_driver._get_client()
            # 1. Fetch all NetBox VMs
            resp = await client.get(
                f"{netbox_driver.base_url}/api/virtualization/virtual-machines/?limit=100",
                headers=headers,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch NetBox VMs: {resp.status_code} {resp.text}")

            netbox_vms = resp.json().get("results", [])

            # Index NetBox VMs by proxmox_vmid and by name
            nb_by_vmid = {}
            nb_by_name = {}
            for vm in netbox_vms:
                cfields = vm.get("custom_fields", {})
                vmid_val = cfields.get("proxmox_vmid")
                if vmid_val:
                    nb_by_vmid[str(vmid_val)] = vm
                vm_name = vm.get("name", "").strip().lower()
                if vm_name:
                    nb_by_name[vm_name] = vm

            # 2. Iterate through Proxmox telemetry and update matching NetBox VM
            for t in telemetry_items:
                vmid_str = str(t["vmid"])
                name_key = t["name"].strip().lower()

                matched_vm = nb_by_vmid.get(vmid_str) or nb_by_name.get(name_key)
                if not matched_vm:
                    skipped.append({
                        "vmid": t["vmid"],
                        "name": t["name"],
                        "reason": "No matching NetBox VM found",
                    })
                    continue

                nb_id = matched_vm["id"]
                curr_cf = matched_vm.get("custom_fields", {})
                curr_status = matched_vm.get("status", {}).get("value", "")

                # Determine new power state for NetBox (do not override if decommissioning or staging)
                new_status = curr_status
                if curr_status not in ("decommissioning", "staged", "pending"):
                    new_status = "active" if t["status"] == "running" else "offline"

                patch_cf = {
                    "cpu_usage": t["cpu_usage"],
                    "memory_usage": t["memory_usage"],
                    "disk_usage": t["disk_usage"],
                    "uptime": t["uptime"],
                    "guest_agent": t.get("guest_agent", "N/A"),
                    "proxmox_node": t["node"],
                    "metrics_updated": now_str,
                }
                if not curr_cf.get("proxmox_vmid"):
                    patch_cf["proxmox_vmid"] = t["vmid"]

                patch_payload = {
                    "custom_fields": patch_cf,
                }
                if new_status != curr_status:
                    patch_payload["status"] = new_status

                patch_resp = await client.patch(
                    f"{netbox_driver.base_url}/api/virtualization/virtual-machines/{nb_id}/",
                    headers=headers,
                    json=patch_payload,
                )

                if patch_resp.status_code == 200:
                    updated.append({
                        "netbox_id": nb_id,
                        "vmid": t["vmid"],
                        "name": t["name"],
                        "status": new_status,
                        "cpu": t["cpu_usage"],
                        "memory": t["memory_usage"],
                        "uptime": t["uptime"],
                        "node": t["node"],
                    })
                    logger.info("Updated 24h metrics for NetBox VM '%s' (ID: %d, VMID: %d): CPU %s, Mem %s",
                                t["name"], nb_id, t["vmid"], t["cpu_usage"], t["memory_usage"])
                else:
                    logger.warning("Failed to update NetBox VM %d metrics: %s", nb_id, patch_resp.text)

        except Exception as e:
            logger.exception("Error syncing Proxmox metrics to NetBox: %s", e)
            raise

        return {
            "status": "success",
            "timestamp": now_str,
            "total_proxmox_vms": len(telemetry_items),
            "updated_count": len(updated),
            "skipped_count": len(skipped),
            "updated": updated,
            "skipped": skipped,
        }


metrics_sync_driver = MetricsSyncDriver()
