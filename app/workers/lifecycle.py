import asyncio
import logging
from typing import Dict, Any, Optional

from app.core.config import settings
from app.core.app_config import app_config
from app.storage.db import db
from app.drivers.proxmox import proxmox_driver
from app.drivers.netbox import netbox_driver
from app.drivers.notifier import notifier

logger = logging.getLogger("orchestrator.workers.lifecycle")

# Concurrency locks for in-flight lifecycle operations
_active_decommissioning_vms = set()
_active_power_sync_vms = set()

async def run_power_sync_task(
    job_id: str,
    vmid: int,
    hostname: str,
    target_state: str,
    node: Optional[str] = None,
    desired_onboot: Optional[bool] = None,
    netbox_vm_id: Optional[int] = None,
):
    if vmid in _active_power_sync_vms:
        logger.info("[Job %s] Power sync for VMID %d already in-flight. Skipping duplicate.", job_id, vmid)
        return

    _active_power_sync_vms.add(vmid)
    loop = asyncio.get_running_loop()

    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    try:
        sync_log_callback(f"Synchronizing power state for VM '{hostname}' (VMID: {vmid}) -> {target_state.upper()}...")
        result = await loop.run_in_executor(
            None,
            lambda: proxmox_driver.set_vm_power_state(
                vmid=vmid,
                target_state=target_state,
                node=node,
                desired_onboot=desired_onboot,
                log_callback=sync_log_callback,
            ),
        )
        action_performed = result.get("action")
        target_nb_onboot = "off" if target_state == "stop" else ("on" if desired_onboot is not False else "off")

        if action_performed in ("started", "stopped"):
            if netbox_vm_id:
                await netbox_driver.update_virtual_machine(
                    vm_id=netbox_vm_id,
                    start_on_boot=target_nb_onboot,
                )
                sync_log_callback(f"Synchronized NetBox 'Start on boot' -> {target_nb_onboot.upper()}.")
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=f"VM power state synchronized to '{result.get('new_status')}'. Start on boot set to '{target_nb_onboot}'. (Job ID: {job_id})",
                )
            action_key = "power_start" if action_performed == "started" else "power_stop"
            await notifier.notify_job_success(
                job_id,
                action_key,
                {"vmid": vmid, "hostname": hostname, "node": result.get("node")},
            )
            sync_log_callback(f"Power state successfully changed to '{result.get('new_status')}'.")
        else:
            sync_log_callback(f"VM '{hostname}' was already {result.get('new_status')}. No state transition needed.")

    except Exception as e:
        err_msg = str(e)
        logger.exception("Power sync job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await notifier.notify_job_failure(job_id, f"power_{target_state}", err_msg, metadata={"vmid": vmid, "hostname": hostname})
    finally:
        _active_power_sync_vms.discard(vmid)




async def run_decommission_task(
    job_id: str,
    vmid: int,
    hostname: str,
    node: Optional[str] = None,
    netbox_vm_id: Optional[int] = None,
    permanent_purge: bool = False,
):
    if vmid in _active_decommissioning_vms:
        logger.warning("[Job %s] Decommission task for VMID %d is already in-flight. Skipping duplicate.", job_id, vmid)
        await db.append_log(job_id, f"Decommission task for VMID {vmid} is already running in background. Skipping duplicate.")
        await db.update_job(job_id, status="completed", vmid=vmid, hostname=hostname)
        return

    _active_decommissioning_vms.add(vmid)
    loop = asyncio.get_running_loop()

    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    try:
        if permanent_purge:
            sync_log_callback(f"Starting PERMANENT PURGE workflow for VM '{hostname}' (VMID: {vmid})")
            result = await loop.run_in_executor(
                None,
                lambda: proxmox_driver.delete_vm(
                    vmid=vmid,
                    node=node,
                    purge=True,
                    log_callback=sync_log_callback,
                ),
            )
        else:
            sync_log_callback(f"Starting SAFE DECOMMISSION (Quarantine) for VM '{hostname}' (VMID: {vmid})")
            result = await loop.run_in_executor(
                None,
                lambda: proxmox_driver.quarantine_vm(
                    vmid=vmid,
                    node=node,
                    log_callback=sync_log_callback,
                ),
            )

        # 2. Delete DNS records (A & PTR) from NetBox DNS
        dns_zone = app_config.dns.get("default_zone", "homelab.local")
        sync_log_callback(f"Deleting DNS records for '{hostname}.{dns_zone}' from NetBox DNS...")
        await netbox_driver.delete_dns_record(hostname=hostname, zone_name=dns_zone)

        # 3. Clean up NetBox interfaces and IPAM
        if netbox_vm_id:
            sync_log_callback(f"Cleaning up NetBox interfaces & IPAM for VM #{netbox_vm_id}...")
            await netbox_driver.cleanup_vm_ip_and_interfaces(vm_id=netbox_vm_id)

            if permanent_purge:
                await netbox_driver.update_virtual_machine(
                    vm_id=netbox_vm_id,
                    status="offline",
                    start_on_boot="off",
                    custom_fields={"proxmox_vmid": None},
                    comments=f"Permanently purged from Proxmox (Job ID: {job_id})",
                )
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=f"VM was permanently purged from Proxmox node '{result.get('node')}'. DNS and IP records removed. (Job ID: {job_id})",
                )
            else:
                await netbox_driver.update_virtual_machine(
                    vm_id=netbox_vm_id,
                    status="decommissioning",
                    start_on_boot="off",
                    comments=f"Safely decommissioned & quarantined on Proxmox (Disks intact, VMID: {vmid}, Job ID: {job_id})",
                )
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=f"VM was safely decommissioned: powered off, start-on-boot disabled, network isolated, and tagged 'decommissioned' on Proxmox node '{result.get('node')}'. Storage and disks remain intact for recovery. (Job ID: {job_id})",
                )

        await db.update_job(
            job_id,
            status="completed",
            vmid=vmid,
            hostname=hostname,
        )
        sync_log_callback(f"Decommission workflow for '{hostname}' completed successfully!")

        # 4. Notify via Signal
        action_name = "decommission_vm" if permanent_purge else "quarantine_vm"
        await notifier.notify_job_success(
            job_id,
            action_name,
            {"vmid": vmid, "hostname": hostname, "node": result.get("node")},
        )

    except Exception as e:
        err_msg = str(e)
        logger.exception("Decommission job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await db.update_job(job_id, status="failed", error=err_msg)
        if netbox_vm_id:
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Decommission job {job_id} failed: {err_msg}",
            )
        await notifier.notify_job_failure(job_id, "decommission_vm", err_msg, metadata={"vmid": vmid, "hostname": hostname})
    finally:
        _active_decommissioning_vms.discard(vmid)




async def run_vm_sync_task(
    job_id: str,
    vmid: int,
    hostname: str,
    node: Optional[str] = None,
    onboot: Optional[bool] = None,
    cores: Optional[int] = None,
    memory_mb: Optional[int] = None,
    disk_size_gb: Optional[int] = None,
    netbox_vm_id: Optional[int] = None,
    ip_address: Optional[str] = None,
):
    loop = asyncio.get_running_loop()

    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    try:
        sync_log_callback(f"Checking configuration / name updates for VM '{hostname}' (VMID: {vmid}) on Proxmox...")

        result = await loop.run_in_executor(
            None,
            lambda: proxmox_driver.update_vm_config(
                vmid=vmid,
                node=node,
                name=hostname,
                onboot=onboot,
                cores=cores,
                memory_mb=memory_mb,
                disk_size_gb=disk_size_gb,
                log_callback=sync_log_callback,
            ),
        )

        has_changes = result.get("changed", False)
        diff_summary = result.get("diff_summary", [])
        name_changed = result.get("name_changed", False)
        old_name = result.get("old_name")

        if name_changed and old_name:
            dns_zone = app_config.dns.get("default_zone", "homelab.local")
            sync_log_callback(f"VM rename detected: '{old_name}' -> '{hostname}'. Updating NetBox DNS records in zone '{dns_zone}'...")
            await netbox_driver.rename_dns_record(
                old_hostname=old_name,
                new_hostname=hostname,
                ip_address=ip_address,
                zone_name=dns_zone,
            )
            sync_log_callback(f"Updated NetBox DNS A and PTR records for '{hostname}.{dns_zone}'.")
            await notifier.notify_job_success(
                job_id,
                "vm_rename",
                {"vmid": vmid, "old_name": old_name, "hostname": hostname, "node": result.get("node")},
            )

        if has_changes:
            summary_str = ", ".join(diff_summary)
            sync_log_callback(f"Successfully applied VM {vmid} configuration changes: {summary_str}")
            if netbox_vm_id:
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=f"Configuration / Name changed on Proxmox VMID {vmid}: {summary_str} (Job ID: {job_id})",
                )
        else:
            sync_log_callback(f"VM {vmid} configuration on Proxmox already matches NetBox specs. No changes needed.")

        await db.update_job(
            job_id,
            status="completed",
            vmid=vmid,
            hostname=hostname,
        )

    except Exception as e:
        err_msg = str(e)
        logger.exception("VM sync job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await db.update_job(job_id, status="failed", error=err_msg)



