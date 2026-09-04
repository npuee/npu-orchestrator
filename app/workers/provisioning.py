import asyncio
import logging
import re
from typing import Dict, Any, Optional

from app.core.config import settings
from app.core.app_config import app_config
from app.storage.db import db
from app.drivers.proxmox import proxmox_driver
from app.drivers.netbox import netbox_driver
from app.drivers.notifier import notifier
from app.core.modules import module_manager

logger = logging.getLogger("orchestrator.workers.provisioning")

async def run_linux_provision_task(
    job_id: str,
    params: Dict[str, Any],
    netbox_vm_id: Optional[int] = None,
):
    loop = asyncio.get_running_loop()
    
    # Thread-safe callback to stream step logs to SQLite from executor thread
    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    def netbox_progress(msg: str):
        if netbox_vm_id:
            try:
                asyncio.run_coroutine_threadsafe(
                    netbox_driver.add_journal_entry(
                        assigned_object_type="virtualization.virtualmachine",
                        assigned_object_id=netbox_vm_id,
                        comment=f"{msg} (Job ID: {job_id})",
                    ),
                    loop,
                )
            except Exception as e:
                logger.warning("Could not append NetBox progress journal: %s", e)

    try:
        sync_log_callback(f"Starting Linux VM clone workflow for '{params.get('hostname')}'")

        # Run the blocking Proxmox API calls in a thread pool to avoid blocking async event loop
        result = await loop.run_in_executor(
            None,
            lambda: proxmox_driver.clone_linux_vm(
                hostname=params["hostname"],
                template_id=params.get("template_id"),
                node=params.get("node"),
                vmid=params.get("vmid"),
                ip_address=params.get("ip_address"),
                gateway=params.get("gateway"),
                dns_server=params.get("dns_server"),
                dns_domain=params.get("dns_domain"),
                disk_size_gb=params.get("disk_size_gb", 20),
                cores=params.get("cores"),
                memory_mb=params.get("memory_mb"),
                onboot=params.get("onboot", True),
                ssh_key=params.get("ssh_key"),
                ci_user=params.get("ci_user", "root"),
                storage=params.get("storage"),
                bridge=params.get("bridge"),
                start_on_create=params.get("start_on_create", True),
                log_callback=sync_log_callback,
                progress_callback=netbox_progress,
            ),
        )

        vmid = result["vmid"]
        hostname = result["hostname"]
        ip_addr = result["ip_address"]

        await db.append_log(job_id, f"Provisioning completed successfully! VMID: {vmid}")
        await db.update_job(
            job_id,
            status="completed",
            vmid=vmid,
            hostname=hostname,
            ip_address=ip_addr,
        )

        dns_zone = params.get("dns_domain") or app_config.dns.get("default_zone", "homelab.local")

        # Update NetBox if triggered via NetBox
        if netbox_vm_id:
            await db.append_log(job_id, f"Syncing status, interface, IP & DNS ({hostname}.{dns_zone}) back to NetBox VM #{netbox_vm_id}...")
            await netbox_driver.ensure_vm_interface_and_ip(
                vm_id=netbox_vm_id,
                hostname=hostname,
                ip_address=ip_addr,
                interface_name="eth0",
                domain=dns_zone,
            )
            if module_manager.is_enabled("dns"):
                await db.append_log(job_id, f"Registering DNS A & PTR records for '{hostname}.{dns_zone}' -> {ip_addr} in NetBox DNS...")
                await netbox_driver.create_or_update_dns_record(
                    hostname=hostname,
                    ip_address=ip_addr,
                    zone_name=dns_zone,
                )
            else:
                await db.append_log(job_id, "DNS module disabled or unconfigured; skipping DNS record registration.")
            await netbox_driver.update_virtual_machine(
                vm_id=netbox_vm_id,
                status="active",
                custom_fields={"proxmox_vmid": vmid},
                comments=f"Provisioned via NPU Orchestrator. Proxmox VMID: {vmid}, IP: {ip_addr}",
            )
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Successfully provisioned on Proxmox node '{result['node']}' with VMID {vmid}, IP: {ip_addr} (DNS: {hostname}.{dns_zone}) (Job ID: {job_id})",
            )

        # Notify via Signal
        await notifier.notify_job_success(job_id, "clone_linux", result)

    except Exception as e:
        err_msg = str(e)
        logger.exception("Job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await db.update_job(job_id, status="failed", error=err_msg)

        if netbox_vm_id:
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Orchestrator Job {job_id} failed during provisioning: {err_msg}",
            )

        await notifier.notify_job_failure(job_id, "clone_linux", err_msg, metadata=params)




async def run_windows_provision_task(
    job_id: str,
    params: Dict[str, Any],
    netbox_vm_id: Optional[int] = None,
):
    loop = asyncio.get_running_loop()

    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    def netbox_progress(msg: str):
        if netbox_vm_id:
            try:
                asyncio.run_coroutine_threadsafe(
                    netbox_driver.add_journal_entry(
                        assigned_object_type="virtualization.virtualmachine",
                        assigned_object_id=netbox_vm_id,
                        comment=f"{msg} (Job ID: {job_id})",
                    ),
                    loop,
                )
            except Exception as e:
                logger.warning("Could not append NetBox progress journal: %s", e)

    try:
        sync_log_callback(f"Starting Windows VM clone workflow for '{params.get('hostname')}'")

        result = await loop.run_in_executor(
            None,
            lambda: proxmox_driver.clone_windows_vm(
                hostname=params["hostname"],
                admin_password=params["admin_password"],
                template_id=params.get("template_id"),
                node=params.get("node"),
                vmid=params.get("vmid"),
                ip_address=params.get("ip_address"),
                gateway=params.get("gateway"),
                dns_server=params.get("dns_server"),
                dns_domain=params.get("dns_domain"),
                disk_size_gb=params.get("disk_size_gb", 32),
                cores=params.get("cores", 4),
                memory_mb=params.get("memory_mb", 8192),
                balloon_mb=params.get("balloon_mb", 512),
                storage=params.get("storage"),
                bridge=params.get("bridge"),
                start_on_create=params.get("start_on_create", True),
                log_callback=sync_log_callback,
                progress_callback=netbox_progress,
            ),
        )

        vmid = result["vmid"]
        hostname = result["hostname"]
        ip_addr = result["ip_address"]

        await db.append_log(job_id, f"Windows VM provisioning completed successfully! VMID: {vmid}")
        await db.update_job(
            job_id,
            status="completed",
            vmid=vmid,
            hostname=hostname,
            ip_address=ip_addr,
        )

        dns_zone = params.get("dns_domain") or app_config.dns.get("default_zone", "homelab.local")

        if netbox_vm_id:
            await db.append_log(job_id, f"Syncing status, interface, IP & DNS ({hostname}.{dns_zone}) back to NetBox VM #{netbox_vm_id}...")
            await netbox_driver.ensure_vm_interface_and_ip(
                vm_id=netbox_vm_id,
                hostname=hostname,
                ip_address=ip_addr,
                interface_name="eth0",
                domain=dns_zone,
            )
            if module_manager.is_enabled("dns"):
                await db.append_log(job_id, f"Registering DNS A & PTR records for '{hostname}.{dns_zone}' -> {ip_addr} in NetBox DNS...")
                await netbox_driver.create_or_update_dns_record(
                    hostname=hostname,
                    ip_address=ip_addr,
                    zone_name=dns_zone,
                )
            else:
                await db.append_log(job_id, "DNS module disabled or unconfigured; skipping DNS record registration.")
            await netbox_driver.update_virtual_machine(
                vm_id=netbox_vm_id,
                status="active",
                custom_fields={"proxmox_vmid": vmid},
                comments=f"Provisioned via NPU Orchestrator. Proxmox VMID: {vmid}, IP: {ip_addr}",
            )
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Successfully provisioned Windows VM on Proxmox node '{result['node']}' with VMID {vmid}, IP: {ip_addr} (DNS: {hostname}.{dns_zone}) (Job ID: {job_id})",
            )

        await notifier.notify_job_success(job_id, "clone_windows", result)

    except Exception as e:
        err_msg = str(e)
        logger.exception("Job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await db.update_job(job_id, status="failed", error=err_msg)

        if netbox_vm_id:
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Orchestrator Job {job_id} failed: {err_msg}",
            )

        await notifier.notify_job_failure(job_id, "clone_windows", err_msg, metadata=params)




async def run_lxc_provision_task(
    job_id: str,
    params: Dict[str, Any],
    netbox_vm_id: Optional[int] = None,
):
    loop = asyncio.get_running_loop()

    def sync_log_callback(msg: str):
        logger.info("[Job %s] %s", job_id, msg)
        try:
            asyncio.run_coroutine_threadsafe(db.append_log(job_id, msg), loop)
        except Exception as e:
            logger.warning("Could not append log to DB: %s", e)

    def netbox_progress(msg: str):
        if netbox_vm_id:
            try:
                asyncio.run_coroutine_threadsafe(
                    netbox_driver.add_journal_entry(
                        assigned_object_type="virtualization.virtualmachine",
                        assigned_object_id=netbox_vm_id,
                        comment=f"{msg} (Job ID: {job_id})",
                    ),
                    loop,
                )
            except Exception as e:
                logger.warning("Could not append NetBox progress journal: %s", e)

    try:
        sync_log_callback(f"Starting LXC Container creation workflow for '{params.get('hostname')}'")

        result = await loop.run_in_executor(
            None,
            lambda: proxmox_driver.create_lxc_container(
                hostname=params["hostname"],
                template_volid=params.get("template_volid"),
                node=params.get("node"),
                vmid=params.get("vmid"),
                ip_address=params.get("ip_address"),
                gateway=params.get("gateway"),
                dns_server=params.get("dns_server"),
                dns_domain=params.get("dns_domain"),
                disk_size_gb=params.get("disk_size_gb", 20),
                cores=params.get("cores", 2),
                memory_mb=params.get("memory_mb", 2048),
                swap_mb=params.get("swap_mb", 512),
                onboot=params.get("onboot", True),
                ssh_key=params.get("ssh_key"),
                storage=params.get("storage"),
                bridge=params.get("bridge"),
                start_on_create=params.get("start_on_create", True),
                log_callback=sync_log_callback,
                progress_callback=netbox_progress,
            ),
        )

        vmid = result["vmid"]
        hostname = result["hostname"]
        ip_addr = result["ip_address"]

        await db.update_job(
            job_id,
            status="completed",
            vmid=vmid,
            hostname=hostname,
            ip_address=ip_addr,
        )

        dns_zone = params.get("dns_domain") or app_config.dns.get("default_zone", "homelab.local")

        if netbox_vm_id:
            await db.append_log(job_id, f"Syncing status, interface, IP & DNS ({hostname}.{dns_zone}) back to NetBox VM #{netbox_vm_id}...")
            await netbox_driver.ensure_vm_interface_and_ip(
                vm_id=netbox_vm_id,
                hostname=hostname,
                ip_address=ip_addr,
                interface_name="eth0",
                domain=dns_zone,
            )
            if module_manager.is_enabled("dns"):
                await db.append_log(job_id, f"Registering DNS A & PTR records for '{hostname}.{dns_zone}' -> {ip_addr} in NetBox DNS...")
                await netbox_driver.create_or_update_dns_record(
                    hostname=hostname,
                    ip_address=ip_addr,
                    zone_name=dns_zone,
                )
            else:
                await db.append_log(job_id, "DNS module disabled or unconfigured; skipping DNS record registration.")
            await netbox_driver.update_virtual_machine(
                vm_id=netbox_vm_id,
                status="active",
                custom_fields={"proxmox_vmid": vmid},
                comments=f"Provisioned LXC Container. Proxmox CT ID: {vmid}, IP: {ip_addr}",
            )
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"Successfully provisioned LXC Container on Proxmox node '{result['node']}' with CT ID {vmid}, IP: {ip_addr} (DNS: {hostname}.{dns_zone}) (Job ID: {job_id})",
            )

        await notifier.notify_job_success(job_id, "create_lxc", result)

    except Exception as e:
        err_msg = str(e)
        logger.exception("Job %s failed: %s", job_id, err_msg)
        await db.append_log(job_id, f"ERROR: {err_msg}")
        await db.update_job(job_id, status="failed", error=err_msg)

        if netbox_vm_id:
            await netbox_driver.add_journal_entry(
                assigned_object_type="virtualization.virtualmachine",
                assigned_object_id=netbox_vm_id,
                comment=f"LXC Orchestrator Job {job_id} failed: {err_msg}",
            )

        await notifier.notify_job_failure(job_id, "create_lxc", err_msg, metadata=params)



