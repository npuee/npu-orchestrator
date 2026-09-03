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
from app.workers.provisioning import (
    run_linux_provision_task,
    run_windows_provision_task,
    run_lxc_provision_task,
)
from app.workers.lifecycle import (
    run_power_sync_task,
    run_decommission_task,
    run_vm_sync_task,
    _active_decommissioning_vms,
    _active_power_sync_vms,
)

logger = logging.getLogger("orchestrator.workers.dispatcher")

# In-flight provisioning lock
_active_provisioning_vms = set()

async def process_netbox_webhook_event(job_id: str, payload: Dict[str, Any]):
    """
    Parses incoming NetBox webhook data and dispatches the corresponding provisioning, sync, or deprovisioning task.
    """
    event = payload.get("event")
    model = payload.get("model") or payload.get("data", {}).get("object_type")
    data = payload.get("data", {})

    await db.append_log(job_id, f"Received NetBox event '{event}' for model '{model}'")

    # In NetBox 4.x, verify if this is a Virtual Machine object
    is_vm = (
        model in ("virtualmachine", "virtualization.virtualmachine", "Virtual Machine")
        or "vcpus" in data
        or "cluster" in data
        or "disk" in data
        or "virtual_machine_type" in data
    )

    if not is_vm:
        await db.append_log(job_id, f"Ignoring non-VM model '{model}'")
        await db.update_job(job_id, status="completed")
        return

    # Extract VM details
    netbox_vm_id = data.get("id")
    hostname = data.get("name")
    if not hostname:
        raise ValueError("NetBox VM payload missing required 'name' field")

    custom_fields = data.get("custom_fields", {})
    existing_vmid = custom_fields.get("proxmox_vmid")

    # Check if this is a Decommission / Delete event
    status_data = data.get("status")
    status_val = status_data.get("value") if isinstance(status_data, dict) else str(status_data or "")

    # 1. Permanent Purge if VM object was completely deleted from NetBox
    if event == "deleted":
        if not existing_vmid:
            await db.append_log(job_id, f"Deleted VM '{hostname}' has no Proxmox VMID assigned. Skipping Proxmox deletion.")
            await db.update_job(job_id, status="completed")
            return

        await db.append_log(
            job_id,
            f"VM '{hostname}' deleted from NetBox. Triggering PERMANENT PURGE of VMID {existing_vmid} on Proxmox...",
        )
        await run_decommission_task(
            job_id=job_id,
            vmid=int(existing_vmid),
            hostname=hostname,
            node=data.get("custom_fields", {}).get("proxmox_node"),
            netbox_vm_id=None,
            permanent_purge=True,
        )
        return

    # 2. Safe Decommission (Quarantine, Disks Intact) if status is decommissioning / deprovisioning
    if status_val.lower() in ("decommissioning", "deprovisioning"):
        if not existing_vmid:
            await db.append_log(job_id, f"Decommissioning VM '{hostname}' has no Proxmox VMID. Skipping Proxmox quarantine.")
            await db.update_job(job_id, status="completed")
            return

        await db.append_log(
            job_id,
            f"VM '{hostname}' status is '{status_val}'. Triggering SAFE DECOMMISSION (Quarantine) for VMID {existing_vmid} on Proxmox...",
        )
        await run_decommission_task(
            job_id=job_id,
            vmid=int(existing_vmid),
            hostname=hostname,
            node=data.get("custom_fields", {}).get("proxmox_node"),
            netbox_vm_id=netbox_vm_id,
            permanent_purge=False,
        )
        return

    # 3. Existing VM: Power State Synchronization & Hardware/Name Synchronization
    if existing_vmid:
        node = custom_fields.get("proxmox_node")
        if not node and "device" in data and isinstance(data["device"], dict):
            dev_name = data["device"].get("name", "")
            if "proxmox" in dev_name.lower():
                node = dev_name.split(".")[0] if "." in dev_name else dev_name

        # Parse Start on Boot from NetBox
        onboot = None
        if "start_on_boot" in data:
            start_on_boot_data = data.get("start_on_boot")
            if isinstance(start_on_boot_data, dict):
                onboot = start_on_boot_data.get("value") != "off"
            elif isinstance(start_on_boot_data, str):
                onboot = start_on_boot_data.lower() not in ("off", "false", "0")

        # Extract Primary IP if available
        primary_ip = None
        if data.get("primary_ip4") and isinstance(data["primary_ip4"], dict):
            primary_ip = data["primary_ip4"].get("address", "").split("/")[0]
        elif data.get("primary_ip") and isinstance(data["primary_ip"], dict):
            primary_ip = data["primary_ip"].get("address", "").split("/")[0]
        elif custom_fields.get("requested_ip"):
            raw_ip = str(custom_fields["requested_ip"]).strip()
            primary_ip = raw_ip.split("/")[0] if raw_ip else None

        # A) Power State Synchronization
        if status_val.lower() in ("offline", "stopped"):
            await db.append_log(job_id, f"VM '{hostname}' status is 'offline'. Synchronizing power state -> STOP (disabling onboot)...")
            await run_power_sync_task(
                job_id=job_id,
                vmid=int(existing_vmid),
                hostname=hostname,
                target_state="stop",
                node=node,
                desired_onboot=False,
                netbox_vm_id=netbox_vm_id,
            )
        elif status_val.lower() in ("active", "running"):
            await db.append_log(job_id, f"VM '{hostname}' status is 'active'. Synchronizing power state -> START (enabling onboot)...")
            await run_power_sync_task(
                job_id=job_id,
                vmid=int(existing_vmid),
                hostname=hostname,
                target_state="start",
                node=node,
                desired_onboot=True,
                netbox_vm_id=netbox_vm_id,
            )

        # B) Hardware Specs & Name Synchronization (name, cores, RAM, disk, onboot)
        raw_disk = data.get("disk") or custom_fields.get("disk_size_gb")
        disk_size_gb = None
        if raw_disk:
            try:
                d_val = int(raw_disk)
                disk_size_gb = d_val // 1024 if d_val >= 1024 else d_val
            except (ValueError, TypeError):
                pass

        raw_cores = data.get("vcpus")
        cores = int(raw_cores) if raw_cores else None

        raw_memory = data.get("memory")
        memory_mb = None
        if raw_memory:
            try:
                m_val = int(raw_memory)
                memory_mb = m_val * 1024 if m_val < 128 else m_val
            except (ValueError, TypeError):
                pass

        # Check if Virtual Machine Type changed on an existing VM
        snapshots = payload.get("snapshots", {})
        pre_type = snapshots.get("prechange", {}).get("virtual_machine_type")
        post_type = snapshots.get("postchange", {}).get("virtual_machine_type") or data.get("virtual_machine_type")

        pre_type_id = pre_type.get("id") if isinstance(pre_type, dict) else pre_type
        post_type_id = post_type.get("id") if isinstance(post_type, dict) else post_type

        if post_type_id and pre_type_id != post_type_id:
            try:
                vm_type_obj = await netbox_driver.get_virtual_machine_type(int(post_type_id))
                if vm_type_obj:
                    type_name = vm_type_obj.get("name", "")
                    def_vcpus = vm_type_obj.get("default_vcpus")
                    def_mem = vm_type_obj.get("default_memory")
                    await db.append_log(
                        job_id,
                        f"Virtual Machine Type changed to '{type_name}' (Default: {def_vcpus} vCPUs, {def_mem} MB RAM). Scaling VM specs...",
                    )
                    if def_vcpus:
                        cores = int(def_vcpus)
                    if def_mem:
                        memory_mb = int(def_mem)
                    # Update NetBox VM fields so the numbers reflect the new type
                    if netbox_vm_id:
                        await netbox_driver.update_virtual_machine(
                            vm_id=netbox_vm_id,
                            vcpus=cores,
                            memory=memory_mb,
                        )
            except Exception as exc:
                logger.warning("Could not apply blueprint defaults for type %s: %s", post_type_id, exc)

        # If the VM is offline, onboot=0; if active, onboot=1
        effective_onboot = 0 if status_val.lower() in ("offline", "stopped") else 1

        await db.append_log(job_id, f"Checking hardware & name configuration for '{hostname}' (VMID: {existing_vmid})...")
        await run_vm_sync_task(
            job_id=job_id,
            vmid=int(existing_vmid),
            hostname=hostname,
            node=node,
            onboot=effective_onboot,
            cores=cores,
            memory_mb=memory_mb,
            disk_size_gb=disk_size_gb,
            netbox_vm_id=netbox_vm_id,
            ip_address=primary_ip,
        )
        return

    # Guard: do NOT provision VMs that are in offline, failed, or decommissioning status
    if status_val.lower() in ("offline", "failed", "decommissioning", "decommissioned"):
        await db.append_log(job_id, f"VM '{hostname}' is in status '{status_val}'. Skipping automatic provisioning.")
        await db.update_job(job_id, status="completed")
        return

    # Extract Primary IP / Requested IP
    primary_ip = None
    if custom_fields.get("requested_ip"):
        raw_ip = str(custom_fields["requested_ip"]).strip()
        primary_ip = raw_ip.split("/")[0] if raw_ip else None
    elif data.get("primary_ip4") and isinstance(data["primary_ip4"], dict):
        primary_ip = data["primary_ip4"].get("address", "").split("/")[0]
    elif data.get("primary_ip") and isinstance(data["primary_ip"], dict):
        primary_ip = data["primary_ip"].get("address", "").split("/")[0]

    # Extract Platform details
    platform_slug = ""
    platform_name = ""
    if data.get("platform") and isinstance(data["platform"], dict):
        platform_name = data["platform"].get("name", "")
        platform_slug = data["platform"].get("slug", "")

    custom_fields = data.get("custom_fields", {})
    template_id_override = int(custom_fields["template_id"]) if custom_fields.get("template_id") else None

    # Auto-assign defaults if omitted (configured in config.yml)
    defaults_cfg = app_config.defaults
    defaults_to_patch = {}
    if not data.get("tenant"):
        defaults_to_patch["tenant"] = defaults_cfg.get("tenant_id", 6)
    if not data.get("site"):
        defaults_to_patch["site"] = defaults_cfg.get("site_id", 2)
    if not data.get("cluster"):
        defaults_to_patch["cluster"] = defaults_cfg.get("cluster_id", 2)
    if not data.get("role"):
        defaults_to_patch["role"] = defaults_cfg.get("role_vm_id", 16)

    if defaults_to_patch and netbox_vm_id:
        try:
            await netbox_driver.update_virtual_machine(
                vm_id=netbox_vm_id,
                tenant=defaults_to_patch.get("tenant"),
                site=defaults_to_patch.get("site"),
                cluster=defaults_to_patch.get("cluster"),
                role=defaults_to_patch.get("role"),
            )
            await db.append_log(job_id, f"Auto-assigned homelab defaults to NetBox VM: {defaults_to_patch}")
        except Exception as exc:
            logger.warning("Could not auto-assign defaults to NetBox VM %d: %s", netbox_vm_id, exc)
    
    # 1. Resolve Target Proxmox Node
    node = custom_fields.get("proxmox_node")
    if not node and data.get("device") and isinstance(data["device"], dict):
        dev_name = data["device"].get("name", "")
        if "proxmox" in dev_name.lower():
            node = dev_name.split(".")[0] if "." in dev_name else dev_name

    vmid = custom_fields.get("proxmox_vmid")

    # 2. Correlate NetBox Platform to Proxmox template
    platform_desc = data.get("platform", {}).get("description", "") if isinstance(data.get("platform"), dict) else ""
    resolved_tpl_id, resolved_tpl_name, category = proxmox_driver.resolve_template_for_platform(
        platform_slug=platform_slug,
        platform_name=platform_name,
        platform_description=platform_desc,
        requested_template_id=template_id_override,
        node=node,
    )

    await db.append_log(
        job_id,
        f"Correlated Platform '{platform_name}' ({platform_slug}) -> Proxmox Template '{resolved_tpl_name}' (ID: {resolved_tpl_id}, Category: {category})"
    )

    # 3. Parse Hardware Specs
    raw_disk = data.get("disk") or custom_fields.get("disk_size_gb")
    disk_size_gb = None
    if raw_disk:
        try:
            d_val = int(raw_disk)
            disk_size_gb = d_val // 1024 if d_val >= 1024 else d_val
        except (ValueError, TypeError):
            pass

    raw_cores = data.get("vcpus")
    cores = int(raw_cores) if raw_cores else None

    raw_memory = data.get("memory")
    memory_mb = None
    if raw_memory:
        try:
            m_val = int(raw_memory)
            # If user entered in GB (e.g. 24), convert to MB (24576)
            memory_mb = m_val * 1024 if m_val < 128 else m_val
        except (ValueError, TypeError):
            pass

    # Resolve from Virtual Machine Type (User-managed hardware sizing in NetBox) if sliders were left empty
    if (not cores or not memory_mb or not disk_size_gb) and data.get("virtual_machine_type"):
        vm_t = data.get("virtual_machine_type")
        if isinstance(vm_t, dict):
            if not cores and vm_t.get("default_vcpus"):
                try: cores = int(vm_t["default_vcpus"])
                except (ValueError, TypeError): pass
            if not memory_mb and vm_t.get("default_memory"):
                try: memory_mb = int(vm_t["default_memory"])
                except (ValueError, TypeError): pass
            if not disk_size_gb and vm_t.get("default_disk"):
                try: disk_size_gb = int(vm_t["default_disk"])
                except (ValueError, TypeError): pass

    # Apply global fallbacks from config.yml if still unassigned
    fb = app_config.fallbacks
    cores = cores or fb.get("cores", 2)
    memory_mb = memory_mb or fb.get("memory_mb", 2048)
    disk_size_gb = disk_size_gb or fb.get("disk_gb", 20)

    # 4. Parse Start on Boot
    onboot = True
    start_on_boot_data = data.get("start_on_boot")
    if isinstance(start_on_boot_data, dict):
        if start_on_boot_data.get("value") == "off":
            onboot = False
    elif isinstance(start_on_boot_data, str) and start_on_boot_data.lower() in ("off", "false", "0"):
        onboot = False

    # Check Role to see if this is an LXC Container
    role_slug = ""
    role_name = ""
    if data.get("role") and isinstance(data["role"], dict):
        role_slug = data["role"].get("slug", "")
        role_name = data["role"].get("name", "")

    is_lxc = (
        role_slug == "lxc-container"
        or "lxc" in role_slug.lower()
        or "container" in role_slug.lower()
        or "lxc" in role_name.lower()
    )

    lock_key = f"vm_{netbox_vm_id or hostname}"
    if lock_key in _active_provisioning_vms:
        await db.append_log(job_id, f"Provisioning workflow already in progress for '{hostname}' (NetBox ID: {netbox_vm_id}). Skipping duplicate event.")
        await db.update_job(job_id, status="completed")
        return

    # Extract NetBox Config Context for cluster datastore/bridge, site networking and credentials
    cfg_ctx = data.get("config_context") or {}
    ctx_gateway = cfg_ctx.get("gateway")
    ctx_dns = cfg_ctx.get("dns_servers", [None])[0] if cfg_ctx.get("dns_servers") else None
    ctx_domain = cfg_ctx.get("domain") or cfg_ctx.get("dns_domain")
    ctx_ssh_keys = "\n".join(cfg_ctx.get("ssh_keys", [])) if cfg_ctx.get("ssh_keys") else None
    ctx_user = cfg_ctx.get("default_user")

    # Cluster-aware datastore and network bridge (Cluster Config Context -> config.yml fallback)
    ctx_storage = cfg_ctx.get("datastore") or cfg_ctx.get("storage") or defaults_cfg.get("storage", "zfs-storage")
    ctx_bridge = cfg_ctx.get("bridge") or defaults_cfg.get("bridge", "vmbr0")
    ctx_node = cfg_ctx.get("default_node")
    if not node and ctx_node:
        node = ctx_node

    if cfg_ctx:
        await db.append_log(
            job_id,
            f"Applied NetBox Config Context: datastore='{ctx_storage}', bridge='{ctx_bridge}', node='{node}', site='{cfg_ctx.get('site_name')}', gateway='{ctx_gateway}', dns='{ctx_dns}', user='{ctx_user}'",
        )

    _active_provisioning_vms.add(lock_key)
    try:
        if is_lxc:
            m_lxc_desc = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", platform_desc or "")
            lxc_volid = custom_fields.get("lxc_template") or (m_lxc_desc.group(1).strip() if m_lxc_desc else None)
            params = {
                "hostname": hostname,
                "template_volid": lxc_volid,
                "node": node,
                "vmid": int(vmid) if vmid else None,
                "ip_address": primary_ip,
                "gateway": ctx_gateway,
                "dns_servers": ctx_dns,
                "dns_domain": ctx_domain,
                "disk_size_gb": disk_size_gb or 20,
                "cores": cores or 2,
                "memory_mb": memory_mb or 2048,
                "onboot": onboot,
                "ssh_key": custom_fields.get("ssh_key") or ctx_ssh_keys,
                "storage": custom_fields.get("storage") or ctx_storage,
                "bridge": custom_fields.get("bridge") or ctx_bridge,
            }
            await run_lxc_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
        elif category == "windows":
            admin_password = custom_fields.get("admin_password") or app_config.templates.get("default_windows_password", "P@ssw0rdInitial!")
            params = {
                "hostname": hostname,
                "admin_password": admin_password,
                "template_id": resolved_tpl_id,
                "node": node,
                "vmid": int(vmid) if vmid else None,
                "ip_address": primary_ip,
                "gateway": ctx_gateway,
                "dns_server": ctx_dns,
                "dns_domain": ctx_domain,
                "disk_size_gb": disk_size_gb or 32,
                "cores": cores or 4,
                "memory_mb": memory_mb or 8192,
                "onboot": onboot,
                "storage": custom_fields.get("storage") or ctx_storage,
                "bridge": custom_fields.get("bridge") or ctx_bridge,
            }
            await run_windows_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
        else:
            params = {
                "hostname": hostname,
                "template_id": resolved_tpl_id,
                "node": node,
                "vmid": int(vmid) if vmid else None,
                "ip_address": primary_ip,
                "gateway": ctx_gateway,
                "dns_server": ctx_dns,
                "dns_domain": ctx_domain,
                "disk_size_gb": disk_size_gb or 20,
                "cores": cores,
                "memory_mb": memory_mb,
                "onboot": onboot,
                "ssh_key": custom_fields.get("ssh_key") or ctx_ssh_keys,
                "ci_user": ctx_user or "root",
                "storage": custom_fields.get("storage") or ctx_storage,
                "bridge": custom_fields.get("bridge") or ctx_bridge,
            }
            await run_linux_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
    finally:
        _active_provisioning_vms.discard(lock_key)

