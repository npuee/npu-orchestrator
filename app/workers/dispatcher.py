import asyncio
import logging
import re
from typing import Dict, Any, Optional, Tuple, List

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
    is_recently_decommissioned,
)

logger = logging.getLogger("orchestrator.workers.dispatcher")

# In-flight provisioning lock
_active_provisioning_vms = set()

TELEMETRY_CUSTOM_FIELDS = {
    "cpu_usage",
    "memory_usage",
    "disk_usage",
    "uptime",
    "guest_agent",
    "metrics_updated",
    "kuma_monitor_id",
}

IGNORED_TOP_LEVEL_KEYS = {
    "last_updated",
    "created",
}

def _normalize_status(val: Any) -> Optional[str]:
    if isinstance(val, dict):
        s = val.get("value") or val.get("label")
        return str(s).strip().lower() if s is not None else None
    elif val is not None:
        return str(val).strip().lower()
    return None

def _normalize_ip(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    ip_data = obj.get("primary_ip4") or obj.get("primary_ip")
    if isinstance(ip_data, dict):
        addr = ip_data.get("address", "")
        return addr.split("/")[0].strip() if addr else None
    elif isinstance(ip_data, str):
        return ip_data.split("/")[0].strip() if ip_data else None
    return None

def _normalize_vm_type_id(val: Any) -> Optional[int]:
    if isinstance(val, dict):
        v = val.get("id")
        return int(v) if v is not None else None
    elif val is not None:
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
    return None

def _normalize_start_on_boot(val: Any) -> Optional[bool]:
    if isinstance(val, dict):
        v = val.get("value")
        return v != "off" if v is not None else None
    elif isinstance(val, str):
        return val.lower() not in ("off", "false", "0")
    elif isinstance(val, bool):
        return val
    return None

def extract_vm_deltas(pre: Optional[Dict[str, Any]], post: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Computes fine-grained differences between NetBox prechange and postchange snapshots.
    Identifies if an update is purely a background telemetry/metric echo, or if it
    contains actionable power state transitions, hardware slider changes, name changes, or IP changes.
    """
    if not isinstance(pre, dict) or not isinstance(post, dict):
        return {
            "has_snapshots": False,
            "status_changed": True,
            "hardware_changed": True,
            "ip_changed": False,
            "is_telemetry_only": False,
            "old_status": None,
            "new_status": None,
            "old_ip": None,
            "new_ip": None,
            "changed_fields": [],
        }

    # 1. Power State Delta
    old_status = _normalize_status(pre.get("status"))
    new_status = _normalize_status(post.get("status"))
    status_changed = bool(old_status and new_status and old_status != new_status)

    # 2. IP Delta
    old_ip = _normalize_ip(pre) or str((pre.get("custom_fields") or {}).get("requested_ip") or "").split("/")[0].strip() or None
    new_ip = _normalize_ip(post) or str((post.get("custom_fields") or {}).get("requested_ip") or "").split("/")[0].strip() or None
    ip_changed = bool(new_ip and old_ip != new_ip)

    # 3. Hardware Specs & Identity Deltas
    pre_cf = pre.get("custom_fields") or {}
    post_cf = post.get("custom_fields") or {}
    pre_vmid = pre_cf.get("proxmox_vmid")
    post_vmid = post_cf.get("proxmox_vmid")
    is_provisioning_writeback = bool(not pre_vmid and post_vmid)

    name_changed = pre.get("name") != post.get("name")
    vcpus_changed = pre.get("vcpus") != post.get("vcpus")
    memory_changed = pre.get("memory") != post.get("memory")
    disk_changed = (
        pre.get("disk") != post.get("disk")
        or pre_cf.get("disk_size_gb") != post_cf.get("disk_size_gb")
    )
    type_changed = _normalize_vm_type_id(pre.get("virtual_machine_type")) != _normalize_vm_type_id(post.get("virtual_machine_type"))
    onboot_changed = _normalize_start_on_boot(pre.get("start_on_boot")) != _normalize_start_on_boot(post.get("start_on_boot"))
    node_changed = bool(pre_vmid) and (pre_cf.get("proxmox_node") != post_cf.get("proxmox_node"))

    hardware_changed = any([
        name_changed,
        vcpus_changed,
        memory_changed,
        disk_changed,
        type_changed,
        onboot_changed,
        node_changed,
    ])

    # 4. Telemetry-Only Inspection
    changed_fields = []
    all_keys = set(pre.keys()).union(set(post.keys()))
    non_telemetry_changes = False

    if status_changed:
        non_telemetry_changes = True
        changed_fields.append(f"status: {old_status} -> {new_status}")
    if ip_changed:
        non_telemetry_changes = True
        changed_fields.append(f"primary_ip: {old_ip} -> {new_ip}")
    if hardware_changed:
        non_telemetry_changes = True
        if name_changed: changed_fields.append(f"name: {pre.get('name')} -> {post.get('name')}")
        if vcpus_changed: changed_fields.append(f"vcpus: {pre.get('vcpus')} -> {post.get('vcpus')}")
        if memory_changed: changed_fields.append(f"memory: {pre.get('memory')} -> {post.get('memory')}")
        if disk_changed: changed_fields.append("disk")
        if type_changed: changed_fields.append("virtual_machine_type")
        if onboot_changed: changed_fields.append("start_on_boot")
        if node_changed: changed_fields.append("proxmox_node")

    cf_keys = set(pre_cf.keys()).union(set(post_cf.keys()))
    cf_telemetry_changes = []
    for k in cf_keys:
        if pre_cf.get(k) != post_cf.get(k):
            if k in TELEMETRY_CUSTOM_FIELDS:
                cf_telemetry_changes.append(k)
            elif k not in ("proxmox_node", "disk_size_gb", "requested_ip"):
                non_telemetry_changes = True
                changed_fields.append(f"custom_fields.{k}")

    for k in all_keys:
        if k in ("custom_fields", "status", "primary_ip4", "primary_ip", "name", "vcpus", "memory", "disk", "virtual_machine_type", "start_on_boot"):
            continue
        if k in IGNORED_TOP_LEVEL_KEYS:
            continue
        if pre.get(k) != post.get(k):
            non_telemetry_changes = True
            changed_fields.append(k)

    is_telemetry_only = bool(cf_telemetry_changes and not non_telemetry_changes)

    return {
        "has_snapshots": True,
        "status_changed": status_changed,
        "hardware_changed": hardware_changed,
        "ip_changed": ip_changed,
        "is_telemetry_only": is_telemetry_only,
        "is_provisioning_writeback": is_provisioning_writeback,
        "old_status": old_status,
        "new_status": new_status,
        "old_ip": old_ip,
        "new_ip": new_ip,
        "changed_fields": changed_fields or cf_telemetry_changes,
    }


async def validate_vm_creation_blueprint(
    job_id: str,
    data: Dict[str, Any],
    cfg_ctx: Dict[str, Any],
    custom_fields: Dict[str, Any],
    primary_ip: Optional[str] = None,
    netbox_vm_id: Optional[int] = None,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    """
    Declarative pre-flight validation for VM and Container provisioning.
    Validates required parameters across NetBox object fields, custom fields, and NetBox Config Context.
    If required attributes are missing or invalid, posts a formatted failure entry to the NetBox Journal
    and marks the job failed without executing commands on Proxmox.
    """
    hostname = data.get("name")
    missing_errors: List[str] = []

    # 1. Target Proxmox Node
    node = custom_fields.get("proxmox_node")
    if not node and data.get("device") and isinstance(data["device"], dict):
        dev_name = data["device"].get("name", "")
        if "proxmox" in dev_name.lower():
            node = dev_name.split(".")[0] if "." in dev_name else dev_name
    if not node and cfg_ctx.get("default_node"):
        node = cfg_ctx.get("default_node")

    if not node:
        missing_errors.append(
            "- **Target Node**: No Proxmox node specified (expected `default_node` in NetBox Config Context or custom field `proxmox_node`)."
        )

    # 2. Workload Classification (LXC vs QEMU)
    platform_name = ""
    platform_slug = ""
    platform_desc = ""
    if data.get("platform") and isinstance(data["platform"], dict):
        platform_name = data["platform"].get("name", "")
        platform_slug = data["platform"].get("slug", "")
        platform_desc = data["platform"].get("description", "")

    role_slug = ""
    role_name = ""
    if data.get("role") and isinstance(data["role"], dict):
        role_slug = data["role"].get("slug", "")
        role_name = data["role"].get("name", "")

    vm_t = data.get("virtual_machine_type") or {}
    vm_type_slug = vm_t.get("slug", "") if isinstance(vm_t, dict) else ""
    vm_type_name = vm_t.get("name", "") if isinstance(vm_t, dict) else ""

    is_lxc = (
        role_slug == "lxc-container"
        or "lxc" in role_slug.lower()
        or "container" in role_slug.lower()
        or "lxc" in role_name.lower()
        or platform_slug.startswith("pve-lxc-")
        or "[Proxmox LXC Template:" in platform_desc
        or "lxc" in platform_name.lower()
        or "lxc" in vm_type_slug.lower()
        or "lxc" in vm_type_name.lower()
    )

    # 3. OS Template Resolution
    template_volid = None
    template_id = None
    template_name = ""
    category = "linux"

    if is_lxc:
        m_lxc_desc = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", platform_desc or "")
        template_volid = custom_fields.get("lxc_template") or (m_lxc_desc.group(1).strip() if m_lxc_desc else None)
        if not template_volid and (platform_slug or platform_name):
            _, tpl_name, cat = proxmox_driver.resolve_template_for_platform(
                platform_slug=platform_slug,
                platform_name=platform_name,
                platform_description=platform_desc,
                requested_template_id=None,
                node=node,
            )
            if cat == "lxc" and tpl_name:
                template_volid = tpl_name
        if not template_volid and node:
            template_volid = proxmox_driver.find_lxc_template(node=node)
        if not template_volid:
            missing_errors.append(
                f"- **OS Template (LXC)**: No valid Proxmox LXC template found for Platform '{platform_name or 'None'}'. "
                "Ensure the Platform description contains `[Proxmox LXC Template: <volid>]` or custom field `lxc_template` is populated."
            )
        template_name = template_volid or ""
    else:
        template_id_override = int(custom_fields["template_id"]) if custom_fields.get("template_id") else None
        tpl_id, tpl_name, cat = proxmox_driver.resolve_template_for_platform(
            platform_slug=platform_slug,
            platform_name=platform_name,
            platform_description=platform_desc,
            requested_template_id=template_id_override,
            node=node,
        )
        template_id = tpl_id
        template_name = tpl_name
        category = cat or "linux"
        if not template_id:
            missing_errors.append(
                f"- **OS Template (VM)**: Platform '{platform_name or 'None'}' does not map to a Proxmox VM template ID. "
                "Ensure the Platform description contains `[Proxmox VM Template: <vmid>]` or custom field `template_id` is populated."
            )

    # 4. Storage Pool (Datastore)
    storage = (
        custom_fields.get("storage")
        or cfg_ctx.get("datastore")
        or cfg_ctx.get("storage")
        or app_config.defaults.get("storage")
    )
    if not storage:
        missing_errors.append(
            "- **Storage Pool**: No storage datastore defined (expected `datastore` in NetBox Config Context or custom field `storage`)."
        )

    # 5. Network Bridge
    bridge = (
        custom_fields.get("bridge")
        or cfg_ctx.get("bridge")
        or app_config.defaults.get("bridge")
    )
    if not bridge:
        missing_errors.append(
            "- **Network Bridge**: No network bridge defined (expected `bridge` in NetBox Config Context or custom field `bridge`)."
        )

    # 6. IP Address / Subnet
    if not primary_ip and not cfg_ctx.get("subnet") and not app_config.defaults.get("subnet"):
        missing_errors.append(
            "- **IP Address / Subnet**: No primary IP assigned and `subnet` is missing from NetBox Config Context for dynamic IPAM allocation."
        )

    # 7. Authentication / Credentials
    ctx_ssh_keys = "\n".join(cfg_ctx.get("ssh_keys", [])) if cfg_ctx.get("ssh_keys") else None
    lxc_ssh_key = custom_fields.get("ssh_key") or ctx_ssh_keys
    lxc_password = custom_fields.get("admin_password") or app_config.templates.get("default_linux_password") or app_config.templates.get("default_password")
    if is_lxc and not lxc_ssh_key and not lxc_password:
        missing_errors.append(
            "- **Authentication (LXC)**: No SSH public key or root password provided (expected `ssh_keys` in NetBox Config Context or custom field `ssh_key`/`admin_password`)."
        )

    # 8. Hardware Sizing
    raw_disk = data.get("disk") or custom_fields.get("disk_size_gb")
    disk_size_gb = None
    if raw_disk:
        try:
            d_val = int(raw_disk)
            disk_size_gb = d_val // 1024 if d_val >= 1024 else d_val
        except (ValueError, TypeError): pass

    raw_cores = data.get("vcpus")
    cores = int(raw_cores) if raw_cores else None

    raw_memory = data.get("memory")
    memory_mb = None
    if raw_memory:
        try:
            m_val = int(raw_memory)
            memory_mb = m_val * 1024 if m_val < 128 else m_val
        except (ValueError, TypeError): pass

    if (not cores or not memory_mb or not disk_size_gb) and data.get("virtual_machine_type"):
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

    fb = app_config.fallbacks
    cores = cores or fb.get("cores", 2)
    memory_mb = memory_mb or fb.get("memory_mb", 2048)
    disk_size_gb = disk_size_gb or fb.get("disk_gb", 20)

    # 9. Start on Boot
    onboot = True
    start_on_boot_data = data.get("start_on_boot")
    if isinstance(start_on_boot_data, dict):
        if start_on_boot_data.get("value") == "off":
            onboot = False
    elif isinstance(start_on_boot_data, str) and start_on_boot_data.lower() in ("off", "false", "0"):
        onboot = False

    # Check if validation passed
    if missing_errors:
        workload_kind = "LXC Container" if is_lxc else "Virtual Machine"
        bullet_list = "\n".join(missing_errors)
        journal_comment = (
            f"❌ **Provisioning Pre-Flight Validation Failed** (Job ID: `{job_id}`)\n\n"
            f"The {workload_kind} could not be provisioned because required parameters are missing or invalid:\n"
            f"{bullet_list}\n\n"
            f"**Remediation**:\n"
            f"Please configure the missing attributes on the NetBox object or in its Config Context, then re-save to trigger provisioning."
        )
        if netbox_vm_id:
            try:
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=journal_comment,
                )
            except Exception as j_err:
                logger.warning("Could not post pre-flight validation failure to NetBox Journal: %s", j_err)

        await db.append_log(
            job_id,
            f"Provisioning pre-flight validation failed for '{hostname}': {'; '.join(missing_errors)}"
        )
        await db.update_job(job_id, status="failed", hostname=hostname)
        return False, missing_errors, {}

    vmid = custom_fields.get("proxmox_vmid")
    blueprint = {
        "hostname": hostname,
        "node": node,
        "is_lxc": is_lxc,
        "category": category,
        "template_volid": template_volid,
        "template_id": template_id,
        "template_name": template_name,
        "vmid": int(vmid) if vmid else None,
        "primary_ip": primary_ip,
        "storage": storage,
        "bridge": bridge,
        "cores": cores,
        "memory_mb": memory_mb,
        "disk_size_gb": disk_size_gb,
        "onboot": onboot,
        "gateway": cfg_ctx.get("gateway"),
        "dns_server": cfg_ctx.get("dns_servers", [None])[0] if cfg_ctx.get("dns_servers") else None,
        "dns_domain": cfg_ctx.get("domain") or cfg_ctx.get("dns_domain"),
        "ssh_key": lxc_ssh_key if is_lxc else (custom_fields.get("ssh_key") or ctx_ssh_keys),
        "ci_user": cfg_ctx.get("default_user") or "root",
    }

    await db.append_log(
        job_id,
        f"Pre-flight blueprint validated successfully: node='{node}', type='{'LXC' if is_lxc else category.upper()}', "
        f"template='{template_volid or template_name}', storage='{storage}', bridge='{bridge}', ip='{primary_ip or 'dhcp'}'"
    )
    return True, [], blueprint


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
    tag_slugs = {
        (t.get("slug") or t.get("name", "")).lower() if isinstance(t, dict) else str(t).lower()
        for t in data.get("tags", [])
    }

    # Guard 1: Strict Cluster ID Matching
    # If the VM has an assigned cluster, verify it matches our Proxmox cluster ID in config.yml
    target_cluster_id = app_config.defaults.get("cluster_id")
    cluster_data = data.get("cluster") or {}
    vm_cluster_id = cluster_data.get("id") if isinstance(cluster_data, dict) else None
    cluster_name = cluster_data.get("name", "") if isinstance(cluster_data, dict) else ""

    if target_cluster_id and vm_cluster_id and vm_cluster_id != target_cluster_id:
        await db.append_log(
            job_id,
            f"VM/CT '{hostname}' belongs to cluster '{cluster_name}' (ID: {vm_cluster_id}), but orchestrator is configured for Proxmox Cluster ID {target_cluster_id}. Skipping orchestration.",
        )
        await db.update_job(job_id, status="completed")
        return

    # Guard 2: Strict Site ID Matching
    # If the VM has an assigned site, verify it matches our Proxmox site ID in config.yml
    target_site_id = app_config.defaults.get("site_id")
    site_data = data.get("site") or {}
    vm_site_id = site_data.get("id") if isinstance(site_data, dict) else None
    site_name = site_data.get("name", "") if isinstance(site_data, dict) else ""

    if target_site_id and vm_site_id and vm_site_id != target_site_id:
        await db.append_log(
            job_id,
            f"VM/CT '{hostname}' belongs to site '{site_name}' (ID: {vm_site_id}), but orchestrator is configured for Proxmox Site ID {target_site_id}. Skipping orchestration.",
        )
        await db.update_job(job_id, status="completed")
        return

    # Check if this is a Decommission / Delete event
    status_data = data.get("status")
    status_val = status_data.get("value") if isinstance(status_data, dict) else str(status_data or "")

    # 1. Permanent Purge if VM object was completely deleted from NetBox
    if event == "deleted":
        if not existing_vmid:
            await db.append_log(job_id, f"Deleted VM/CT '{hostname}' has no Proxmox VMID assigned. Skipping Proxmox deletion.")
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

        # Guard A: Already tagged as decommissioned
        if "decommissioned" in tag_slugs:
            await db.append_log(
                job_id,
                f"VM '{hostname}' is already tagged as 'decommissioned'. Skipping redundant decommission workflow.",
            )
            await db.update_job(job_id, status="completed")
            return

        # Guard B: Check debounce cache (prevents echo webhooks from triggering within debounce window)
        if is_recently_decommissioned(int(existing_vmid)):
            await db.append_log(
                job_id,
                f"VM '{hostname}' (VMID {existing_vmid}) was quarantined within the debounce window. Skipping echo webhook.",
            )
            await db.update_job(job_id, status="completed")
            return

        # Guard C: Snapshot verification (only trigger if status transitioned TO decommissioning)
        snapshots = payload.get("snapshots") or {}
        pre_status_data = snapshots.get("prechange", {}).get("status") if isinstance(snapshots.get("prechange"), dict) else None
        if pre_status_data is not None:
            pre_val = (pre_status_data.get("value") if isinstance(pre_status_data, dict) else str(pre_status_data)).lower()
            if pre_val in ("decommissioning", "deprovisioning"):
                await db.append_log(
                    job_id,
                    f"VM '{hostname}' pre-change status was already '{pre_val}'. Status did not transition to decommissioning in this event. Skipping.",
                )
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

    # 3. Existing VM: Power State Synchronization, Hardware/Name Synchronization & Dynamic DNS
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

        # Extract deltas between prechange and postchange snapshots
        snapshots = payload.get("snapshots") or {}
        pre_snapshot = snapshots.get("prechange") if isinstance(snapshots, dict) else None
        post_snapshot = snapshots.get("postchange") if isinstance(snapshots, dict) else None
        deltas = extract_vm_deltas(pre_snapshot, post_snapshot)

        # Drop initial provisioning completion write-back echoes in <1ms
        if deltas.get("is_provisioning_writeback"):
            await db.append_log(
                job_id,
                f"VM '{hostname}' update is the initial provisioning write-back (VMID: {existing_vmid}). Dropping echo webhook in <1ms.",
            )
            await db.update_job(job_id, status="completed", vmid=int(existing_vmid), hostname=hostname)
            return

        # Drop pure telemetry/metrics echoes in <1ms without calling Proxmox
        if deltas.get("is_telemetry_only"):
            ch_fields = deltas.get("changed_fields", [])
            await db.append_log(
                job_id,
                f"VM '{hostname}' (VMID: {existing_vmid}) update contains only telemetry/metrics delta ({', '.join(ch_fields)}). Dropping echo webhook in <1ms.",
            )
            await db.update_job(job_id, status="completed", vmid=int(existing_vmid), hostname=hostname)
            return

        # Drop non-actionable metadata-only updates (e.g. tags, comments, description) in <1ms
        if (
            deltas.get("has_snapshots")
            and not deltas.get("status_changed")
            and not deltas.get("hardware_changed")
            and not deltas.get("ip_changed")
        ):
            ch_fields = deltas.get("changed_fields", [])
            await db.append_log(
                job_id,
                f"VM '{hostname}' (VMID: {existing_vmid}) update contains no actionable power, hardware, or network changes (delta: {', '.join(ch_fields) if ch_fields else 'none'}). Dropping webhook in <1ms.",
            )
            await db.update_job(job_id, status="completed", vmid=int(existing_vmid), hostname=hostname)
            return

        # Dynamic DNS reconciliation if primary IP changed
        if deltas.get("ip_changed"):
            new_ip = deltas.get("new_ip") or primary_ip
            old_ip = deltas.get("old_ip")
            dns_zone = app_config.dns.get("default_zone", "homelab.local")
            await db.append_log(
                job_id,
                f"VM '{hostname}' primary IP changed from '{old_ip}' to '{new_ip}'. Updating NetBox DNS (A & PTR) in zone '{dns_zone}'...",
            )
            try:
                dns_ok = await netbox_driver.create_or_update_dns_record(
                    hostname=hostname,
                    ip_address=new_ip,
                    zone_name=dns_zone,
                )
                if dns_ok:
                    await db.append_log(job_id, f"Successfully reconciled NetBox DNS records for '{hostname}.{dns_zone}' -> {new_ip}")
                    if netbox_vm_id:
                        await netbox_driver.add_journal_entry(
                            assigned_object_type="virtualization.virtualmachine",
                            assigned_object_id=netbox_vm_id,
                            comment=f"Primary IP updated from {old_ip} to {new_ip}. Reconciled DNS A and PTR records in zone '{dns_zone}'. (Job ID: {job_id})",
                        )
                    await notifier.notify_job_success(
                        job_id,
                        "vm_ip_changed",
                        {"vmid": int(existing_vmid), "hostname": hostname, "old_ip": old_ip, "new_ip": new_ip},
                    )
            except Exception as e:
                logger.warning("Could not reconcile DNS records for %s: %s", hostname, e)
                await db.append_log(job_id, f"Warning: Failed to reconcile DNS for '{hostname}': {e}")

        # A) Power State Synchronization (only if status actually changed or snapshots absent)
        power_task_ran = False
        if deltas.get("status_changed", True):
            if status_val.lower() in ("offline", "stopped"):
                await db.append_log(job_id, f"VM '{hostname}' status changed to 'offline'. Synchronizing power state -> STOP (disabling onboot)...")
                await run_power_sync_task(
                    job_id=job_id,
                    vmid=int(existing_vmid),
                    hostname=hostname,
                    target_state="stop",
                    node=node,
                    desired_onboot=False,
                    netbox_vm_id=netbox_vm_id,
                )
                power_task_ran = True
            elif status_val.lower() in ("active", "running"):
                await db.append_log(job_id, f"VM '{hostname}' status changed to 'active'. Synchronizing power state -> START (enabling onboot)...")
                await run_power_sync_task(
                    job_id=job_id,
                    vmid=int(existing_vmid),
                    hostname=hostname,
                    target_state="start",
                    node=node,
                    desired_onboot=True,
                    netbox_vm_id=netbox_vm_id,
                )
                power_task_ran = True

        # B) Hardware Specs & Name Synchronization (only if hardware specs changed or snapshots absent)
        vm_sync_ran = False
        if deltas.get("hardware_changed", True):
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
            pre_type = pre_snapshot.get("virtual_machine_type") if pre_snapshot else None
            post_type = post_snapshot.get("virtual_machine_type") if post_snapshot else data.get("virtual_machine_type")

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
            vm_sync_ran = True

        if not power_task_ran and not vm_sync_ran:
            await db.append_log(
                job_id,
                f"VM '{hostname}' (VMID: {existing_vmid}) update did not change power state or hardware specs. No Proxmox action required.",
            )
            await db.update_job(job_id, status="completed", vmid=int(existing_vmid), hostname=hostname)

        return

    # Guard: do NOT provision VMs that are in offline, failed, or decommissioning status, or tagged decommissioned
    if status_val.lower() in ("offline", "failed", "decommissioning", "decommissioned") or "decommissioned" in tag_slugs:
        await db.append_log(job_id, f"VM '{hostname}' is in status '{status_val}' (tags: {list(tag_slugs)}). Skipping automatic provisioning.")
        await db.update_job(job_id, status="completed")
        return

    # Guard 3: Check if a VM/CT with this exact hostname already exists on Proxmox
    existing_pve_vm = proxmox_driver.find_vm_by_name(hostname)
    if existing_pve_vm:
        discovered_vmid = existing_pve_vm["vmid"]
        discovered_node = existing_pve_vm.get("node")
        discovered_type = existing_pve_vm.get("type", "qemu")
        await db.append_log(
            job_id,
            f"Found existing Proxmox {discovered_type.upper()} '{hostname}' with VMID {discovered_vmid} on node '{discovered_node}'. "
            f"Auto-linking NetBox VM #{netbox_vm_id} to VMID {discovered_vmid} instead of cloning duplicate.",
        )
        if netbox_vm_id:
            try:
                await netbox_driver.update_virtual_machine(
                    vm_id=netbox_vm_id,
                    custom_fields={"proxmox_vmid": discovered_vmid, "proxmox_node": discovered_node},
                    comments=f"Auto-linked to existing Proxmox {discovered_type.upper()} (VMID: {discovered_vmid}).",
                )
            except Exception as e:
                logger.warning("Could not auto-link VMID %d to NetBox VM %d: %s", discovered_vmid, netbox_vm_id, e)

        # Trigger sync task to align power/specs
        effective_onboot = 0 if status_val.lower() in ("offline", "stopped") else 1
        await run_vm_sync_task(
            job_id=job_id,
            vmid=discovered_vmid,
            hostname=hostname,
            node=discovered_node,
            onboot=effective_onboot,
            netbox_vm_id=netbox_vm_id,
            ip_address=None,
        )
        return

    # Early Concurrency Lock: prevent recursive webhook race conditions from duplicate provisioning
    lock_key = f"vm_{netbox_vm_id or hostname}"
    if lock_key in _active_provisioning_vms:
        await db.append_log(job_id, f"Provisioning workflow already in progress for '{hostname}' (NetBox ID: {netbox_vm_id}). Skipping duplicate event.")
        await db.update_job(job_id, status="completed")
        return

    _active_provisioning_vms.add(lock_key)
    try:
        # Extract NetBox Config Context early
        cfg_ctx = data.get("config_context") or {}
        ctx_subnet = cfg_ctx.get("subnet") or app_config.defaults.get("subnet")

        # Extract Primary IP / Requested IP
        primary_ip = None
        if custom_fields.get("requested_ip"):
            raw_ip = str(custom_fields["requested_ip"]).strip()
            primary_ip = raw_ip.split("/")[0] if raw_ip else None
        elif data.get("primary_ip4") and isinstance(data["primary_ip4"], dict):
            primary_ip = data["primary_ip4"].get("address", "").split("/")[0]
        elif data.get("primary_ip") and isinstance(data["primary_ip"], dict):
            primary_ip = data["primary_ip"].get("address", "").split("/")[0]

        # Dynamic NetBox IPAM Next-Available-IP Allocation if no IP was provided
        if not primary_ip and ctx_subnet:
            allocated_ip = await netbox_driver.get_or_allocate_available_ip(
                prefix_cidr=ctx_subnet,
                hostname=hostname,
            )
            if allocated_ip:
                primary_ip = allocated_ip
                await db.append_log(job_id, f"Auto-allocated next available IP from NetBox IPAM: {primary_ip}")

        # Declarative Pre-Flight Blueprint Validation (Config Context SSoT)
        is_valid, validation_errors, bp = await validate_vm_creation_blueprint(
            job_id=job_id,
            data=data,
            cfg_ctx=cfg_ctx,
            custom_fields=custom_fields,
            primary_ip=primary_ip,
            netbox_vm_id=netbox_vm_id,
        )

        if not is_valid:
            # Pre-flight validation failed; errors were recorded to NetBox Journal and DB
            return

        # Execute provisioning based on validated blueprint
        if bp["is_lxc"]:
            lxc_password = custom_fields.get("admin_password") or app_config.templates.get("default_linux_password") or app_config.templates.get("default_password")
            params = {
                "hostname": hostname,
                "template_volid": bp["template_volid"],
                "node": bp["node"],
                "vmid": bp["vmid"],
                "ip_address": bp["primary_ip"],
                "gateway": bp["gateway"],
                "dns_server": bp["dns_server"],
                "dns_domain": bp["dns_domain"],
                "disk_size_gb": bp["disk_size_gb"],
                "cores": bp["cores"],
                "memory_mb": bp["memory_mb"],
                "swap_mb": int(custom_fields.get("swap_mb", 512)),
                "onboot": bp["onboot"],
                "ssh_key": bp["ssh_key"],
                "password": lxc_password,
                "storage": bp["storage"],
                "bridge": bp["bridge"],
                "unprivileged": True,
                "features": "nesting=1",
            }
            await run_lxc_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
        elif bp["category"] == "windows":
            admin_password = custom_fields.get("admin_password") or app_config.templates.get("default_windows_password", "P@ssw0rdInitial!")
            params = {
                "hostname": hostname,
                "admin_password": admin_password,
                "template_id": bp["template_id"],
                "node": bp["node"],
                "vmid": bp["vmid"],
                "ip_address": bp["primary_ip"],
                "gateway": bp["gateway"],
                "dns_server": bp["dns_server"],
                "dns_domain": bp["dns_domain"],
                "disk_size_gb": bp["disk_size_gb"],
                "cores": bp["cores"],
                "memory_mb": bp["memory_mb"],
                "onboot": bp["onboot"],
                "storage": bp["storage"],
                "bridge": bp["bridge"],
            }
            await run_windows_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
        else:
            params = {
                "hostname": hostname,
                "template_id": bp["template_id"],
                "node": bp["node"],
                "vmid": bp["vmid"],
                "ip_address": bp["primary_ip"],
                "gateway": bp["gateway"],
                "dns_server": bp["dns_server"],
                "dns_domain": bp["dns_domain"],
                "disk_size_gb": bp["disk_size_gb"],
                "cores": bp["cores"],
                "memory_mb": bp["memory_mb"],
                "onboot": bp["onboot"],
                "ssh_key": bp["ssh_key"],
                "ci_user": bp["ci_user"],
                "storage": bp["storage"],
                "bridge": bp["bridge"],
            }
            await run_linux_provision_task(job_id, params, netbox_vm_id=netbox_vm_id)
    finally:
        _active_provisioning_vms.discard(lock_key)

