import logging
from typing import Dict, Any, Optional

from app.core.app_config import app_config
from app.storage.db import db
from app.workers.reconciler import reconciler, normalize_status, normalize_ip, normalize_start_on_boot

logger = logging.getLogger("orchestrator.workers.dispatcher")

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


def extract_vm_deltas(pre: Optional[Dict[str, Any]], post: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Computes fine-grained differences between NetBox prechange and postchange snapshots.
    Used for fast <1ms dropping of telemetry echoes and provisioning write-back echoes.
    """
    if not isinstance(pre, dict) or not isinstance(post, dict):
        return {
            "has_snapshots": False,
            "status_changed": True,
            "hardware_changed": True,
            "ip_changed": False,
            "is_telemetry_only": False,
            "is_provisioning_writeback": False,
            "changed_fields": [],
        }

    # 1. Power State Delta
    old_status = normalize_status(pre.get("status"))
    new_status = normalize_status(post.get("status"))
    status_changed = bool(old_status and new_status and old_status != new_status)

    # 2. IP Delta
    old_ip = normalize_ip(pre) or str((pre.get("custom_fields") or {}).get("requested_ip") or "").split("/")[0].strip() or None
    new_ip = normalize_ip(post) or str((post.get("custom_fields") or {}).get("requested_ip") or "").split("/")[0].strip() or None
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
    onboot_changed = normalize_start_on_boot(pre.get("start_on_boot")) != normalize_start_on_boot(post.get("start_on_boot"))
    node_changed = bool(pre_vmid) and (pre_cf.get("proxmox_node") != post_cf.get("proxmox_node"))

    hardware_changed = any([
        name_changed,
        vcpus_changed,
        memory_changed,
        disk_changed,
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


async def process_netbox_webhook_event(job_id: str, payload: Dict[str, Any]):
    """
    Lightweight Ingress Filter & Webhook Wakeup Hint Dispatcher.
    Applies strict cluster/site boundary filters and fast echo drops,
    then dispatches the event to the Declared-State Reconciliation Engine.
    """
    event = payload.get("event")
    model = payload.get("model") or payload.get("data", {}).get("object_type")
    data = payload.get("data", {})

    await db.append_log(job_id, f"Received NetBox webhook hint '{event}' for model '{model}'")

    # Guard 1: Verify this is a Virtual Machine object
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

    netbox_vm_id = data.get("id")
    hostname = data.get("name")
    if not hostname and event != "deleted":
        raise ValueError("NetBox VM payload missing required 'name' field")

    # Guard 2: Strict Cluster ID Matching (<2ms drop)
    target_cluster_id = app_config.defaults.get("cluster_id")
    cluster_data = data.get("cluster") or {}
    vm_cluster_id = cluster_data.get("id") if isinstance(cluster_data, dict) else None
    cluster_name = cluster_data.get("name", "") if isinstance(cluster_data, dict) else ""

    if target_cluster_id and vm_cluster_id and vm_cluster_id != target_cluster_id:
        await db.append_log(
            job_id,
            f"VM '{hostname}' belongs to cluster '{cluster_name}' (ID: {vm_cluster_id}), but orchestrator manages Cluster ID {target_cluster_id}. Skipping in <2ms.",
        )
        await db.update_job(job_id, status="completed", hostname=hostname)
        return

    # Guard 3: Strict Site ID Matching (<2ms drop)
    target_site_id = app_config.defaults.get("site_id")
    site_data = data.get("site") or {}
    vm_site_id = site_data.get("id") if isinstance(site_data, dict) else None
    site_name = site_data.get("name", "") if isinstance(site_data, dict) else ""

    if target_site_id and vm_site_id and vm_site_id != target_site_id:
        await db.append_log(
            job_id,
            f"VM '{hostname}' belongs to site '{site_name}' (ID: {vm_site_id}), but orchestrator manages Site ID {target_site_id}. Skipping in <2ms.",
        )
        await db.update_job(job_id, status="completed", hostname=hostname)
        return

    # Guard 4: Fast Echo Filtering via Snapshots (<1ms drop)
    snapshots = payload.get("snapshots") or {}
    pre_snapshot = snapshots.get("prechange") if isinstance(snapshots, dict) else None
    post_snapshot = snapshots.get("postchange") if isinstance(snapshots, dict) else None

    if pre_snapshot and post_snapshot:
        deltas = extract_vm_deltas(pre_snapshot, post_snapshot)

        # Drop initial provisioning completion write-back echoes
        if deltas.get("is_provisioning_writeback"):
            await db.append_log(
                job_id,
                f"VM '{hostname}' update is initial provisioning write-back echo. Dropping in <1ms.",
            )
            await db.update_job(job_id, status="completed", hostname=hostname)
            return

        # Drop pure telemetry/metrics echoes
        if deltas.get("is_telemetry_only"):
            await db.append_log(
                job_id,
                f"VM '{hostname}' update is purely telemetry/metrics echo ({', '.join(deltas.get('changed_fields', []))}). Dropping in <1ms.",
            )
            await db.update_job(job_id, status="completed", hostname=hostname)
            return

        # Drop non-actionable metadata changes (tags, comments, descriptions)
        if (
            not deltas.get("status_changed")
            and not deltas.get("hardware_changed")
            and not deltas.get("ip_changed")
        ):
            await db.append_log(
                job_id,
                f"VM '{hostname}' update contains no actionable power, hardware, or network drift. Dropping in <1ms.",
            )
            await db.update_job(job_id, status="completed", hostname=hostname)
            return

    # Guard 5: Pass to Reconciliation Engine
    await db.append_log(
        job_id,
        f"Dispatching event '{event}' for VM '{hostname}' (#{netbox_vm_id}) to Reconciliation Engine...",
    )
    result = await reconciler.reconcile_workload(
        netbox_vm_id=netbox_vm_id,
        trigger=f"webhook_{event}",
        hint_payload=data,
        hint_event=event,
        job_id=job_id,
    )
    logger.info("Webhook reconciliation for VM #%s complete: %s", netbox_vm_id, result.get("status"))
