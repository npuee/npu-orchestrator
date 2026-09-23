import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Tuple, Set

from app.core.config import settings
from app.core.app_config import app_config
from app.core.modules import module_manager
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
    is_recently_decommissioned,
)

logger = logging.getLogger("orchestrator.reconciler")

# Global locks to prevent race conditions during reconciliation
_reconciliation_locks: Dict[int, asyncio.Lock] = {}
_cluster_reconciliation_lock = asyncio.Lock()


@dataclass
class WorkloadDeclaredState:
    """Normalized declared state from NetBox (Single Source of Truth)."""
    vm_id: int
    name: str
    status: str
    vmid: Optional[int]
    node: Optional[str]
    vcpus: Optional[int]
    memory_mb: Optional[int]
    disk_gb: Optional[int]
    onboot: Optional[bool]
    primary_ip: Optional[str]
    cluster_id: Optional[int]
    site_id: Optional[int]
    tags: Set[str] = field(default_factory=set)
    config_context: Dict[str, Any] = field(default_factory=dict)
    custom_fields: Dict[str, Any] = field(default_factory=dict)
    raw_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkloadActualState:
    """Normalized actual state from Proxmox VE hypervisor."""
    exists: bool
    vmid: Optional[int] = None
    name: Optional[str] = None
    status: Optional[str] = None  # "running", "stopped", etc.
    node: Optional[str] = None
    is_lxc: bool = False
    cores: Optional[int] = None
    memory_mb: Optional[int] = None
    disk_gb: Optional[int] = None
    onboot: Optional[bool] = None


@dataclass
class WorkloadDelta:
    """Calculated difference between declared and actual workload state."""
    action: str  # "noop", "provision", "adopt", "decommission", "purge", "reconcile"
    power_transition: Optional[str] = None  # "start", "stop", None
    onboot_transition: Optional[bool] = None
    hardware_changes: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)  # field: (actual, declared)
    ip_changed: bool = False
    name_changed: bool = False
    old_name: Optional[str] = None
    new_name: Optional[str] = None
    reasons: List[str] = field(default_factory=list)


def normalize_status(val: Any) -> Optional[str]:
    if isinstance(val, dict):
        s = val.get("value") or val.get("label")
        return str(s).strip().lower() if s is not None else None
    elif val is not None:
        return str(val).strip().lower()
    return None


def normalize_ip(obj: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    ip_data = obj.get("primary_ip4") or obj.get("primary_ip")
    if isinstance(ip_data, dict):
        addr = ip_data.get("address", "")
        return addr.split("/")[0].strip() if addr else None
    elif isinstance(ip_data, str):
        return ip_data.split("/")[0].strip() if ip_data else None
    return None


def normalize_start_on_boot(val: Any) -> Optional[bool]:
    if isinstance(val, dict):
        v = val.get("value")
        return v != "off" if v is not None else None
    elif isinstance(val, str):
        return val.lower() not in ("off", "false", "0")
    elif isinstance(val, bool):
        return val
    return None


def extract_declared_state(data: Dict[str, Any]) -> WorkloadDeclaredState:
    """Extracts and normalizes the declared state from a NetBox VirtualMachine payload."""
    vm_id = data.get("id")
    name = data.get("name", "")
    status_raw = data.get("status")
    status_val = normalize_status(status_raw) or "unknown"

    cf = data.get("custom_fields") or {}
    vmid_val = cf.get("proxmox_vmid")
    vmid = int(vmid_val) if vmid_val else None

    node = cf.get("proxmox_node")
    if not node and data.get("device") and isinstance(data["device"], dict):
        dev_name = data["device"].get("name", "")
        if "proxmox" in dev_name.lower():
            node = dev_name.split(".")[0] if "." in dev_name else dev_name

    cfg_ctx = data.get("config_context") or {}
    if not node and cfg_ctx.get("default_node"):
        node = cfg_ctx.get("default_node")

    # Cores
    raw_vcpus = data.get("vcpus")
    cores = int(raw_vcpus) if raw_vcpus else None

    # Memory
    raw_memory = data.get("memory")
    memory_mb = None
    if raw_memory:
        try:
            m_val = int(raw_memory)
            memory_mb = m_val * 1024 if m_val < 128 else m_val
        except (ValueError, TypeError):
            pass

    # Disk
    raw_disk = data.get("disk") or cf.get("disk_size_gb")
    disk_gb = None
    if raw_disk:
        try:
            d_val = int(raw_disk)
            disk_gb = d_val // 1024 if d_val >= 1024 else d_val
        except (ValueError, TypeError):
            pass

    # Fallback to VM Type if hardware not explicitly overridden
    vm_t = data.get("virtual_machine_type")
    if (not cores or not memory_mb or not disk_gb) and isinstance(vm_t, dict):
        if not cores and vm_t.get("default_vcpus"):
            try: cores = int(vm_t["default_vcpus"])
            except (ValueError, TypeError): pass
        if not memory_mb and vm_t.get("default_memory"):
            try: memory_mb = int(vm_t["default_memory"])
            except (ValueError, TypeError): pass
        if not disk_gb and vm_t.get("default_disk"):
            try: disk_gb = int(vm_t["default_disk"])
            except (ValueError, TypeError): pass

    fb = app_config.fallbacks
    cores = cores or fb.get("cores", 2)
    memory_mb = memory_mb or fb.get("memory_mb", 2048)
    disk_gb = disk_gb or fb.get("disk_gb", 20)

    # Start on Boot
    onboot = normalize_start_on_boot(data.get("start_on_boot"))
    if onboot is None:
        onboot = True

    # Primary IP
    primary_ip = normalize_ip(data) or str(cf.get("requested_ip") or "").split("/")[0].strip() or None

    # Cluster & Site
    cluster_data = data.get("cluster") or {}
    cluster_id = cluster_data.get("id") if isinstance(cluster_data, dict) else None

    site_data = data.get("site") or {}
    site_id = site_data.get("id") if isinstance(site_data, dict) else None

    # Tags
    tags = {
        (t.get("slug") or t.get("name", "")).lower() if isinstance(t, dict) else str(t).lower()
        for t in data.get("tags", [])
    }

    return WorkloadDeclaredState(
        vm_id=vm_id,
        name=name,
        status=status_val,
        vmid=vmid,
        node=node,
        vcpus=cores,
        memory_mb=memory_mb,
        disk_gb=disk_gb,
        onboot=onboot,
        primary_ip=primary_ip,
        cluster_id=cluster_id,
        site_id=site_id,
        tags=tags,
        config_context=cfg_ctx,
        custom_fields=cf,
        raw_data=data,
    )


def fetch_actual_state(
    vmid: Optional[int] = None,
    hostname: Optional[str] = None,
    node: Optional[str] = None,
) -> WorkloadActualState:
    """
    Inspects Proxmox VE to retrieve current live state for a VMID or hostname.
    Diff-aware: queries live resource status and configuration.
    """
    pve = proxmox_driver.client_mgr.get_client()
    target_node = proxmox_driver.client_mgr.resolve_node(node)

    # 1. Lookup by VMID if present
    matching_res = None
    try:
        resources = pve.cluster.resources.get(type="vm")
        if vmid:
            for r in resources:
                if r.get("vmid") == vmid:
                    matching_res = r
                    break
        elif hostname:
            for r in resources:
                if r.get("name") == hostname:
                    matching_res = r
                    break
    except Exception as e:
        logger.warning("Could not query cluster resources: %s", e)

    if not matching_res and not vmid and hostname:
        existing = proxmox_driver.find_vm_by_name(hostname)
        if existing:
            vmid = existing["vmid"]
            target_node = existing.get("node", target_node)

    if not matching_res and not vmid:
        return WorkloadActualState(exists=False)

    if matching_res:
        vmid = matching_res.get("vmid")
        target_node = matching_res.get("node", target_node)
        live_status = matching_res.get("status")
        is_lxc = matching_res.get("type") == "lxc"
        live_name = matching_res.get("name")
    else:
        # Fallback to direct node status probe
        is_lxc = False
        live_status = "unknown"
        live_name = hostname
        try:
            status_data = pve.nodes(target_node).lxc(vmid).status.current.get()
            is_lxc = True
            live_status = status_data.get("status")
            live_name = status_data.get("name")
        except Exception:
            try:
                status_data = pve.nodes(target_node).qemu(vmid).status.current.get()
                live_status = status_data.get("status")
                live_name = status_data.get("name")
            except Exception:
                return WorkloadActualState(exists=False, vmid=vmid)

    # 2. Fetch detailed configuration specs from Proxmox
    client_obj = pve.nodes(target_node).lxc(vmid) if is_lxc else pve.nodes(target_node).qemu(vmid)
    try:
        config = client_obj.config.get()
    except Exception as e:
        logger.warning("Could not fetch detailed config for VMID %d: %s", vmid, e)
        config = {}

    # Extract actual hardware specs
    cores_val = config.get("cores", 1)
    try:
        cores = int(cores_val)
    except (ValueError, TypeError):
        cores = 1

    mem_val = config.get("memory", 512)
    try:
        memory_mb = int(mem_val)
    except (ValueError, TypeError):
        memory_mb = 512

    onboot_val = config.get("onboot", 0)
    try:
        onboot = bool(int(onboot_val))
    except (ValueError, TypeError):
        onboot = False

    disk_gb = 0
    if is_lxc:
        rootfs_str = config.get("rootfs", "")
        size_match = re.search(r"size=([0-9]+)([GM])", rootfs_str)
        if size_match:
            val, unit = int(size_match.group(1)), size_match.group(2)
            disk_gb = val if unit == "G" else val // 1024
        conf_name = config.get("hostname")
        if conf_name:
            live_name = conf_name
    else:
        scsi_str = config.get("scsi0", "")
        size_match = re.search(r"size=([0-9]+)([GM])", scsi_str)
        if size_match:
            val, unit = int(size_match.group(1)), size_match.group(2)
            disk_gb = val if unit == "G" else val // 1024
        conf_name = config.get("name")
        if conf_name:
            live_name = conf_name

    return WorkloadActualState(
        exists=True,
        vmid=vmid,
        name=live_name,
        status=live_status,
        node=target_node,
        is_lxc=is_lxc,
        cores=cores,
        memory_mb=memory_mb,
        disk_gb=disk_gb,
        onboot=onboot,
    )


def compute_workload_delta(
    declared: Optional[WorkloadDeclaredState],
    actual: WorkloadActualState,
    hint_event: Optional[str] = None,
) -> WorkloadDelta:
    """
    Core Pure Function: Compares Declared State (NetBox) against Actual State (Proxmox).
    Returns a typed WorkloadDelta specifying exact state transitions required.
    """
    # Case 1: Workload was deleted from NetBox or explicitly deleted in event hint
    if declared is None or hint_event == "deleted":
        if actual.exists and actual.vmid:
            return WorkloadDelta(
                action="purge",
                reasons=[f"Workload does not exist in NetBox (SSoT). Purging VMID {actual.vmid} from Proxmox."],
            )
        return WorkloadDelta(action="noop", reasons=["Workload deleted from NetBox and does not exist on Proxmox."])

    # Case 2: Declared state is decommissioning or tagged decommissioned
    if declared.status in ("decommissioning", "deprovisioning") or "decommissioned" in declared.tags:
        if actual.exists and actual.vmid:
            if "decommissioned" in declared.tags:
                return WorkloadDelta(
                    action="noop",
                    reasons=["Workload already tagged as 'decommissioned'. Quarantine complete."],
                )
            return WorkloadDelta(
                action="decommission",
                reasons=[f"Declared status is '{declared.status}'. Quarantining VMID {actual.vmid} on Proxmox."],
            )
        return WorkloadDelta(action="noop", reasons=["Workload is marked decommissioned and not present on Proxmox."])

    # Case 3: Workload is missing on Proxmox
    if not actual.exists:
        if declared.status in ("active", "staged"):
            return WorkloadDelta(
                action="provision",
                reasons=[f"Workload '{declared.name}' is declared '{declared.status}' in NetBox but missing on Proxmox."],
            )
        return WorkloadDelta(
            action="noop",
            reasons=[f"Workload '{declared.name}' is in status '{declared.status}' and not deployed on Proxmox. Skipping."],
        )

    # Case 4: Pre-existing Workload Auto-Adoption
    # Proxmox has the VM, but NetBox object does not have proxmox_vmid populated
    if actual.exists and not declared.vmid:
        return WorkloadDelta(
            action="adopt",
            reasons=[f"Found pre-existing { 'LXC' if actual.is_lxc else 'QEMU' } workload '{actual.name}' (VMID: {actual.vmid}). Auto-linking to NetBox."],
        )

    # Case 5: Both exist -> Compute fine-grained drift
    reasons: List[str] = []
    power_transition = None
    desired_onboot = None
    hardware_changes: Dict[str, Tuple[Any, Any]] = {}
    name_changed = False
    old_name = actual.name
    new_name = declared.name

    # 5.1 Power State Drift
    # NetBox 'active' -> Proxmox running (onboot=True)
    # NetBox 'offline'/'stopped' -> Proxmox stopped (onboot=False)
    if declared.status in ("active", "running"):
        if actual.status not in ("running", "active"):
            power_transition = "start"
            desired_onboot = True
            reasons.append(f"Power drift: Declared 'active' but Proxmox status is '{actual.status}' -> START")
        elif actual.onboot is False:
            desired_onboot = True
            reasons.append("Onboot drift: Workload is active but onboot is 0 -> set onboot=1")
    elif declared.status in ("offline", "stopped"):
        if actual.status in ("running", "active"):
            power_transition = "stop"
            desired_onboot = False
            reasons.append(f"Power drift: Declared 'offline' but Proxmox status is '{actual.status}' -> STOP")
        elif actual.onboot is True:
            desired_onboot = False
            reasons.append("Onboot drift: Workload is offline but onboot is 1 -> set onboot=0")

    # 5.2 Hostname / Rename Drift
    if declared.name and actual.name and declared.name != actual.name:
        name_changed = True
        hardware_changes["name"] = (actual.name, declared.name)
        reasons.append(f"Hostname drift: Proxmox '{actual.name}' -> NetBox '{declared.name}'")

    # 5.3 Cores Drift
    if declared.vcpus and actual.cores and declared.vcpus != actual.cores:
        hardware_changes["cores"] = (actual.cores, declared.vcpus)
        reasons.append(f"CPU cores drift: Proxmox {actual.cores} -> NetBox {declared.vcpus}")

    # 5.4 Memory Drift
    if declared.memory_mb and actual.memory_mb and declared.memory_mb != actual.memory_mb:
        hardware_changes["memory"] = (actual.memory_mb, declared.memory_mb)
        reasons.append(f"RAM drift: Proxmox {actual.memory_mb}MB -> NetBox {declared.memory_mb}MB")

    # 5.5 Disk Drift (Only expands, Proxmox does not support safe shrinking)
    if declared.disk_gb and actual.disk_gb and declared.disk_gb > actual.disk_gb:
        hardware_changes["disk"] = (actual.disk_gb, declared.disk_gb)
        reasons.append(f"Disk size drift: Proxmox {actual.disk_gb}G -> NetBox {declared.disk_gb}G")

    # 5.6 Evaluate whether any drift exists
    has_drift = bool(power_transition or desired_onboot is not None or hardware_changes or name_changed)
    if has_drift:
        return WorkloadDelta(
            action="reconcile",
            power_transition=power_transition,
            onboot_transition=desired_onboot,
            hardware_changes=hardware_changes,
            name_changed=name_changed,
            old_name=old_name,
            new_name=new_name,
            reasons=reasons,
        )

    return WorkloadDelta(action="noop", reasons=["Workload is in full sync with declared state."])


async def validate_vm_creation_blueprint(
    job_id: str,
    declared: WorkloadDeclaredState,
) -> Tuple[bool, List[str], Dict[str, Any]]:
    """
    Declarative pre-flight validation for VM and Container provisioning.
    Validates required parameters across NetBox object fields, custom fields, and NetBox Config Context.
    """
    data = declared.raw_data
    cfg_ctx = declared.config_context
    custom_fields = declared.custom_fields
    hostname = declared.name
    primary_ip = declared.primary_ip
    netbox_vm_id = declared.vm_id
    missing_errors: List[str] = []

    # 1. Target Proxmox Node
    node = declared.node
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

    # If validation failed, write to NetBox Journal and orchestrator log
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

    blueprint = {
        "hostname": hostname,
        "node": node,
        "is_lxc": is_lxc,
        "category": category,
        "template_volid": template_volid,
        "template_id": template_id,
        "template_name": template_name,
        "vmid": declared.vmid,
        "primary_ip": primary_ip,
        "storage": storage,
        "bridge": bridge,
        "cores": declared.vcpus,
        "memory_mb": declared.memory_mb,
        "disk_size_gb": declared.disk_gb,
        "onboot": declared.onboot,
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


class ReconciliationEngine:
    """
    Kubernetes Controller / Terraform Model Declared-State Reconciliation Engine.
    Continuously compares Declared State (NetBox) against Actual State (Proxmox),
    computing typed deltas and driving the hypervisor to the declared state idempotently.
    """

    def __init__(self):
        self._active_vm_locks: Dict[int, asyncio.Lock] = {}

    def _get_lock(self, vm_id: int) -> asyncio.Lock:
        if vm_id not in self._active_vm_locks:
            self._active_vm_locks[vm_id] = asyncio.Lock()
        return self._active_vm_locks[vm_id]

    async def reconcile_workload(
        self,
        netbox_vm_id: int,
        trigger: str = "periodic",
        hint_payload: Optional[Dict[str, Any]] = None,
        hint_event: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Reconciles a single workload identified by its NetBox VM ID.
        Acquires an exclusive per-workload lock to serialize simultaneous reconciliations.
        """
        lock = self._get_lock(netbox_vm_id)
        if lock.locked():
            logger.info("Reconciliation already in progress for NetBox VM #%d. Awaiting lock...", netbox_vm_id)

        async with lock:
            if not job_id:
                job_id = f"job_rec_{netbox_vm_id}_{int(time.time())}"
                await db.create_job(
                    job_id=job_id,
                    action=f"reconcile_{trigger}",
                    metadata={"trigger": trigger, "netbox_vm_id": netbox_vm_id},
                )

            await db.append_log(job_id, f"Beginning state reconciliation for NetBox VM #{netbox_vm_id} (trigger={trigger})...")

            # 1. Fetch Declared State from NetBox
            declared: Optional[WorkloadDeclaredState] = None
            if hint_event != "deleted":
                nb_vm_raw = await netbox_driver.get_virtual_machine(netbox_vm_id)
                if nb_vm_raw:
                    declared = extract_declared_state(nb_vm_raw)
                elif hint_payload:
                    declared = extract_declared_state(hint_payload)
            elif hint_payload:
                # Deleted event snapshot
                declared = extract_declared_state(hint_payload)

            hostname = declared.name if declared else (hint_payload.get("name") if hint_payload else f"vm-{netbox_vm_id}")

            # 2. Strict Cluster & Site Isolation Guard Rails
            if declared:
                target_cluster_id = app_config.defaults.get("cluster_id")
                if target_cluster_id and declared.cluster_id and declared.cluster_id != target_cluster_id:
                    await db.append_log(
                        job_id,
                        f"VM '{hostname}' belongs to cluster ID {declared.cluster_id}, but orchestrator is configured for Cluster ID {target_cluster_id}. Skipping.",
                    )
                    await db.update_job(job_id, status="completed", hostname=hostname)
                    return {"status": "skipped", "reason": "cluster_mismatch"}

                target_site_id = app_config.defaults.get("site_id")
                if target_site_id and declared.site_id and declared.site_id != target_site_id:
                    await db.append_log(
                        job_id,
                        f"VM '{hostname}' belongs to site ID {declared.site_id}, but orchestrator is configured for Site ID {target_site_id}. Skipping.",
                    )
                    await db.update_job(job_id, status="completed", hostname=hostname)
                    return {"status": "skipped", "reason": "site_mismatch"}

            # 3. Fetch Actual State from Proxmox VE
            target_vmid = declared.vmid if declared else (hint_payload.get("custom_fields", {}).get("proxmox_vmid") if hint_payload else None)
            target_node = declared.node if declared else (hint_payload.get("custom_fields", {}).get("proxmox_node") if hint_payload else None)
            actual = fetch_actual_state(vmid=target_vmid, hostname=hostname, node=target_node)

            # 4. Compute State Delta
            delta = compute_workload_delta(declared=declared, actual=actual, hint_event=hint_event)
            await db.append_log(
                job_id,
                f"Computed State Delta for '{hostname}': action='{delta.action}', reasons={delta.reasons}",
            )

            # 5. Dispatch Action Based on Delta
            if delta.action == "noop":
                await db.append_log(job_id, f"Workload '{hostname}' is in sync. No action needed.")
                await db.update_job(job_id, status="completed", vmid=actual.vmid, hostname=hostname)
                return {"status": "in_sync", "vmid": actual.vmid, "action": "noop"}

            elif delta.action == "purge":
                await db.append_log(job_id, f"Permanent purge triggered for VMID {actual.vmid} ('{hostname}')...")
                await run_decommission_task(
                    job_id=job_id,
                    vmid=actual.vmid,
                    hostname=hostname,
                    node=actual.node,
                    netbox_vm_id=None,
                    permanent_purge=True,
                )
                return {"status": "purged", "vmid": actual.vmid}

            elif delta.action == "decommission":
                if is_recently_decommissioned(actual.vmid):
                    await db.append_log(job_id, f"VMID {actual.vmid} was quarantined recently. Skipping redundant decommission.")
                    await db.update_job(job_id, status="completed", vmid=actual.vmid, hostname=hostname)
                    return {"status": "debounced", "vmid": actual.vmid}

                await db.append_log(job_id, f"Quarantine (safe decommission) triggered for VMID {actual.vmid} ('{hostname}')...")
                await run_decommission_task(
                    job_id=job_id,
                    vmid=actual.vmid,
                    hostname=hostname,
                    node=actual.node,
                    netbox_vm_id=netbox_vm_id,
                    permanent_purge=False,
                )
                return {"status": "quarantined", "vmid": actual.vmid}

            elif delta.action == "adopt":
                await db.append_log(
                    job_id,
                    f"Auto-linking existing Proxmox VMID {actual.vmid} to NetBox VM #{netbox_vm_id}...",
                )
                await netbox_driver.update_virtual_machine(
                    vm_id=netbox_vm_id,
                    custom_fields={"proxmox_vmid": actual.vmid, "proxmox_node": actual.node},
                    comments=f"Auto-adopted existing Proxmox workload (VMID: {actual.vmid}).",
                )
                await netbox_driver.add_journal_entry(
                    assigned_object_type="virtualization.virtualmachine",
                    assigned_object_id=netbox_vm_id,
                    comment=f"Auto-adopted pre-existing Proxmox { 'LXC' if actual.is_lxc else 'QEMU' } workload (VMID: {actual.vmid}) on node '{actual.node}'. (Job ID: {job_id})",
                )
                # Re-run reconciliation to align power/hardware specs after adoption
                declared.vmid = actual.vmid
                post_adopt_delta = compute_workload_delta(declared=declared, actual=actual)
                if post_adopt_delta.action == "reconcile":
                    await self._apply_reconcile_transitions(job_id, declared, actual, post_adopt_delta)
                await db.update_job(job_id, status="completed", vmid=actual.vmid, hostname=hostname)
                return {"status": "adopted", "vmid": actual.vmid}

            elif delta.action == "provision":
                # Ensure IP allocation if missing
                if not declared.primary_ip:
                    ctx_subnet = declared.config_context.get("subnet") or app_config.defaults.get("subnet")
                    if ctx_subnet:
                        allocated_ip = await netbox_driver.get_or_allocate_available_ip(
                            prefix_cidr=ctx_subnet,
                            hostname=hostname,
                        )
                        if allocated_ip:
                            declared.primary_ip = allocated_ip
                            await db.append_log(job_id, f"Auto-allocated IP from NetBox IPAM: {allocated_ip}")

                # Pre-flight blueprint validation
                is_valid, validation_errors, bp = await validate_vm_creation_blueprint(
                    job_id=job_id,
                    declared=declared,
                )
                if not is_valid:
                    return {"status": "validation_failed", "errors": validation_errors}

                # Execute provisioning with synchronized VMID allocation lock
                if bp["is_lxc"]:
                    lxc_password = declared.custom_fields.get("admin_password") or app_config.templates.get("default_linux_password") or app_config.templates.get("default_password")
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
                        "swap_mb": int(declared.custom_fields.get("swap_mb", 512)),
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
                    admin_password = declared.custom_fields.get("admin_password") or app_config.templates.get("default_windows_password", "P@ssw0rdInitial!")
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

                return {"status": "provisioned", "hostname": hostname}

            elif delta.action == "reconcile":
                await self._apply_reconcile_transitions(job_id, declared, actual, delta)
                await db.update_job(job_id, status="completed", vmid=actual.vmid, hostname=hostname)
                return {"status": "reconciled", "vmid": actual.vmid, "diff": delta.reasons}

            return {"status": "unknown"}

    async def _apply_reconcile_transitions(
        self,
        job_id: str,
        declared: WorkloadDeclaredState,
        actual: WorkloadActualState,
        delta: WorkloadDelta,
    ):
        """Applies in-place drift corrections (power, hardware, hostname, DNS)."""
        vmid = actual.vmid or declared.vmid
        hostname = declared.name
        node = actual.node or declared.node

        # 1. Power State Transition
        if delta.power_transition:
            await db.append_log(job_id, f"Applying power state transition -> {delta.power_transition.upper()}...")
            await run_power_sync_task(
                job_id=job_id,
                vmid=vmid,
                hostname=hostname,
                target_state=delta.power_transition,
                node=node,
                desired_onboot=delta.onboot_transition,
                netbox_vm_id=declared.vm_id,
            )

        # 2. Hardware Specs / Name Synchronization
        if delta.hardware_changes or delta.name_changed or (delta.onboot_transition is not None and not delta.power_transition):
            effective_onboot = 0 if declared.status in ("offline", "stopped") else 1
            await db.append_log(job_id, f"Aligning Proxmox configuration for VMID {vmid} with declared NetBox specs...")
            await run_vm_sync_task(
                job_id=job_id,
                vmid=vmid,
                hostname=hostname,
                node=node,
                onboot=effective_onboot,
                cores=declared.vcpus,
                memory_mb=declared.memory_mb,
                disk_size_gb=declared.disk_gb,
                netbox_vm_id=declared.vm_id,
                ip_address=declared.primary_ip,
            )

        # 3. Dynamic DNS Alignment
        if delta.name_changed or delta.ip_changed:
            dns_zone = app_config.dns.get("default_zone", "homelab.local")
            if declared.primary_ip:
                await db.append_log(job_id, f"Reconciling NetBox DNS records for '{hostname}.{dns_zone}' -> {declared.primary_ip}...")
                try:
                    await netbox_driver.create_or_update_dns_record(
                        hostname=hostname,
                        ip_address=declared.primary_ip,
                        zone_name=dns_zone,
                    )
                except Exception as e:
                    logger.warning("Could not reconcile DNS for %s: %s", hostname, e)

    async def reconcile_cluster(self, cluster_id: Optional[int] = None, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Safety-Net Pass: Scans all workloads declared in NetBox for the managed cluster,
        evaluates live Proxmox state, and drives each workload into full alignment.
        """
        async with _cluster_reconciliation_lock:
            target_cluster_id = cluster_id or app_config.defaults.get("cluster_id")
            if not target_cluster_id:
                logger.warning("No cluster_id configured; skipping cluster reconciliation.")
                return {"status": "error", "message": "No cluster_id configured"}

            if not job_id:
                job_id = f"job_reconcile_cluster_{int(time.time())}"
                await db.create_job(
                    job_id=job_id,
                    action="cluster_reconciliation_pass",
                    metadata={"cluster_id": target_cluster_id},
                )

            await db.append_log(job_id, f"Starting periodic safety-net reconciliation for Cluster ID {target_cluster_id}...")

            # 1. Fetch all VMs in cluster from NetBox
            vms = await netbox_driver.get_cluster_virtual_machines(target_cluster_id)
            await db.append_log(job_id, f"Discovered {len(vms)} declared virtual workloads in NetBox Cluster #{target_cluster_id}.")

            results = []
            synced_count = 0
            reconciled_count = 0
            failed_count = 0

            for vm_data in vms:
                vm_id = vm_data.get("id")
                vm_name = vm_data.get("name", f"vm_{vm_id}")
                try:
                    res = await self.reconcile_workload(
                        netbox_vm_id=vm_id,
                        trigger="periodic_scan",
                        hint_payload=vm_data,
                    )
                    status = res.get("status")
                    if status == "in_sync":
                        synced_count += 1
                    else:
                        reconciled_count += 1
                    results.append({"vm_id": vm_id, "name": vm_name, "result": res})
                except Exception as exc:
                    failed_count += 1
                    logger.exception("Failed to reconcile VM %s (ID: %d): %s", vm_name, vm_id, exc)
                    await db.append_log(job_id, f"ERROR reconciling VM '{vm_name}' (#{vm_id}): {exc}")
                    results.append({"vm_id": vm_id, "name": vm_name, "error": str(exc)})

            summary = {
                "cluster_id": target_cluster_id,
                "total_workloads": len(vms),
                "in_sync": synced_count,
                "reconciled": reconciled_count,
                "failed": failed_count,
                "details": results,
            }
            await db.append_log(
                job_id,
                f"Cluster reconciliation pass complete: {synced_count} in sync, {reconciled_count} reconciled/actioned, {failed_count} errors.",
            )
            await db.update_job(job_id, status="completed", metadata=summary)
            return summary


reconciler = ReconciliationEngine()
