import logging
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, Depends
from app.core.security import require_api_key
from app.core.app_config import app_config
from app.core.modules import module_manager
from app.drivers.traefik_sync import traefik_sync_driver
from app.drivers.metrics_sync import metrics_sync_driver
from app.drivers.template_sync import template_sync_driver

logger = logging.getLogger("orchestrator.api.sync")
router = APIRouter(prefix="/sync", tags=["Infrastructure Sync"], dependencies=[Depends(require_api_key)])


@router.get("/traefik", summary="Preview Discovered Traefik Routes")
async def preview_traefik_routes():
    """
    Scans configured Traefik instances from config.yml
    and returns all discovered ingress routes without modifying NetBox.
    """
    if not module_manager.is_enabled("traefik"):
        return {"status": "disabled", "message": "Traefik module is disabled or not configured in config.yml"}

    try:
        instances = app_config.traefik.get("instances", [])
        results = {}
        for inst in instances:
            name = inst.get("name", "unknown")
            vm_id = inst.get("netbox_vm_id")
            device_id = inst.get("netbox_device_id")
            inst_type = inst.get("type", "docker")
            if inst_type == "api":
                routes = await traefik_sync_driver.discover_remote_traefik_routes(inst.get("api_url"))
            else:
                routes = await traefik_sync_driver.get_oracle_routes(path=inst.get("path"), conf_dir=inst.get("conf_dir"))
            results[name] = {
                "target_vm_id": vm_id,
                "target_device_id": device_id,
                "parent_type": "dcim.device" if device_id else "virtualization.virtualmachine",
                "parent_id": device_id if device_id else vm_id,
                "type": inst_type,
                "total_routes": len(routes),
                "routes": routes,
            }
        return results
    except Exception as e:
        logger.exception("Error previewing Traefik routes: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/traefik", summary="Synchronize Traefik Ingress Routes to NetBox")
async def sync_traefik_routes_to_netbox(
    netbox_vm_id: Optional[int] = Query(None, description="Specific NetBox VM ID (virtualization.virtualmachine). If omitted, syncs all configured instances."),
    netbox_device_id: Optional[int] = Query(None, description="Specific NetBox Device ID (dcim.device). If omitted, syncs all configured instances."),
):
    """
    Synchronizes discovered Traefik routes into NetBox as Application Services.
    Supports attaching to Virtual Machines (netbox_vm_id) or Bare-Metal Devices (netbox_device_id).
    If both IDs are omitted, automatically syncs all instances configured in config.yml.
    """
    if not module_manager.is_enabled("traefik"):
        return {"status": "disabled", "message": "Traefik module is disabled or not configured in config.yml"}

    try:
        if netbox_vm_id or netbox_device_id:
            found = None
            for inst in app_config.traefik.get("instances", []):
                if netbox_device_id and inst.get("netbox_device_id") == netbox_device_id:
                    found = inst
                    break
                if netbox_vm_id and inst.get("netbox_vm_id") == netbox_vm_id:
                    found = inst
                    break
            name = found.get("name") if found else (f"traefik-device-{netbox_device_id}" if netbox_device_id else f"traefik-vm-{netbox_vm_id}")
            res = await traefik_sync_driver.sync_instance(
                name,
                netbox_vm_id=netbox_vm_id,
                netbox_device_id=netbox_device_id,
                instance_conf=found,
            )
        else:
            res = await traefik_sync_driver.sync_all_instances()

        instances = [i.get("name") for i in app_config.traefik.get("instances", [])]
        module_manager.set_module_status("traefik", "connected", {"instances": instances, "summary": res})
        return res
    except Exception as e:
        logger.exception("Error synchronizing Traefik routes to NetBox: %s", e)
        module_manager.set_module_status("traefik", "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", summary="Preview Proxmox VM Telemetry")
async def preview_proxmox_metrics():
    """
    Queries Proxmox VE cluster resources and returns live telemetry (CPU %, Memory, Disk, Uptime)
    for all active VMs and LXC Containers without modifying NetBox.
    """
    if not module_manager.is_enabled("telemetry"):
        return {"status": "disabled", "message": "Telemetry module is disabled in config.yml"}

    try:
        items = metrics_sync_driver.fetch_proxmox_telemetry()
        return {
            "total_vms": len(items),
            "telemetry": items,
        }
    except Exception as e:
        logger.exception("Error previewing Proxmox metrics: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/metrics", summary="Synchronize Proxmox VM Telemetry to NetBox")
async def sync_proxmox_metrics_to_netbox():
    """
    Fetches live CPU, Memory, Disk, and Uptime metrics from Proxmox VE cluster
    and updates NetBox Virtual Machine custom fields and power states.
    """
    if not module_manager.is_enabled("telemetry"):
        return {"status": "disabled", "message": "Telemetry module is disabled in config.yml"}

    try:
        res = await metrics_sync_driver.sync_metrics_to_netbox()
        module_manager.set_module_status("telemetry", "active", {"updated_vms": res.get("updated_count", 0)})
        return res
    except Exception as e:
        logger.exception("Error synchronizing Proxmox metrics to NetBox: %s", e)
        module_manager.set_module_status("telemetry", "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/ct", summary="Preview Discovered Proxmox CT Templates")
async def preview_ct_templates(
    node: Optional[str] = Query(None, description="Proxmox node name (default: cluster default)"),
):
    """
    Scans Proxmox storage pools for LXC System Container templates (vztmpl)
    and returns discovered templates with parsed OS and NetBox blueprint metadata.
    """
    if not module_manager.is_enabled("templates"):
        return {"status": "disabled", "message": "Templates module is disabled in config.yml"}

    try:
        discovered = template_sync_driver.discover_ct_templates(node)
        return {
            "total_templates": len(discovered),
            "templates": discovered,
        }
    except Exception as e:
        logger.exception("Error previewing Proxmox CT templates: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/platforms", summary="Synchronize All Proxmox Templates (VMs & CTs) to NetBox Platforms")
async def sync_platforms_to_netbox(
    node: Optional[str] = Query(None, description="Proxmox node name (default: cluster default)"),
):
    """
    Discovers all QEMU VM templates and LXC Container templates on Proxmox,
    creates/updates corresponding Platforms in NetBox, and reconciles/deletes orphaned platforms.
    """
    if not module_manager.is_enabled("templates"):
        return {"status": "disabled", "message": "Templates module is disabled in config.yml"}

    try:
        res = await template_sync_driver.sync_all_templates(node)
        module_manager.set_module_status("templates", "active", {"summary": res.get("summary")})
        return res
    except Exception as e:
        logger.exception("Error synchronizing Proxmox templates to NetBox Platforms: %s", e)
        module_manager.set_module_status("templates", "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/templates/ct", summary="Synchronize Proxmox Templates (Legacy Alias)")
async def sync_ct_templates_to_netbox(
    node: Optional[str] = Query(None, description="Proxmox node name (default: cluster default)"),
):
    """Alias for /api/v1/sync/platforms"""
    return await sync_platforms_to_netbox(node=node)


@router.get("/uptime-kuma", summary="Preview NetBox Devices for Uptime Kuma Monitoring")
async def preview_uptime_kuma_sync():
    """
    Scans NetBox DCIM for all physical devices with a Primary IPv4 address
    and checks their existence status in Uptime Kuma without making modifications.
    """
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled in config.yml or missing credentials in .env"}

    try:
        from app.scripts.sync_kuma_inventory import preview_sync
        return await preview_sync()
    except Exception as e:
        logger.exception("Error previewing Uptime Kuma sync: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/uptime-kuma", summary="Synchronize NetBox Static Inventory to Uptime Kuma")
async def sync_netbox_to_uptime_kuma():
    """
    Scans NetBox DCIM for all physical devices with a Primary IPv4 address
    and auto-provisions or reconciles ICMP Ping monitors in Uptime Kuma under Site groups.
    """
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled in config.yml or missing credentials in .env"}

    try:
        from app.scripts.sync_kuma_inventory import run_sync
        res = await run_sync()
        from app.core.config import settings
        module_manager.set_module_status("uptime_kuma", "connected", {
            "monitored_devices": res.get("total_monitored", 0),
            "url": settings.UPTIME_KUMA_URL,
        })
        return res
    except Exception as e:
        logger.exception("Error synchronizing NetBox to Uptime Kuma: %s", e)
        module_manager.set_module_status("uptime_kuma", "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/uptime-kuma/services", summary="Preview NetBox Application Services for Uptime Kuma HTTP Monitoring")
async def preview_uptime_kuma_services_sync():
    """
    Scans NetBox Application Services tagged with the configured source_tag
    and previews which HTTP monitors would be created/deleted — no modifications made.
    """
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled in config.yml or missing credentials in .env"}

    try:
        from app.scripts.sync_kuma_services import preview_sync
        return await preview_sync()
    except Exception as e:
        logger.exception("Error previewing Uptime Kuma services sync: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/uptime-kuma/services", summary="Synchronize NetBox Application Services to Uptime Kuma HTTP Monitors")
async def sync_services_to_uptime_kuma():
    """
    Fetches NetBox Application Services tagged with the configured source_tag
    and provisions HTTP monitors in Uptime Kuma under the configured group.
    Services tagged with exclude_tag (e.g. no-monitor) are skipped or removed.
    Requires uptime_kuma.services.enabled: true in config.yml.
    """
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled in config.yml or missing credentials in .env"}

    try:
        from app.scripts.sync_kuma_services import run_sync
        from app.core.config import settings
        res = await run_sync()
        if res.get("status") != "disabled":
            module_manager.set_module_status("uptime_kuma", "connected", {
                "monitored_services": res.get("created_count", 0) + res.get("existing_count", 0),
                "url": settings.UPTIME_KUMA_URL,
            })
        return res
    except Exception as e:
        logger.exception("Error synchronizing services to Uptime Kuma: %s", e)
        module_manager.set_module_status("uptime_kuma", "error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
