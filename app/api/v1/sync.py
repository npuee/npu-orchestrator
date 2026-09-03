import logging
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, Depends
from app.core.security import require_api_key
from app.core.app_config import app_config
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
    try:
        instances = app_config.traefik.get("instances", [])
        results = {}
        for inst in instances:
            name = inst.get("name", "unknown")
            vm_id = inst.get("netbox_vm_id")
            inst_type = inst.get("type", "docker")
            if inst_type == "api":
                routes = await traefik_sync_driver.discover_remote_traefik_routes(inst.get("api_url"))
            else:
                routes = await traefik_sync_driver.get_oracle_routes(path=inst.get("path"), conf_dir=inst.get("conf_dir"))
            results[name] = {
                "target_vm_id": vm_id,
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
    netbox_vm_id: Optional[int] = Query(None, description="Specific NetBox VM ID. If omitted, syncs all instances configured in config.yml."),
):
    """
    Synchronizes discovered Traefik routes into NetBox as Application Services.
    If netbox_vm_id is omitted, automatically syncs all instances configured in config.yml.
    """
    try:
        if netbox_vm_id:
            found = None
            for inst in app_config.traefik.get("instances", []):
                if inst.get("netbox_vm_id") == netbox_vm_id:
                    found = inst
                    break
            name = found.get("name", f"traefik-vm-{netbox_vm_id}") if found else f"traefik-vm-{netbox_vm_id}"
            return await traefik_sync_driver.sync_instance(name, netbox_vm_id=netbox_vm_id, instance_conf=found)
        else:
            return await traefik_sync_driver.sync_all_instances()
    except Exception as e:
        logger.exception("Error synchronizing Traefik routes to NetBox: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", summary="Preview Proxmox VM Telemetry")
async def preview_proxmox_metrics():
    """
    Queries Proxmox VE cluster resources and returns live telemetry (CPU %, Memory, Disk, Uptime)
    for all active VMs and LXC Containers without modifying NetBox.
    """
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
    try:
        return await metrics_sync_driver.sync_metrics_to_netbox()
    except Exception as e:
        logger.exception("Error synchronizing Proxmox metrics to NetBox: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/ct", summary="Preview Discovered Proxmox CT Templates")
async def preview_ct_templates(
    node: Optional[str] = Query(None, description="Proxmox node name (default: cluster default)"),
):
    """
    Scans Proxmox storage pools for LXC System Container templates (vztmpl)
    and returns discovered templates with parsed OS and NetBox blueprint metadata.
    """
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
    try:
        return await template_sync_driver.sync_all_templates(node)
    except Exception as e:
        logger.exception("Error synchronizing Proxmox templates to NetBox Platforms: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/templates/ct", summary="Synchronize Proxmox Templates (Legacy Alias)")
async def sync_ct_templates_to_netbox(
    node: Optional[str] = Query(None, description="Proxmox node name (default: cluster default)"),
):
    """Alias for /api/v1/sync/platforms"""
    try:
        return await template_sync_driver.sync_all_templates(node)
    except Exception as e:
        logger.exception("Error synchronizing Proxmox templates: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

