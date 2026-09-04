import logging
from fastapi import APIRouter, Depends
from app.core.security import require_api_key
from app.core.preflight import preflight_checker

logger = logging.getLogger("orchestrator.api.system")
router = APIRouter(prefix="/system", tags=["System Diagnostics"], dependencies=[Depends(require_api_key)])


@router.get("/preflight", summary="Run Complete Pre-Flight Health & Config Diagnostic")
async def run_preflight_check():
    """
    Validates secrets in .env, configuration in config.yml, and live
    connectivity to NetBox, Proxmox VE, and Uptime Kuma.
    """
    return await preflight_checker.run_all_checks()


@router.get("/proxmox-audit", summary="Run Proxmox VE Hypervisor & Cluster Audit")
async def run_proxmox_audit():
    """
    Audits Proxmox VE hypervisor node, storage pools, network bridges,
    and OS blueprint template inventory.
    """
    from app.scripts.audit_proxmox import proxmox_auditor
    return await proxmox_auditor.run_audit()
