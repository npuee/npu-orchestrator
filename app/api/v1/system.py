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
    connectivity to NetBox and Proxmox VE. Returns a detailed report.
    """
    return await preflight_checker.run_all_checks()
