import logging
from fastapi import APIRouter, Depends, status
from app.core.security import require_api_key
from app.models.schemas import LinuxProvisionRequest, WindowsProvisionRequest
from app.workers.queue import job_queue

logger = logging.getLogger("orchestrator.api.provision")
router = APIRouter(prefix="/provision", tags=["Provisioning"], dependencies=[Depends(require_api_key)])


@router.post(
    "/linux",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Provision a Linux VM (Ubuntu template clone with Cloud-Init)",
)
async def provision_linux_vm(payload: LinuxProvisionRequest):
    """
    Manually triggers provisioning of a Linux VM by cloning a template (ID 9000-9999),
    configuring network/cloud-init, disk resizing, ZFS cache tuning, and starting the VM.
    """
    job_id = await job_queue.enqueue_linux_provision(payload.model_dump())
    return {
        "status": "queued",
        "job_id": job_id,
        "hostname": payload.hostname,
        "action": "clone_linux",
        "message": f"Linux VM provisioning enqueued for '{payload.hostname}'",
    }


@router.post(
    "/windows",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Provision a Windows VM (Windows template clone with ConfigDrive2)",
)
async def provision_windows_vm(payload: WindowsProvisionRequest):
    """
    Manually triggers provisioning of a Windows Server VM by cloning a template (ID 9200-9299),
    configuring ConfigDrive2 cloud-init, hardware resources, Administrator password, and starting the VM.
    """
    job_id = await job_queue.enqueue_windows_provision(payload.model_dump())
    return {
        "status": "queued",
        "job_id": job_id,
        "hostname": payload.hostname,
        "action": "clone_windows",
        "message": f"Windows VM provisioning enqueued for '{payload.hostname}'",
    }
