import json
import logging
from fastapi import APIRouter, Request, HTTPException, Header, status
from typing import Optional
from app.core.security import verify_netbox_signature
from app.workers.queue import job_queue

logger = logging.getLogger("orchestrator.api.webhooks")
router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


@router.post("/netbox", status_code=status.HTTP_202_ACCEPTED, summary="NetBox Event Webhook Receiver")
async def receive_netbox_webhook(
    request: Request,
    x_hook_signature: Optional[str] = Header(None, alias="X-Hook-Signature"),
):
    """
    Receives incoming webhook events from NetBox.
    Validates HMAC SHA-512 signature if NETBOX_WEBHOOK_SECRET is configured.
    Enqueues provisioning or sync task and immediately returns 202 Accepted.
    """
    raw_body = await request.body()

    if not verify_netbox_signature(raw_body, x_hook_signature):
        logger.warning("Rejected NetBox webhook: invalid HMAC signature")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid webhook signature",
        )

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse NetBox webhook JSON: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Malformed JSON payload: {e}",
        )

    job_id = await job_queue.enqueue_netbox_webhook(payload)

    return {
        "status": "queued",
        "job_id": job_id,
        "event": payload.get("event"),
        "model": payload.get("model"),
        "message": "NetBox event received and task enqueued successfully",
    }
