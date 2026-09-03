import uuid
import asyncio
import logging
from typing import Dict, Any, Optional, Set
from app.storage.db import db
from app.workers.provisioning import (
    run_linux_provision_task,
    run_windows_provision_task,
)
from app.workers.dispatcher import process_netbox_webhook_event

logger = logging.getLogger("orchestrator.queue")


class JobQueue:
    def __init__(self):
        self._running_tasks: Set[asyncio.Task] = set()

    @staticmethod
    def generate_job_id() -> str:
        return f"job_{uuid.uuid4().hex[:12]}"

    def _track_task(self, task: asyncio.Task):
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    async def enqueue_linux_provision(self, params: Dict[str, Any]) -> str:
        job_id = self.generate_job_id()
        await db.create_job(
            job_id=job_id,
            action="clone_linux",
            metadata=params,
            hostname=params.get("hostname"),
            ip_address=params.get("ip_address"),
            vmid=params.get("vmid"),
        )
        task = asyncio.create_task(run_linux_provision_task(job_id, params))
        self._track_task(task)
        logger.info("Enqueued Linux provision job %s for %s", job_id, params.get("hostname"))
        return job_id

    async def enqueue_windows_provision(self, params: Dict[str, Any]) -> str:
        job_id = self.generate_job_id()
        safe_meta = dict(params)
        if "admin_password" in safe_meta:
            safe_meta["admin_password"] = "********"

        await db.create_job(
            job_id=job_id,
            action="clone_windows",
            metadata=safe_meta,
            hostname=params.get("hostname"),
            ip_address=params.get("ip_address"),
            vmid=params.get("vmid"),
        )
        task = asyncio.create_task(run_windows_provision_task(job_id, params))
        self._track_task(task)
        logger.info("Enqueued Windows provision job %s for %s", job_id, params.get("hostname"))
        return job_id

    async def enqueue_netbox_webhook(self, payload: Dict[str, Any]) -> str:
        job_id = self.generate_job_id()
        event_name = payload.get("event", "unknown")
        model_name = payload.get("model", "unknown")
        
        data = payload.get("data", {})
        hostname = data.get("name") if isinstance(data, dict) else None

        await db.create_job(
            job_id=job_id,
            action=f"netbox_{model_name}_{event_name}",
            metadata={"event": event_name, "model": model_name},
            hostname=hostname,
        )
        task = asyncio.create_task(process_netbox_webhook_event(job_id, payload))
        self._track_task(task)
        logger.info("Enqueued NetBox webhook job %s (event=%s, model=%s)", job_id, event_name, model_name)
        return job_id


job_queue = JobQueue()
