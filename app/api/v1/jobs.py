import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from app.core.security import require_api_key
from app.storage.db import db
from app.models.schemas import JobResponse

logger = logging.getLogger("orchestrator.api.jobs")
router = APIRouter(prefix="/jobs", tags=["Jobs & History"], dependencies=[Depends(require_api_key)])


@router.get("", response_model=List[JobResponse], summary="List recent orchestrator jobs")
async def list_jobs(
    limit: int = Query(50, ge=1, le=200, description="Max jobs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """Returns a list of recent orchestrator jobs and their current execution status."""
    return await db.list_jobs(limit=limit, offset=offset)


@router.get("/{job_id}", response_model=JobResponse, summary="Get details and logs for a specific job")
async def get_job(job_id: str):
    """Retrieves full job details, execution timeline, and step logs."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job


@router.post("/prune", summary="Manually prune historical jobs older than retention window")
async def prune_historical_jobs(
    days: int = Query(30, ge=1, le=365, description="Retention window in days (jobs older than this will be deleted)")
):
    """Prunes completed, failed, or interrupted jobs older than the specified days."""
    deleted_count = await db.prune_old_jobs(days=days)
    return {
        "status": "success",
        "deleted_count": deleted_count,
        "retention_days": days,
    }
