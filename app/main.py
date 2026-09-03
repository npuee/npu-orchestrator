import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.config import settings
from app.storage.db import db
from app.api.v1.webhooks import router as webhooks_router
from app.api.v1.provision import router as provision_router
from app.api.v1.templates import router as templates_router
from app.api.v1.jobs import router as jobs_router
from app.api.v1.sync import router as sync_router
from app.api.v1.system import router as system_router

import asyncio
from app.drivers.traefik_sync import traefik_sync_driver
from app.drivers.metrics_sync import metrics_sync_driver
from app.drivers.template_sync import template_sync_driver

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator.main")


async def orchestrator_background_reconciler_loop():
    """Background task that runs Traefik, Proxmox telemetry, and CT templates syncs periodically."""
    interval_seconds = settings.TRAEFIK_SYNC_INTERVAL_MINUTES * 60
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            if settings.TRAEFIK_SYNC_ENABLED:
                logger.info("Executing scheduled Traefik -> NetBox sync for all instances (every %d mins)...", settings.TRAEFIK_SYNC_INTERVAL_MINUTES)
                await traefik_sync_driver.sync_all_instances()

            logger.info("Executing scheduled Proxmox VM Telemetry -> NetBox sync...")
            await metrics_sync_driver.sync_metrics_to_netbox()

            logger.info("Executing scheduled Proxmox Templates -> NetBox Platforms sync...")
            await template_sync_driver.sync_all_templates()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Error during scheduled background reconciliation: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Database
    logger.info("Initializing %s database...", settings.APP_NAME)
    await db.init_db()
    
    # 1. Run initial Traefik sync on startup for all instances (Oracle + Lohusuu)
    if settings.TRAEFIK_SYNC_ENABLED:
        try:
            logger.info("Running initial Traefik -> NetBox sync on startup (Oracle + Lohusuu)...")
            await traefik_sync_driver.sync_all_instances()
            logger.info("Startup Traefik sync completed successfully for all instances.")
        except Exception as exc:
            logger.warning("Initial Traefik sync encountered an issue: %s", exc)

    # 2. Run initial Proxmox VM Telemetry sync on startup
    try:
        logger.info("Running initial Proxmox VM Telemetry -> NetBox sync on startup...")
        m_res = await metrics_sync_driver.sync_metrics_to_netbox()
        logger.info("Startup Proxmox Telemetry sync completed: %d VMs updated.", m_res.get("updated_count", 0))
    except Exception as exc:
        logger.warning("Initial Proxmox Telemetry sync encountered an issue: %s", exc)

    # 3. Run initial Proxmox CT Templates sync on startup
    try:
        logger.info("Running initial Proxmox Templates -> NetBox Platforms sync on startup...")
        t_res = await template_sync_driver.sync_all_templates()
        logger.info("Startup Platform sync completed: %s", t_res.get("summary"))
    except Exception as exc:
        logger.warning("Initial CT Template sync encountered an issue: %s", exc)

    # Launch background recurring loop
    sync_task = asyncio.create_task(orchestrator_background_reconciler_loop())

    logger.info("%s ready to accept requests.", settings.APP_NAME)
    yield
    # Shutdown
    logger.info("Shutting down %s...", settings.APP_NAME)
    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass
    await netbox_driver.close()


app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    description="Central Infrastructure Orchestrator (NetBox Webhooks, Proxmox VE Orchestration, Signal Alerts)",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Include API v1 Routers
app.include_router(webhooks_router, prefix="/api/v1")
app.include_router(provision_router, prefix="/api/v1")
app.include_router(templates_router, prefix="/api/v1")
app.include_router(jobs_router, prefix="/api/v1")
app.include_router(sync_router, prefix="/api/v1")
app.include_router(system_router, prefix="/api/v1")


@app.get("/", summary="Root Endpoint")
async def root():
    return {
        "service": settings.APP_NAME,
        "status": "online",
        "docs": "/docs",
        "endpoints": {
            "webhooks": "/api/v1/webhooks/netbox",
            "provision_linux": "/api/v1/provision/linux",
            "provision_windows": "/api/v1/provision/windows",
            "templates": "/api/v1/templates",
            "jobs": "/api/v1/jobs",
            "sync_traefik": "/api/v1/sync/traefik",
        },
    }


@app.get("/health", summary="Health Check")
async def health():
    return {"status": "ok", "service": settings.APP_NAME}


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
