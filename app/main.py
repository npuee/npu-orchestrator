import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app import __version__
from app.core.config import settings
from app.storage.db import db
from app.api.v1.webhooks import router as webhooks_router
from app.api.v1.provision import router as provision_router
from app.api.v1.templates import router as templates_router
from app.api.v1.jobs import router as jobs_router
from app.api.v1.sync import router as sync_router
from app.api.v1.system import router as system_router

import asyncio
import time
from app.core.app_config import app_config
from app.core.modules import module_manager
from app.drivers.netbox import netbox_driver
from app.drivers.proxmox import proxmox_driver
from app.drivers.traefik_sync import traefik_sync_driver
from app.drivers.metrics_sync import metrics_sync_driver
from app.drivers.template_sync import template_sync_driver

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator.main")


async def orchestrator_background_reconciler_loop():
    """Periodic background reconciler managing scheduled synchronization tasks."""
    logger.info("Background reconciliation loop started (heartbeat every 60s)...")
    last_traefik_sync = 0
    last_telemetry_sync = 0
    last_template_sync = 0
    last_kuma_sync = 0
    last_db_prune = 0

    while True:
        try:
            await asyncio.sleep(60)
            now = time.time()

            # 1. Traefik Ingress Synchronization (Optional Module)
            if module_manager.is_enabled("traefik"):
                traefik_cfg = app_config.traefik
                traefik_interval = traefik_cfg.get("sync_interval_minutes", 15) * 60
                if now - last_traefik_sync >= traefik_interval:
                    logger.info("Executing scheduled Traefik -> NetBox sync (every %d mins)...", traefik_cfg.get("sync_interval_minutes", 15))
                    try:
                        t_res = await traefik_sync_driver.sync_all_instances()
                        instances = [i.get("name") for i in traefik_cfg.get("instances", [])]
                        module_manager.set_module_status("traefik", "connected", {"instances": instances, "summary": t_res})
                    except Exception as e:
                        logger.warning("Scheduled Traefik sync encountered an error: %s", e)
                        module_manager.set_module_status("traefik", "error", error=str(e))
                    last_traefik_sync = now

            # 2. Proxmox Telemetry & Metrics Synchronization (Optional Module)
            if module_manager.is_enabled("telemetry"):
                telemetry_cfg = app_config.telemetry
                telemetry_interval = telemetry_cfg.get("sync_interval_minutes", 15) * 60
                if now - last_telemetry_sync >= telemetry_interval:
                    logger.info("Executing scheduled Proxmox VM Telemetry -> NetBox sync...")
                    try:
                        m_res = await metrics_sync_driver.sync_metrics_to_netbox()
                        module_manager.set_module_status("telemetry", "active", {"updated_vms": m_res.get("updated_count", 0)})
                    except Exception as e:
                        logger.warning("Scheduled Proxmox Telemetry sync encountered an error: %s", e)
                        module_manager.set_module_status("telemetry", "error", error=str(e))
                    last_telemetry_sync = now

            # 3. Proxmox Templates -> NetBox Platforms Synchronization (Optional Module)
            if module_manager.is_enabled("templates"):
                templates_cfg = app_config.templates
                template_interval = templates_cfg.get("sync_interval_minutes", 60) * 60
                if now - last_template_sync >= template_interval:
                    logger.info("Executing scheduled Proxmox Templates -> NetBox Platforms sync...")
                    try:
                        t_res = await template_sync_driver.sync_all_templates()
                        module_manager.set_module_status("templates", "active", {"summary": t_res.get("summary")})
                    except Exception as e:
                        logger.warning("Scheduled Template sync encountered an error: %s", e)
                        module_manager.set_module_status("templates", "error", error=str(e))
                    last_template_sync = now

            # 4. NetBox Inventory -> Uptime Kuma Monitoring Synchronization (Optional Module)
            if module_manager.is_enabled("uptime_kuma"):
                kuma_cfg = app_config.uptime_kuma
                kuma_interval = kuma_cfg.get("sync_interval_minutes", 30) * 60
                if now - last_kuma_sync >= kuma_interval:
                    logger.info("Executing scheduled NetBox -> Uptime Kuma Inventory sync (every %d mins)...", kuma_cfg.get("sync_interval_minutes", 30))
                    try:
                        from app.scripts.sync_kuma_inventory import run_sync
                        k_res = await run_sync()
                        module_manager.set_module_status("uptime_kuma", "connected", {
                            "monitored_devices": k_res.get("total_monitored", 0),
                            "url": settings.UPTIME_KUMA_URL,
                        })
                    except Exception as e:
                        logger.warning("Scheduled Uptime Kuma sync encountered an error: %s", e)
                        module_manager.set_module_status("uptime_kuma", "error", error=str(e))
                    last_kuma_sync = now

            # 5. Database Historical Job Retention Pruning (Core Maintenance)
            db_cfg = app_config.database
            prune_interval = db_cfg.get("prune_interval_hours", 24) * 3600
            if now - last_db_prune >= prune_interval:
                retention_days = db_cfg.get("retention_days", 30)
                try:
                    await db.prune_old_jobs(days=retention_days)
                except Exception as e:
                    logger.warning("Scheduled database pruning encountered an error: %s", e)
                last_db_prune = now

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Unexpected error in background reconciler: %s", e)


async def run_startup_syncs():
    """
    Executes initial synchronizations asynchronously in the background.
    Only loads and invokes modules that are explicitly enabled and configured.
    """
    await asyncio.sleep(1)
    logger.info("Evaluating module configuration for startup synchronizations...")

    # 0. Core Services Verification (NetBox & Proxmox)
    try:
        if netbox_driver.is_configured():
            client = netbox_driver._get_client()
            resp = await client.get(
                f"{netbox_driver.base_url}/api/status/",
                headers={"Authorization": f"Token {netbox_driver.token}", "Accept": "application/json"},
            )
            if resp.status_code == 200:
                nb_data = resp.json()
                module_manager.set_core_status("netbox", "connected", {
                    "url": netbox_driver.base_url,
                    "version": nb_data.get("netbox-version", "unknown"),
                })
            else:
                module_manager.set_core_status("netbox", "error", error=f"HTTP {resp.status_code}")
    except Exception as exc:
        module_manager.set_core_status("netbox", "error", error=str(exc))

    try:
        loop = asyncio.get_running_loop()
        def _check_pve():
            pve = proxmox_driver.get_client()
            ver = pve.version.get()
            nodes = [n.get("node") for n in pve.nodes.get()]
            return ver, nodes
        ver, nodes = await loop.run_in_executor(None, _check_pve)
        module_manager.set_core_status("proxmox", "connected", {
            "version": ver.get("release", "unknown"),
            "cluster_nodes": nodes,
        })
    except Exception as exc:
        module_manager.set_core_status("proxmox", "error", error=str(exc))

    # 1. Traefik Module
    if module_manager.is_enabled("traefik"):
        try:
            logger.info("Running background Traefik -> NetBox sync on startup...")
            t_res = await traefik_sync_driver.sync_all_instances()
            instances = [i.get("name") for i in app_config.traefik.get("instances", [])]
            module_manager.set_module_status("traefik", "connected", {"instances": instances, "summary": t_res})
            logger.info("Background Traefik startup sync completed.")
        except Exception as exc:
            logger.warning("Startup Traefik sync encountered an issue: %s", exc)
            module_manager.set_module_status("traefik", "error", error=str(exc))
    else:
        logger.info("Module 'traefik' is not configured or disabled; skipping startup load.")

    # 2. Proxmox Telemetry Module
    if module_manager.is_enabled("telemetry"):
        try:
            logger.info("Running background Proxmox VM Telemetry -> NetBox sync on startup...")
            m_res = await metrics_sync_driver.sync_metrics_to_netbox()
            module_manager.set_module_status("telemetry", "active", {"updated_vms": m_res.get("updated_count", 0)})
            logger.info("Background Proxmox Telemetry startup sync completed: %d VMs updated.", m_res.get("updated_count", 0))
        except Exception as exc:
            logger.warning("Startup Proxmox Telemetry sync encountered an issue: %s", exc)
            module_manager.set_module_status("telemetry", "error", error=str(exc))
    else:
        logger.info("Module 'telemetry' is disabled; skipping startup load.")

    # 3. Proxmox CT Templates Module
    if module_manager.is_enabled("templates"):
        try:
            logger.info("Running background Proxmox Templates -> NetBox Platforms sync on startup...")
            t_res = await template_sync_driver.sync_all_templates()
            module_manager.set_module_status("templates", "active", {"summary": t_res.get("summary")})
            logger.info("Background Platform startup sync completed: %s", t_res.get("summary"))
        except Exception as exc:
            logger.warning("Startup CT Template sync encountered an issue: %s", exc)
            module_manager.set_module_status("templates", "error", error=str(exc))
    else:
        logger.info("Module 'templates' is disabled; skipping startup load.")

    # 4. NetBox Inventory -> Uptime Kuma Module
    if module_manager.is_enabled("uptime_kuma") and app_config.uptime_kuma.get("sync_on_startup", True):
        try:
            logger.info("Running background NetBox -> Uptime Kuma inventory sync on startup...")
            from app.scripts.sync_kuma_inventory import run_sync
            k_res = await run_sync()
            module_manager.set_module_status("uptime_kuma", "connected", {
                "monitored_devices": k_res.get("total_monitored", 0),
                "url": settings.UPTIME_KUMA_URL,
            })
            logger.info("Background Uptime Kuma startup sync completed: %d newly provisioned, %d existing.", k_res.get("created_count", 0), k_res.get("existing_count", 0))
        except Exception as exc:
            logger.warning("Startup Uptime Kuma sync encountered an issue: %s", exc)
            module_manager.set_module_status("uptime_kuma", "error", error=str(exc))
    else:
        logger.info("Module 'uptime_kuma' is not configured or disabled; skipping startup load.")

    logger.info("Background startup synchronizations completed.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Database & Recover any orphaned jobs
    logger.info("Initializing %s database...", settings.APP_NAME)
    await db.init_db()
    await db.recover_orphaned_jobs()

    # Register initial core services status
    module_manager.set_core_status("netbox", "ready", {"url": settings.NETBOX_URL})
    module_manager.set_core_status("proxmox", "ready", {"host": f"{settings.PROXMOX_HOST}:{settings.PROXMOX_PORT}"})

    # Launch background startup synchronizations (non-blocking)
    startup_task = asyncio.create_task(run_startup_syncs())

    # Launch background recurring loop
    reconciler_task = asyncio.create_task(orchestrator_background_reconciler_loop())

    logger.info("%s ready to accept requests.", settings.APP_NAME)
    yield

    # Shutdown
    logger.info("Shutting down %s...", settings.APP_NAME)
    startup_task.cancel()
    reconciler_task.cancel()
    try:
        await asyncio.gather(startup_task, reconciler_task, return_exceptions=True)
    except Exception:
        pass
    await netbox_driver.close()
    await db.close()


app = FastAPI(
    title=settings.APP_NAME,
    version=__version__,
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
        "version": __version__,
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


@app.get("/health", summary="Health & Module Diagnostics")
async def health():
    """
    Returns an instantaneous health and module status diagnostic report.
    Reports connectivity of Core services (NetBox, Proxmox) and all optional
    integration modules (Uptime Kuma, Traefik, DNS, Telemetry, Signal).
    """
    return module_manager.get_health_report()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
