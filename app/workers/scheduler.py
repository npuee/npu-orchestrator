import asyncio
import logging
import time
from typing import Callable, Awaitable, Optional, Dict, Any, List

from app.core.config import settings
from app.core.app_config import app_config
from app.core.modules import module_manager
from app.storage.db import db

logger = logging.getLogger("orchestrator.scheduler")


class PeriodicTask:
    """
    An independent, isolated background task runner.
    Guarantees that errors, long network operations, or failures in one task
    do not block or crash any other tasks.
    """

    def __init__(
        self,
        name: str,
        task_func: Callable[[], Awaitable[Any]],
        interval_getter: Callable[[], float],
        enabled_getter: Callable[[], bool],
        timeout_seconds: float = 300.0,
        initial_delay: float = 0.0,
    ):
        self.name = name
        self.task_func = task_func
        self.interval_getter = interval_getter
        self.enabled_getter = enabled_getter
        self.timeout_seconds = timeout_seconds
        self.initial_delay = initial_delay

        self.last_run: Optional[float] = None
        self.last_duration: Optional[float] = None
        self.last_status: str = "initialized"
        self.last_error: Optional[str] = None
        self.run_count: int = 0

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def _run_loop(self):
        # Initial delay before entering regular loop
        if self.initial_delay > 0:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.initial_delay)
                return
            except asyncio.TimeoutError:
                pass

        while not self._stop_event.is_set():
            if not self.enabled_getter():
                self.last_status = "disabled"
                # Check again after 30s if module gets enabled
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass
                continue

            # Execute task with timeout guard
            t_start = time.time()
            self.last_run = t_start
            self.last_status = "running"
            self.run_count += 1

            try:
                logger.debug("[%s] Executing periodic task...", self.name)
                await asyncio.wait_for(self.task_func(), timeout=self.timeout_seconds)
                self.last_duration = round(time.time() - t_start, 2)
                self.last_status = "success"
                self.last_error = None
                logger.debug("[%s] Periodic task completed in %.2fs", self.name, self.last_duration)
            except asyncio.CancelledError:
                self.last_status = "cancelled"
                break
            except asyncio.TimeoutError:
                self.last_duration = round(time.time() - t_start, 2)
                self.last_status = "timeout"
                self.last_error = f"Timed out after {self.timeout_seconds}s"
                logger.warning("[%s] Task timed out after %ds", self.name, self.timeout_seconds)
            except Exception as exc:
                self.last_duration = round(time.time() - t_start, 2)
                self.last_status = "error"
                self.last_error = str(exc)
                logger.warning("[%s] Task encountered error: %s", self.name, exc)

            # Sleep for interval (interruptible by stop_event)
            try:
                interval = max(5.0, float(self.interval_getter()))
            except Exception:
                interval = 60.0

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    def start(self):
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run_loop(), name=f"scheduler-{self.name}")
            logger.info("Started background runner for '%s'", self.name)

    async def stop(self):
        if self._task and not self._task.done():
            self._stop_event.set()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("Stopped background runner for '%s'", self.name)

    def get_status(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.last_status,
            "enabled": self.enabled_getter(),
            "run_count": self.run_count,
            "last_run": self.last_run,
            "last_duration_seconds": self.last_duration,
            "last_error": self.last_error,
        }


class BackgroundScheduler:
    """
    Central scheduler managing all decoupled asynchronous background loops.
    """

    def __init__(self):
        self.tasks: List[PeriodicTask] = []
        self._initialized = False

    def _build_tasks(self):
        if self._initialized:
            return

        # 1. Traefik Ingress Sync
        async def _run_traefik():
            from app.drivers.traefik_sync import traefik_sync_driver
            t_res = await traefik_sync_driver.sync_all_instances()
            instances = [i.get("name") for i in app_config.traefik.get("instances", [])]
            module_manager.set_module_status("traefik", "connected", {"instances": instances, "summary": t_res})

        self.tasks.append(
            PeriodicTask(
                name="traefik_sync",
                task_func=_run_traefik,
                interval_getter=lambda: app_config.traefik.get("sync_interval_minutes", 15) * 60,
                enabled_getter=lambda: module_manager.is_enabled("traefik"),
                timeout_seconds=120.0,
                initial_delay=60.0,
            )
        )

        # 2. Proxmox Telemetry & Metrics Sync
        async def _run_telemetry():
            from app.drivers.metrics_sync import metrics_sync_driver
            m_res = await metrics_sync_driver.sync_metrics_to_netbox()
            module_manager.set_module_status("telemetry", "active", {"updated_vms": m_res.get("updated_count", 0)})

        self.tasks.append(
            PeriodicTask(
                name="telemetry_sync",
                task_func=_run_telemetry,
                interval_getter=lambda: app_config.telemetry.get("sync_interval_minutes", 15) * 60,
                enabled_getter=lambda: module_manager.is_enabled("telemetry"),
                timeout_seconds=180.0,
                initial_delay=60.0,
            )
        )

        # 3. Proxmox Templates -> NetBox Platforms Sync
        async def _run_templates():
            from app.drivers.template_sync import template_sync_driver
            t_res = await template_sync_driver.sync_all_templates()
            module_manager.set_module_status("templates", "active", {"summary": t_res.get("summary")})

        self.tasks.append(
            PeriodicTask(
                name="templates_sync",
                task_func=_run_templates,
                interval_getter=lambda: app_config.templates.get("sync_interval_minutes", 60) * 60,
                enabled_getter=lambda: module_manager.is_enabled("templates"),
                timeout_seconds=180.0,
                initial_delay=60.0,
            )
        )

        # 4. NetBox Inventory -> Uptime Kuma Device Ping Sync
        async def _run_kuma_devices():
            from app.scripts.sync_kuma_inventory import run_sync
            k_res = await run_sync()
            module_manager.set_module_status("uptime_kuma", "connected", {
                "monitored_devices": k_res.get("total_monitored", 0),
                "url": settings.UPTIME_KUMA_URL,
            })

        self.tasks.append(
            PeriodicTask(
                name="kuma_device_sync",
                task_func=_run_kuma_devices,
                interval_getter=lambda: (
                    app_config.uptime_kuma.get("devices", {}).get(
                        "sync_interval_minutes",
                        app_config.uptime_kuma.get("sync_interval_minutes", 30)
                    ) * 60
                ),
                enabled_getter=lambda: module_manager.is_enabled("uptime_kuma"),
                timeout_seconds=180.0,
                initial_delay=60.0,
            )
        )

        # 5. NetBox Services -> Uptime Kuma HTTP Services Sync
        async def _run_kuma_services():
            from app.scripts.sync_kuma_services import run_sync as run_services_sync
            await run_services_sync()

        self.tasks.append(
            PeriodicTask(
                name="kuma_services_sync",
                task_func=_run_kuma_services,
                interval_getter=lambda: app_config.uptime_kuma.get("services", {}).get("sync_interval_minutes", 15) * 60,
                enabled_getter=lambda: (
                    module_manager.is_enabled("uptime_kuma")
                    and app_config.uptime_kuma.get("services", {}).get("enabled", False)
                ),
                timeout_seconds=180.0,
                initial_delay=75.0,
            )
        )

        # 6. Database Historical Job Retention Pruning
        async def _run_db_pruning():
            retention_days = app_config.database.get("retention_days", 30)
            await db.prune_old_jobs(days=retention_days)

        self.tasks.append(
            PeriodicTask(
                name="database_prune",
                task_func=_run_db_pruning,
                interval_getter=lambda: app_config.database.get("prune_interval_hours", 24) * 3600,
                enabled_getter=lambda: True,
                timeout_seconds=60.0,
                initial_delay=120.0,
            )
        )

        self._initialized = True

    def start(self):
        """Starts all periodic tasks."""
        self._build_tasks()
        logger.info("Starting background scheduler with %d decoupled task runners...", len(self.tasks))
        for t in self.tasks:
            t.start()

    async def stop(self):
        """Stops all periodic tasks."""
        logger.info("Stopping background scheduler...")
        stop_futs = [t.stop() for t in self.tasks]
        if stop_futs:
            await asyncio.gather(*stop_futs, return_exceptions=True)
        logger.info("All background scheduler tasks stopped.")

    def get_status(self) -> Dict[str, Any]:
        """Returns the current state and execution history of all background runners."""
        return {
            "total_runners": len(self.tasks),
            "runners": [t.get_status() for t in self.tasks],
        }


scheduler = BackgroundScheduler()
