"""
Uptime Kuma Driver for NPU Orchestrator.

Compatible with Uptime Kuma v1 and v2 (Conditions & Group hierarchy).
Manages real-time Socket.IO communication with Uptime Kuma for:
- Automatic Site group management (Lohusuu, Hulja, Oracle)
- Automated ICMP Ping monitor provisioning for NetBox infrastructure
- Automatic attachment of default notification channels (e.g. NPU Monitooring)
- Automatic reconciliation & deletion of monitors for devices with excluded tags (e.g. no-monitor)
- Single-session batch provisioning to eliminate rate limits
"""

import asyncio
import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, Tuple, List, Generator
from uptime_kuma_api import UptimeKumaApi, MonitorType, Event

from app.core.config import settings
from app.core.app_config import app_config

logger = logging.getLogger("orchestrator.uptime_kuma")


class UptimeKumaDriver:
    def __init__(self):
        self.url = settings.UPTIME_KUMA_URL
        self.username = settings.UPTIME_KUMA_USERNAME
        self.password = settings.UPTIME_KUMA_PASSWORD

    def _get_api(self) -> UptimeKumaApi:
        """Synchronous connection factory ensuring a valid, authenticated session with backoff retry."""
        if not self.username or not self.password:
            raise ValueError("UPTIME_KUMA_USERNAME and UPTIME_KUMA_PASSWORD must be configured in .env")
        
        for attempt in range(5):
            api = None
            try:
                api = UptimeKumaApi(self.url, timeout=30)
                api.login(self.username, self.password)
                return api
            except Exception as e:
                if api:
                    try:
                        api.disconnect()
                    except Exception:
                        pass
                if ("Too frequently" in str(e) or "Timeout" in type(e).__name__ or "timed out" in str(e).lower()) and attempt < 4:
                    wait_secs = 5 * (attempt + 1)
                    logger.warning("Uptime Kuma login attempt %d failed (%s), retrying in %ds...", attempt + 1, e, wait_secs)
                    time.sleep(wait_secs)
                    continue
                raise e

    @contextmanager
    def session(self) -> Generator[UptimeKumaApi, None, None]:
        """Context manager providing an authenticated session for single or batch operations."""
        api = self._get_api()
        try:
            yield api
        finally:
            try:
                api.disconnect()
            except Exception:
                pass

    def _add_monitor_safe(self, api: UptimeKumaApi, **kwargs) -> int:
        """
        Builds monitor payload with Uptime Kuma v2 compatibility (conditions='[]')
        and executes the creation call with Event.MONITOR_LIST.
        Includes automatic retry with backoff if rate-limited.
        """
        data = api._build_monitor_data(**kwargs)
        # Uptime Kuma v2 schema requires 'conditions' to be non-null
        data["conditions"] = "[]"

        for attempt in range(5):
            try:
                with api.wait_for_event(Event.MONITOR_LIST):
                    res = api._call("add", data)
                # Polite pacing between operations
                time.sleep(1.0)
                return res.get("monitorID")
            except Exception as e:
                if "Too frequently" in str(e) and attempt < 4:
                    wait_secs = 10 * (attempt + 1)
                    logger.warning("Uptime Kuma operation rate limited, waiting %ds...", wait_secs)
                    time.sleep(wait_secs)
                    continue
                raise e

    async def test_connection(self) -> Dict[str, Any]:
        """Tests authentication and returns monitor count."""
        def _test():
            with self.session() as api:
                monitors = api.get_monitors()
                return {
                    "status": "connected",
                    "url": self.url,
                    "total_monitors": len(monitors)
                }
        return await asyncio.to_thread(_test)

    async def get_or_create_group(self, name: str, api: Optional[UptimeKumaApi] = None) -> int:
        """Finds an existing GROUP monitor by name or creates it, returning its ID."""
        def _get_or_create(session_api: UptimeKumaApi):
            monitors = session_api.get_monitors()
            for m in monitors:
                if m.get("type") == MonitorType.GROUP and m.get("name", "").strip().lower() == name.strip().lower():
                    return m["id"]
            
            mid = self._add_monitor_safe(
                session_api,
                type=MonitorType.GROUP,
                name=name
            )
            logger.info("Created Uptime Kuma group '%s' (ID: %s)", name, mid)
            return mid

        if api:
            return _get_or_create(api)
        with self.session() as sess:
            return _get_or_create(sess)

    async def sync_devices_batch(
        self,
        devices: List[Dict[str, Any]],
        excluded_devices: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Batch synchronizes a list of devices using a single authenticated session.
        - Attaches default notifications if enable_default_notifications is true.
        - Deletes monitors for any devices in excluded_devices (e.g. tagged no-monitor).
        """
        def _batch_sync():
            with self.session() as api:
                monitors = api.get_monitors()
                
                # Fast lookups of existing monitors and groups
                existing_by_ip: Dict[str, int] = {}
                existing_by_name: Dict[str, int] = {}
                monitor_map: Dict[int, Dict[str, Any]] = {}
                groups: Dict[str, int] = {}

                for m in monitors:
                    mid = m["id"]
                    monitor_map[mid] = m
                    m_type = m.get("type")
                    m_name = (m.get("name") or "").strip()
                    m_host = (m.get("hostname") or "").strip().split('/')[0]

                    if m_type == MonitorType.GROUP:
                        groups[m_name.lower()] = mid
                    else:
                        if m_host:
                            existing_by_ip[m_host] = mid
                        if m_name:
                            existing_by_name[m_name.lower()] = mid

                cfg = app_config.uptime_kuma
                devices_cfg = cfg.get("devices", {})
                # Support new nested keys; fall back to legacy flat keys
                group_by_site = devices_cfg.get("group_by_site", cfg.get("group_by_site", True))
                enable_default_notifs = cfg.get("enable_default_notifications", True)
                ping_interval = devices_cfg.get("ping_interval", cfg.get("ping_interval", 60))
                ping_retry_interval = devices_cfg.get("ping_retry_interval", cfg.get("ping_retry_interval", 60))
                max_retries = devices_cfg.get("max_retries", cfg.get("max_retries", 3))

                # Resolve default notification channels
                default_notifs: Dict[int, bool] = {}
                if enable_default_notifs:
                    try:
                        all_notifs = api.get_notifications()
                        for n in all_notifs:
                            if n.get("isDefault"):
                                default_notifs[n["id"]] = True
                        if default_notifs:
                            logger.info("Found %d default notification channel(s): %s", len(default_notifs), list(default_notifs.keys()))
                    except Exception as e:
                        logger.warning("Could not fetch notifications from Uptime Kuma: %s", e)

                created_count = 0
                existing_count = 0
                deleted_count = 0
                error_count = 0
                details = []

                # PHASE 1: Reconcile Excluded Devices (Delete if exists in Kuma)
                if excluded_devices:
                    for ex in excluded_devices:
                        ex_name = ex["name"]
                        ex_ip = ex.get("hostname", "").strip().split('/')[0]
                        site = ex.get("site") or "Default"
                        mid = existing_by_ip.get(ex_ip) or existing_by_name.get(ex_name.lower())
                        
                        if mid:
                            try:
                                api._call("deleteMonitor", mid)
                                deleted_count += 1
                                logger.info("Deleted Uptime Kuma monitor '%s' (ID: %s) because device is tagged as excluded.", ex_name, mid)
                                details.append({
                                    "name": ex_name,
                                    "ip": ex_ip,
                                    "site": site,
                                    "monitor_id": mid,
                                    "status": "deleted_excluded"
                                })
                                if ex_ip in existing_by_ip:
                                    del existing_by_ip[ex_ip]
                                if ex_name.lower() in existing_by_name:
                                    del existing_by_name[ex_name.lower()]
                                time.sleep(0.6)
                            except Exception as e:
                                error_count += 1
                                logger.error("Failed to delete excluded monitor '%s' (ID: %s): %s", ex_name, mid, e)
                                details.append({
                                    "name": ex_name,
                                    "ip": ex_ip,
                                    "site": site,
                                    "error": str(e),
                                    "status": "error"
                                })

                # PHASE 2: Provision or Reconcile Monitored Devices
                for dev in devices:
                    name = dev["name"]
                    raw_ip = dev["hostname"]
                    clean_ip = raw_ip.strip().split('/')[0]
                    site = dev.get("site") or "Default"
                    role = dev.get("role") or "Device"
                    desc = dev.get("description") or f"Managed by NPU Orchestrator. Site: {site}, Role: {role}"

                    # Check if already monitored
                    existing_id = existing_by_ip.get(clean_ip) or existing_by_name.get(name.lower())
                    if existing_id:
                        existing_count += 1
                        
                        # Verify notifications on existing monitor
                        if default_notifs and existing_id in monitor_map:
                            m_obj = monitor_map[existing_id]
                            cur_notifs = m_obj.get("notificationIDList") or []
                            missing_nids = [nid for nid in default_notifs if nid not in cur_notifs]
                            if missing_nids:
                                try:
                                    m_edit = dict(m_obj)
                                    combined = {nid: True for nid in (cur_notifs + missing_nids)}
                                    m_edit["notificationIDList"] = combined
                                    m_edit["conditions"] = m_edit.get("conditions") or "[]"
                                    api._call("editMonitor", m_edit)
                                    logger.info("Attached missing default notification(s) %s to monitor '%s' (ID: %s)", missing_nids, name, existing_id)
                                    time.sleep(0.4)
                                except Exception as e:
                                    logger.warning("Failed updating notifications for monitor '%s': %s", name, e)

                        details.append({
                            "name": name,
                            "ip": clean_ip,
                            "site": site,
                            "monitor_id": existing_id,
                            "status": "existing"
                        })
                        continue

                    # Group resolution
                    parent_id = None
                    if group_by_site and site:
                        site_key = site.strip().lower()
                        if site_key in groups:
                            parent_id = groups[site_key]
                        else:
                            try:
                                parent_id = self._add_monitor_safe(
                                    api,
                                    type=MonitorType.GROUP,
                                    name=site
                                )
                                groups[site_key] = parent_id
                                logger.info("Created Uptime Kuma group '%s' (ID: %s)", site, parent_id)
                            except Exception as e:
                                logger.error("Failed to create group '%s': %s", site, e)

                    # Monitor creation
                    kwargs = {
                        "type": MonitorType.PING,
                        "name": name,
                        "hostname": clean_ip,
                        "interval": ping_interval,
                        "retryInterval": ping_retry_interval,
                        "maxretries": max_retries,
                        "description": desc
                    }
                    if parent_id:
                        kwargs["parent"] = parent_id
                    if default_notifs:
                        kwargs["notificationIDList"] = default_notifs

                    try:
                        mid = self._add_monitor_safe(api, **kwargs)
                        created_count += 1
                        existing_by_ip[clean_ip] = mid
                        existing_by_name[name.lower()] = mid
                        details.append({
                            "name": name,
                            "ip": clean_ip,
                            "site": site,
                            "monitor_id": mid,
                            "status": "created"
                        })
                        logger.info("Created Ping monitor '%s' (%s) in group '%s' (ID: %s) with notifications %s", name, clean_ip, site, mid, list(default_notifs.keys()))
                    except Exception as e:
                        error_count += 1
                        details.append({
                            "name": name,
                            "ip": clean_ip,
                            "site": site,
                            "error": str(e),
                            "status": "error"
                        })
                        logger.error("Failed to create monitor '%s' (%s): %s", name, clean_ip, e)

                return {
                    "status": "success" if error_count == 0 else "partial",
                    "total_evaluated": len(devices) + (len(excluded_devices) if excluded_devices else 0),
                    "created_count": created_count,
                    "existing_count": existing_count,
                    "deleted_count": deleted_count,
                    "error_count": error_count,
                    "details": details
                }

        return await asyncio.to_thread(_batch_sync)

    async def sync_ping_monitor(
        self,
        name: str,
        hostname: str,
        site: Optional[str] = None,
        role: Optional[str] = None,
        description: Optional[str] = None
    ) -> Tuple[int, bool]:
        """Ensures a single PING monitor exists."""
        res = await self.sync_devices_batch([{
            "name": name,
            "hostname": hostname,
            "site": site,
            "role": role,
            "description": description
        }])
        detail = res["details"][0]
        if detail.get("status") == "error":
            raise RuntimeError(detail.get("error"))
        return detail["monitor_id"], (detail.get("status") == "created")

    async def sync_services_batch(
        self,
        services: List[Dict[str, Any]],
        excluded_services: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Batch synchronizes HTTP monitors for application services (e.g. Traefik routes).
        - services: list of dicts with keys: name, url, description
        - excluded_services: services to remove from Kuma (tagged no-monitor)
        """
        def _batch_sync():
            with self.session() as api:
                monitors = api.get_monitors()

                existing_by_url: Dict[str, int] = {}
                existing_by_name: Dict[str, int] = {}
                monitor_map: Dict[int, Dict[str, Any]] = {}
                groups: Dict[str, int] = {}

                for m in monitors:
                    mid = m["id"]
                    monitor_map[mid] = m
                    m_type = m.get("type")
                    m_name = (m.get("name") or "").strip()
                    m_url = (m.get("url") or "").strip().rstrip("/")

                    if m_type == MonitorType.GROUP:
                        groups[m_name.lower()] = mid
                    else:
                        if m_url:
                            existing_by_url[m_url] = mid
                        if m_name:
                            existing_by_name[m_name.lower()] = mid

                cfg = app_config.uptime_kuma
                svc_cfg = cfg.get("services", {})
                group_name = svc_cfg.get("group_name", "Web Services")
                heartbeat_interval = svc_cfg.get("heartbeat_interval", 60)
                accepted_statuses = svc_cfg.get("accepted_statuses", [200, 301, 302])
                enable_default_notifs = cfg.get("enable_default_notifications", True)

                # Resolve default notification channels
                default_notifs: Dict[int, bool] = {}
                if enable_default_notifs:
                    try:
                        all_notifs = api.get_notifications()
                        for n in all_notifs:
                            if n.get("isDefault"):
                                default_notifs[n["id"]] = True
                    except Exception as e:
                        logger.warning("Could not fetch notifications from Uptime Kuma: %s", e)

                # Ensure group exists
                group_key = group_name.strip().lower()
                if group_key in groups:
                    parent_id = groups[group_key]
                else:
                    try:
                        parent_id = self._add_monitor_safe(api, type=MonitorType.GROUP, name=group_name)
                        groups[group_key] = parent_id
                        logger.info("Created Uptime Kuma group '%s' (ID: %s)", group_name, parent_id)
                    except Exception as e:
                        logger.error("Failed to create group '%s': %s", group_name, e)
                        parent_id = None

                created_count = 0
                existing_count = 0
                deleted_count = 0
                error_count = 0
                details = []

                # PHASE 1: Remove excluded services
                if excluded_services:
                    for ex in excluded_services:
                        ex_name = ex["name"]
                        ex_url = ex.get("url", "").rstrip("/")
                        mid = existing_by_url.get(ex_url) or existing_by_name.get(ex_name.lower())
                        if mid:
                            try:
                                api._call("deleteMonitor", mid)
                                deleted_count += 1
                                logger.info("Deleted HTTP monitor '%s' (excluded by tag)", ex_name)
                                details.append({"name": ex_name, "url": ex_url, "monitor_id": mid, "status": "deleted_excluded"})
                                existing_by_url.pop(ex_url, None)
                                existing_by_name.pop(ex_name.lower(), None)
                                time.sleep(0.6)
                            except Exception as e:
                                error_count += 1
                                details.append({"name": ex_name, "url": ex_url, "error": str(e), "status": "error"})

                # PHASE 2: Provision or reconcile services
                for svc in services:
                    name = svc["name"]
                    url = svc.get("url", "").rstrip("/")
                    desc = svc.get("description", f"Managed by NPU Orchestrator")

                    if not url:
                        logger.warning("Service '%s' has no public_url; skipping.", name)
                        details.append({"name": name, "url": "", "status": "skipped_no_url"})
                        continue

                    existing_id = existing_by_url.get(url) or existing_by_name.get(name.lower())
                    if existing_id:
                        existing_count += 1
                        details.append({"name": name, "url": url, "monitor_id": existing_id, "status": "existing"})
                        continue

                    kwargs = {
                        "type": MonitorType.HTTP,
                        "name": name,
                        "url": url,
                        "interval": heartbeat_interval,
                        "description": desc,
                        "accepted_statuscodes": [str(s) for s in accepted_statuses],
                    }
                    if parent_id:
                        kwargs["parent"] = parent_id
                    if default_notifs:
                        kwargs["notificationIDList"] = default_notifs

                    try:
                        mid = self._add_monitor_safe(api, **kwargs)
                        created_count += 1
                        existing_by_url[url] = mid
                        existing_by_name[name.lower()] = mid
                        details.append({"name": name, "url": url, "monitor_id": mid, "status": "created"})
                        logger.info("Created HTTP monitor '%s' -> %s (ID: %s)", name, url, mid)
                    except Exception as e:
                        error_count += 1
                        details.append({"name": name, "url": url, "error": str(e), "status": "error"})
                        logger.error("Failed to create HTTP monitor '%s': %s", name, e)

                return {
                    "status": "success" if error_count == 0 else "partial",
                    "total_evaluated": len(services) + (len(excluded_services) if excluded_services else 0),
                    "created_count": created_count,
                    "existing_count": existing_count,
                    "deleted_count": deleted_count,
                    "error_count": error_count,
                    "details": details,
                }

        return await asyncio.to_thread(_batch_sync)

    async def delete_monitor_by_hostname(self, hostname: str) -> bool:
        """Deletes a monitor matching the given hostname/IP."""
        def _del():
            with self.session() as api:
                clean_host = hostname.strip().split('/')[0]
                monitors = api.get_monitors()
                for m in monitors:
                    m_host = (m.get("hostname") or "").strip().split('/')[0]
                    if m_host == clean_host:
                        api._call("deleteMonitor", m["id"])
                        logger.info("Deleted monitor '%s' (ID: %s) for host %s", m["name"], m["id"], clean_host)
                        return True
                return False
        return await asyncio.to_thread(_del)


uptime_kuma_driver = UptimeKumaDriver()
