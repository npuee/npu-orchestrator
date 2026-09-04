"""
NetBox Application Services -> Uptime Kuma HTTP Monitor Synchronization.

Fetches NetBox ipam.service records tagged with the configured source_tag:
- Provisions HTTP monitors under the configured group_name.
- Skips services tagged with exclude_tag (e.g. no-monitor).
- Removes HTTP monitors for services that have been excluded.
- Uses public_url custom field as the monitor URL.
"""

import asyncio
import logging
from typing import List, Dict, Any, Tuple

from app.core.app_config import app_config
from app.core.modules import module_manager
from app.drivers.netbox import netbox_driver
from app.drivers.uptime_kuma import uptime_kuma_driver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("sync_kuma_services")

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
MAGENTA = "\033[95m"
RESET = "\033[0m"


def _get_services_cfg() -> Dict[str, Any]:
    return app_config.uptime_kuma.get("services", {})


async def fetch_services_partitioned() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetches NetBox application services tagged with source_tag and partitions
    them into (monitored_services, excluded_services).
    """
    svc_cfg = _get_services_cfg()
    source_tag = svc_cfg.get("source_tag", "traefik").strip().lower()
    exclude_tag = svc_cfg.get("exclude_tag", "no-monitor").strip().lower()

    # Read the configured public_url custom field name
    traefik_cfg = app_config.traefik if app_config else {}
    field_public_url = (
        traefik_cfg.get("service_fields", {}).get("public_url")
        or traefik_cfg.get("custom_fields", {}).get("public_url", "public_url")
    )

    client = netbox_driver._get_client()
    headers = {"Authorization": f"Token {netbox_driver.token}", "Accept": "application/json"}

    # Filter by source_tag for efficiency
    url = f"{netbox_driver.base_url}/api/ipam/services/?tag={source_tag}&limit=500"
    resp = await client.get(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"NetBox API returned HTTP {resp.status_code}: {resp.text}")

    services = resp.json().get("results", [])

    monitored = []
    excluded = []

    for svc in services:
        cf = svc.get("custom_fields") or {}
        public_url = cf.get(field_public_url, "")
        name = svc.get("name", "")

        tag_slugs = {t.get("slug", "").lower() for t in svc.get("tags", [])}
        tag_names = {t.get("name", "").lower() for t in svc.get("tags", [])}
        all_tags = tag_slugs | tag_names

        entry = {
            "id": svc["id"],
            "name": name,
            "url": public_url,
            "tags": list(all_tags),
            "description": svc.get("description", f"NPU Orchestrator: {name}"),
        }

        if exclude_tag in all_tags:
            excluded.append(entry)
        else:
            monitored.append(entry)

    monitored.sort(key=lambda x: x["name"])
    excluded.sort(key=lambda x: x["name"])
    return monitored, excluded


async def preview_sync() -> Dict[str, Any]:
    """Read-only preview of what the sync would do."""
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled"}

    svc_cfg = _get_services_cfg()
    if not svc_cfg.get("enabled", False):
        return {"status": "disabled", "message": "uptime_kuma.services is disabled in config.yml"}

    monitored, excluded = await fetch_services_partitioned()

    def _preview():
        with uptime_kuma_driver.session() as api:
            monitors = api.get_monitors()
            existing_urls = {
                (m.get("url") or "").rstrip("/"): m["id"]
                for m in monitors if m.get("url")
            }
            existing_names = {
                (m.get("name") or "").lower(): m["id"]
                for m in monitors if m.get("name")
            }

            preview_list = []
            existing_count = 0
            pending_count = 0
            pending_deletions = 0

            for svc in monitored:
                url = svc["url"].rstrip("/")
                mid = existing_urls.get(url) or existing_names.get(svc["name"].lower())
                if mid:
                    existing_count += 1
                    status_str = "exists"
                else:
                    pending_count += 1
                    status_str = "pending_create"
                preview_list.append({**svc, "monitor_id": mid, "status": status_str})

            for svc in excluded:
                url = svc["url"].rstrip("/")
                mid = existing_urls.get(url) or existing_names.get(svc["name"].lower())
                if mid:
                    pending_deletions += 1
                    status_str = "pending_delete (tagged no-monitor)"
                else:
                    status_str = "excluded_ok"
                preview_list.append({**svc, "monitor_id": mid, "status": status_str})

            return {
                "total_services": len(monitored) + len(excluded),
                "active_candidates": len(monitored),
                "excluded_services": len(excluded),
                "already_monitored": existing_count,
                "pending_provisioning": pending_count,
                "pending_deletions": pending_deletions,
                "services": preview_list,
            }

    return await asyncio.to_thread(_preview)


async def run_sync() -> Dict[str, Any]:
    """Execute the services -> Uptime Kuma HTTP monitor sync."""
    if not module_manager.is_enabled("uptime_kuma"):
        print(f"\n{YELLOW}[SKIP] Uptime Kuma module is disabled; skipping service sync.{RESET}\n")
        return {"status": "disabled", "message": "Uptime Kuma module is disabled"}

    svc_cfg = _get_services_cfg()
    if not svc_cfg.get("enabled", False):
        return {"status": "disabled", "message": "uptime_kuma.services is disabled in config.yml -- set enabled: true to activate"}

    print(f"\n{BOLD}{CYAN}================================================================{RESET}")
    print(f"{BOLD}{CYAN}    NPU Orchestrator: NetBox Services -> Uptime Kuma HTTP Sync   {RESET}")
    print(f"{BOLD}{CYAN}================================================================{RESET}\n")

    try:
        status = await uptime_kuma_driver.test_connection()
        print(f"[{GREEN}OK{RESET}] Uptime Kuma connected: {status['url']} ({status['total_monitors']} monitors)")
    except Exception as e:
        print(f"[{RED}ERR{RESET}] Failed to connect to Uptime Kuma: {e}")
        return {"status": "error", "error": str(e)}

    try:
        monitored, excluded = await fetch_services_partitioned()
        print(f"[{GREEN}OK{RESET}] NetBox: {len(monitored)} services to monitor, {len(excluded)} excluded.\n")
    except Exception as e:
        print(f"[{RED}ERR{RESET}] Failed to query NetBox services: {e}")
        return {"status": "error", "error": str(e)}

    print(f"{BOLD}{'SERVICE NAME':<30} {'URL':<45} {'STATUS'}{RESET}")
    print("-" * 85)

    res = await uptime_kuma_driver.sync_services_batch(monitored, excluded)

    for item in res.get("details", []):
        name = item.get("name", "N/A")
        url = item.get("url", "N/A")
        st = item.get("status")
        mid = item.get("monitor_id")

        if st == "created":
            print(f"{name:<30} {url:<45} {GREEN}CREATED (ID: {mid}){RESET}")
        elif st == "existing":
            print(f"{name:<30} {url:<45} {YELLOW}EXISTS  (ID: {mid}){RESET}")
        elif st == "deleted_excluded":
            print(f"{name:<30} {url:<45} {MAGENTA}DELETED (no-monitor){RESET}")
        elif st == "skipped_no_url":
            print(f"{name:<30} {'(no public_url)':<45} {YELLOW}SKIPPED{RESET}")
        else:
            err = item.get("error", "Unknown error")
            print(f"{name:<30} {url:<45} {RED}ERROR: {err}{RESET}")

    print("\n" + "=" * 85)
    print(f"{BOLD}Sync Complete!{RESET}")
    print(f"  Total Services:    {len(monitored)}")
    print(f"  {GREEN}Newly Created:     {res.get('created_count', 0)}{RESET}")
    print(f"  {YELLOW}Already Existed:   {res.get('existing_count', 0)}{RESET}")
    if res.get("deleted_count", 0) > 0:
        print(f"  {MAGENTA}Removed (Tags):    {res.get('deleted_count', 0)}{RESET}")
    if res.get("error_count", 0) > 0:
        print(f"  {RED}Errors:            {res.get('error_count', 0)}{RESET}")
    print("=" * 85 + "\n")

    return res


if __name__ == "__main__":
    asyncio.run(run_sync())
