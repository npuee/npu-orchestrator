"""
NetBox to Uptime Kuma Inventory Synchronization Script.

Fetches all physical devices with a Primary IPv4 from NetBox DCIM:
- Provisions ICMP Ping monitors under Site groups with default notifications.
- Automatically synchronizes 'kuma_monitor_id' custom field in NetBox for 1-click dashboard linking.
- Automatically removes monitors for devices tagged with 'no-monitor' (or configured exclude_tags).
- Reconciles missing notifications on existing monitors.
"""

import asyncio
import sys
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
logger = logging.getLogger("sync_kuma_inventory")

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
MAGENTA = "\033[95m"
RESET = "\033[0m"


async def fetch_devices_partitioned() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetches physical devices with Primary IPv4 from NetBox DCIM
    and partitions them into (monitored_devices, excluded_devices) based on NetBox tags.
    """
    client = netbox_driver._get_client()
    headers = {"Authorization": f"Token {netbox_driver.token}", "Accept": "application/json"}
    url = f"{netbox_driver.base_url}/api/dcim/devices/?limit=200"
    resp = await client.get(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"NetBox API returned HTTP {resp.status_code}: {resp.text}")
    devices = resp.json().get("results", [])

    # Read from new nested devices config; fall back to legacy flat keys for compatibility
    devices_cfg = app_config.uptime_kuma.get("devices", {})
    exclude_tag = devices_cfg.get("exclude_tag") or ""
    # Legacy fallback: old config used exclude_tags as a list
    legacy_exclude = app_config.uptime_kuma.get("exclude_tags", [])
    exclude_tags = {exclude_tag.strip().lower()} if exclude_tag else set()
    exclude_tags.update(t.strip().lower() for t in legacy_exclude if t.strip())

    monitored_devices = []
    excluded_devices = []

    for d in devices:
        primary_ip = d.get("primary_ip4")
        if not primary_ip or not primary_ip.get("address"):
            continue

        clean_ip = primary_ip["address"].split("/")[0]
        site_name = d.get("site", {}).get("name", "Default")
        role_name = d.get("role", {}).get("name", "Device")
        current_cf_kuma = (d.get("custom_fields") or {}).get("kuma_monitor_id")
        
        # Collect tag names and slugs
        tag_list = []
        for t in d.get("tags", []):
            if t.get("slug"):
                tag_list.append(t["slug"].lower())
            if t.get("name"):
                tag_list.append(t["name"].lower())

        is_excluded = any(et in tag_list for et in exclude_tags)

        entry = {
            "id": d["id"],
            "name": d["name"],
            "hostname": clean_ip,
            "site": site_name,
            "role": role_name,
            "tags": tag_list,
            "kuma_monitor_id": current_cf_kuma,
            "description": f"NetBox ID: {d['id']} | Model: {d.get('device_type', {}).get('model', 'N/A')}"
        }

        if is_excluded:
            excluded_devices.append(entry)
        else:
            monitored_devices.append(entry)

    monitored_devices.sort(key=lambda x: (x["site"], x["name"]))
    excluded_devices.sort(key=lambda x: (x["site"], x["name"]))
    return monitored_devices, excluded_devices


async def preview_sync() -> Dict[str, Any]:
    """Previews NetBox candidate devices, exclusions, and checks against current Uptime Kuma monitors (read-only)."""
    if not module_manager.is_enabled("uptime_kuma"):
        return {"status": "disabled", "message": "Uptime Kuma module is disabled or not configured in .env / config.yml"}

    monitored_devices, excluded_devices = await fetch_devices_partitioned()

    def _preview():
        with uptime_kuma_driver.session() as api:
            monitors = api.get_monitors()
            existing_ips = {m.get("hostname").split('/')[0]: m["id"] for m in monitors if m.get("hostname")}
            existing_names = {m.get("name").lower(): m["id"] for m in monitors if m.get("name")}

            preview_list = []
            existing_count = 0
            pending_count = 0
            pending_deletions = 0

            # Check monitored devices
            for dev in monitored_devices:
                ip = dev["hostname"]
                name = dev["name"]
                mid = existing_ips.get(ip) or existing_names.get(name.lower())
                if mid:
                    existing_count += 1
                    status_str = "exists"
                else:
                    pending_count += 1
                    status_str = "pending_create"
                preview_list.append({
                    "id": dev["id"],
                    "name": name,
                    "ip": ip,
                    "site": dev["site"],
                    "role": dev["role"],
                    "monitor_id": mid,
                    "kuma_monitor_id": dev["kuma_monitor_id"],
                    "status": status_str
                })

            # Check excluded devices that still have monitors in Kuma
            for ex in excluded_devices:
                ip = ex["hostname"]
                name = ex["name"]
                mid = existing_ips.get(ip) or existing_names.get(name.lower())
                if mid:
                    pending_deletions += 1
                    status_str = "pending_delete (tagged no-monitor)"
                else:
                    status_str = "excluded_ok"
                preview_list.append({
                    "id": ex["id"],
                    "name": name,
                    "ip": ip,
                    "site": ex["site"],
                    "role": ex["role"],
                    "monitor_id": mid,
                    "kuma_monitor_id": ex["kuma_monitor_id"],
                    "status": status_str
                })

            return {
                "total_devices_with_ip": len(monitored_devices) + len(excluded_devices),
                "active_candidates": len(monitored_devices),
                "excluded_devices": len(excluded_devices),
                "already_monitored": existing_count,
                "pending_provisioning": pending_count,
                "pending_deletions": pending_deletions,
                "devices": preview_list
            }

    return await asyncio.to_thread(_preview)


async def run_sync() -> Dict[str, Any]:
    if not module_manager.is_enabled("uptime_kuma"):
        print(f"\n{YELLOW}[⏩ SKIP] Uptime Kuma module is disabled or not configured in .env / config.yml; skipping sync.{RESET}\n")
        return {"status": "disabled", "message": "Uptime Kuma module is disabled or not configured in .env / config.yml"}

    print(f"\n{BOLD}{CYAN}================================================================{RESET}")
    print(f"{BOLD}{CYAN}     NPU Orchestrator: NetBox ➔ Uptime Kuma Inventory Sync      {RESET}")
    print(f"{BOLD}{CYAN}================================================================{RESET}\n")

    # 1. Test Uptime Kuma Connection
    try:
        status = await uptime_kuma_driver.test_connection()
        print(f"[{GREEN}✓{RESET}] Uptime Kuma connected: {status['url']} (Current monitors: {status['total_monitors']})")
    except Exception as e:
        print(f"[{RED}✗{RESET}] Failed to connect to Uptime Kuma: {e}")
        return {"status": "error", "error": f"Failed to connect to Uptime Kuma: {e}"}

    # 2. Fetch Devices from NetBox
    try:
        monitored_devices, excluded_devices = await fetch_devices_partitioned()
        print(f"[{GREEN}✓{RESET}] NetBox: {len(monitored_devices)} monitored devices, {len(excluded_devices)} excluded by tag.\n")
    except Exception as e:
        print(f"[{RED}✗{RESET}] Failed to query NetBox: {e}")
        return {"status": "error", "error": f"Failed to query NetBox: {e}"}

    print(f"{BOLD}{'SITE':<12} {'DEVICE NAME':<25} {'IP ADDRESS':<16} {'STATUS'}{RESET}")
    print("-" * 65)

    # 3. Execute Batch Sync with Exclusions Reconciliation
    res = await uptime_kuma_driver.sync_devices_batch(monitored_devices, excluded_devices)

    client = netbox_driver._get_client()
    headers = {"Authorization": f"Token {netbox_driver.token}", "Accept": "application/json"}
    monitored_map = {d["name"]: d for d in monitored_devices}
    excluded_map = {d["name"]: d for d in excluded_devices}

    for item in res.get("details", []):
        site = item.get("site", "N/A")
        name = item.get("name", "N/A")
        ip = item.get("ip", "N/A")
        st = item.get("status")
        mid = item.get("monitor_id")

        if st == "created":
            print(f"{site:<12} {name:<25} {ip:<16} {GREEN}CREATED (ID: {mid}){RESET}")
            # Update NetBox custom field if needed
            dev = monitored_map.get(name)
            if dev and dev.get("kuma_monitor_id") != mid:
                try:
                    await client.patch(f"{netbox_driver.base_url}/api/dcim/devices/{dev['id']}/", headers=headers, json={"custom_fields": {"kuma_monitor_id": mid}})
                except Exception as e:
                    logger.warning("Could not set kuma_monitor_id on NetBox device '%s': %s", name, e)

        elif st == "existing":
            print(f"{site:<12} {name:<25} {ip:<16} {YELLOW}EXISTS  (ID: {mid}){RESET}")
            # Ensure NetBox custom field is populated
            dev = monitored_map.get(name)
            if dev and dev.get("kuma_monitor_id") != mid:
                try:
                    await client.patch(f"{netbox_driver.base_url}/api/dcim/devices/{dev['id']}/", headers=headers, json={"custom_fields": {"kuma_monitor_id": mid}})
                except Exception as e:
                    logger.warning("Could not set kuma_monitor_id on NetBox device '%s': %s", name, e)

        elif st == "deleted_excluded":
            print(f"{site:<12} {name:<25} {ip:<16} {MAGENTA}DELETED (Tag: no-monitor){RESET}")
            # Clear NetBox custom field
            dev = excluded_map.get(name)
            if dev and dev.get("kuma_monitor_id") is not None:
                try:
                    await client.patch(f"{netbox_driver.base_url}/api/dcim/devices/{dev['id']}/", headers=headers, json={"custom_fields": {"kuma_monitor_id": None}})
                except Exception as e:
                    logger.warning("Could not clear kuma_monitor_id on NetBox device '%s': %s", name, e)

        else:
            err = item.get("error", "Unknown error")
            print(f"{site:<12} {name:<25} {ip:<16} {RED}ERROR: {err}{RESET}")

    print("\n" + "=" * 65)
    print(f"{BOLD}Sync Complete!{RESET}")
    print(f"  Total Monitored:   {len(monitored_devices)}")
    print(f"  {GREEN}Newly Provisioned: {res.get('created_count', 0)}{RESET}")
    print(f"  {YELLOW}Already Existed:   {res.get('existing_count', 0)}{RESET}")
    if res.get('deleted_count', 0) > 0:
        print(f"  {MAGENTA}Removed (Tags):    {res.get('deleted_count', 0)}{RESET}")
    if res.get('error_count', 0) > 0:
        print(f"  {RED}Errors:            {res.get('error_count', 0)}{RESET}")
    print("=" * 65 + "\n")

    return res


if __name__ == "__main__":
    asyncio.run(run_sync())
