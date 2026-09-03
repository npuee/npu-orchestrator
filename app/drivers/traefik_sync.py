import glob
import logging
import os
import re
from typing import Any, Dict, List, Optional
import httpx
import yaml

from app.core.config import settings
from app.core.app_config import app_config
from app.drivers.netbox import netbox_driver

logger = logging.getLogger("orchestrator.traefik_sync")

DOCKER_SOCKET_PATH = "/var/run/docker.sock"
TRAEFIK_DIR = "/cloud/traefik"
TRAEFIK_CONF_DIR = "/cloud/traefik/conf"
REMOTE_DEFAULT_TRAEFIK_API = "http://192.168.1.50:8080"


class TraefikSyncDriver:
    """
    Discovers Traefik Ingress Routers from multiple instances:
    1. traefik-oracle (Oracle Cloud Docker & Local configs -> NetBox VM #7)
    2. traefik-remote (Proxmox VE On-Premises Service -> NetBox VM #6)
    and idempotently synchronizes them into NetBox as Application Services.
    """

    def get_default_entrypoint_middlewares(self, traefik_dir: Optional[str] = None) -> List[str]:
        """Reads default entrypoint middlewares from traefik.yml (e.g. websecure)."""
        target_dir = traefik_dir or TRAEFIK_DIR
        traefik_yaml_path = os.path.join(target_dir, "traefik.yml")
        if not os.path.exists(traefik_yaml_path):
            return []

        try:
            with open(traefik_yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                entrypoints = data.get("entryPoints", {})
                websecure = entrypoints.get("websecure", {})
                return websecure.get("http", {}).get("middlewares", []) or []
        except Exception as e:
            logger.warning("Could not read default entrypoint middlewares from traefik.yml: %s", e)

        return []

    async def discover_docker_routes(self, traefik_dir: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries Docker Engine API via unix socket to extract containers with Traefik router rules.
        """
        routes = []
        if not os.path.exists(DOCKER_SOCKET_PATH):
            logger.warning("Docker socket '%s' not found. Skipping Docker Traefik route discovery.", DOCKER_SOCKET_PATH)
            return routes

        default_middlewares = self.get_default_entrypoint_middlewares(traefik_dir=traefik_dir)

        try:
            transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET_PATH)
            async with httpx.AsyncClient(transport=transport, timeout=10.0) as client:
                resp = await client.get("http://docker/containers/json")
                if resp.status_code != 200:
                    logger.error("Failed to query Docker API: %s %s", resp.status_code, resp.text)
                    return routes

                containers = resp.json()
                for c in containers:
                    c_name = c["Names"][0].lstrip("/") if c.get("Names") else ""
                    labels = c.get("Labels", {})
                    
                    for k, v in labels.items():
                        if "routers." in k and ".rule" in k:
                            rule = v
                            match_rname = re.search(r"routers\.([^.]+)\.rule", k)
                            rname = match_rname.group(1) if match_rname else ""

                            hosts = re.findall(r"Host\(`([^`]+)`\)", rule)
                            if not hosts:
                                continue
                            
                            primary_host = hosts[0]
                            all_hosts_str = ", ".join(hosts)
                            
                            port_val = 80
                            for pk, pv in labels.items():
                                if "loadbalancer.server.port" in pk:
                                    try:
                                        port_val = int(pv)
                                    except ValueError:
                                        pass
                                    break
                            
                            router_middlewares = []
                            if rname:
                                m_label = labels.get(f"traefik.http.routers.{rname}.middlewares", "")
                                if m_label:
                                    router_middlewares = [m.strip() for m in m_label.split(",") if m.strip()]
                            if not router_middlewares:
                                for mk, mv in labels.items():
                                    if "middlewares" in mk and "http.middlewares" not in mk:
                                        router_middlewares = [m.strip() for m in mv.split(",") if m.strip()]
                                        break

                            combined_middlewares = []
                            for dm in default_middlewares:
                                if dm not in combined_middlewares:
                                    combined_middlewares.append(dm)
                            for rm in router_middlewares:
                                if rm not in combined_middlewares:
                                    combined_middlewares.append(rm)

                            middlewares_str = ", ".join(combined_middlewares)
                            sso_protected = "npu-sso" in middlewares_str.lower()
                            ip_whitelist = "npu-ip-whitelist" in middlewares_str.lower()

                            tags = []
                            if sso_protected:
                                tags.append({"name": "SSO", "slug": "sso"})
                            if ip_whitelist:
                                tags.append({"name": "NPU Whitelist", "slug": "npu-whitelist"})
                            if not sso_protected and not ip_whitelist:
                                tags.append({"name": "Public Ingress", "slug": "public-ingress"})

                            nets = c.get("NetworkSettings", {}).get("Networks", {})
                            c_ip = ""
                            if nets:
                                c_ip = list(nets.values())[0].get("IPAddress", "")
                            
                            friendly_name = c_name.replace("-", " ").title()
                            service_name = f"{friendly_name} Ingress"
                            target_backend = f"http://{c_ip}:{port_val}" if c_ip else f"port {port_val}"
                            description = f"Traefik Ingress: {primary_host} -> {target_backend}"
                            
                            routes.append({
                                "name": service_name,
                                "fqdn": all_hosts_str,
                                "primary_fqdn": primary_host,
                                "public_url": f"https://{primary_host}",
                                "target": target_backend,
                                "protocol": "tcp",
                                "ports": [443],
                                "description": description,
                                "container_name": c_name,
                                "sso_protected": sso_protected,
                                "ip_whitelist": ip_whitelist,
                                "middlewares": middlewares_str,
                                "tags": tags,
                                "source": "docker",
                            })
        except Exception as e:
            logger.exception("Error discovering Docker Traefik routes: %s", e)

        return routes

    def discover_file_routes(self, conf_dir: Optional[str] = None, traefik_dir: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Parses dynamic YAML files in conf_dir to extract file-based Traefik routers.
        """
        target_conf_dir = conf_dir or TRAEFIK_CONF_DIR
        routes = []
        if not os.path.exists(target_conf_dir):
            return routes

        default_middlewares = self.get_default_entrypoint_middlewares(traefik_dir=traefik_dir)

        try:
            yaml_files = glob.glob(os.path.join(target_conf_dir, "*.yml")) + glob.glob(os.path.join(target_conf_dir, "*.yaml"))
            for ypath in yaml_files:
                try:
                    with open(ypath, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f)
                    if not isinstance(data, dict):
                        continue

                    http_data = data.get("http", {})
                    routers = http_data.get("routers", {})
                    services = http_data.get("services", {})

                    for r_name, r_conf in routers.items():
                        rule = r_conf.get("rule", "")
                        hosts = re.findall(r"Host\(`([^`]+)`\)", rule)
                        if not hosts:
                            continue

                        primary_host = hosts[0]
                        all_hosts_str = ", ".join(hosts)
                        
                        m_list = r_conf.get("middlewares", [])
                        router_middlewares = m_list if isinstance(m_list, list) else [str(m_list)]

                        combined_middlewares = []
                        for dm in default_middlewares:
                            if dm not in combined_middlewares:
                                combined_middlewares.append(dm)
                        for rm in router_middlewares:
                            if rm not in combined_middlewares:
                                combined_middlewares.append(rm)

                        middlewares_str = ", ".join(combined_middlewares)
                        sso_protected = "npu-sso" in middlewares_str.lower()
                        ip_whitelist = "npu-ip-whitelist" in middlewares_str.lower()

                        tags = []
                        if sso_protected:
                            tags.append({"name": "SSO", "slug": "sso"})
                        if ip_whitelist:
                            tags.append({"name": "NPU Whitelist", "slug": "npu-whitelist"})
                        if not sso_protected and not ip_whitelist:
                            tags.append({"name": "Public Ingress", "slug": "public-ingress"})

                        svc_name = r_conf.get("service")
                        target_url = "dynamic"
                        if svc_name and svc_name in services:
                            servers = services[svc_name].get("loadBalancer", {}).get("servers", [])
                            if servers and isinstance(servers[0], dict) and "url" in servers[0]:
                                target_url = servers[0]["url"]

                        fname = os.path.basename(ypath).replace(".yml", "").replace(".yaml", "")
                        service_name = f"{fname.replace('-', ' ').title()} Ingress"
                        description = f"Traefik Ingress: {primary_host} -> {target_url}"

                        routes.append({
                            "name": service_name,
                            "fqdn": all_hosts_str,
                            "primary_fqdn": primary_host,
                            "public_url": f"https://{primary_host}",
                            "target": target_url,
                            "protocol": "tcp",
                            "ports": [443],
                            "description": description,
                            "container_name": fname,
                            "sso_protected": sso_protected,
                            "ip_whitelist": ip_whitelist,
                            "middlewares": middlewares_str,
                            "tags": tags,
                            "source": "file",
                        })
                except Exception as fe:
                    logger.warning("Could not parse Traefik config file %s: %s", ypath, fe)
        except Exception as e:
            logger.exception("Error discovering file Traefik routes: %s", e)

        return routes

    async def discover_remote_traefik_routes(self, api_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries a remote Traefik instance (e.g. traefik-remote on Proxmox)
        to extract all active routers, services, target URLs, and middleware chains.
        """
        target_url = api_url or REMOTE_DEFAULT_TRAEFIK_API
        routes = []
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                # 1. Fetch routers & services
                resp_routers = await client.get(f"{target_url}/api/http/routers")
                if resp_routers.status_code != 200:
                    logger.warning("Could not fetch remote Traefik routers from %s (status: %d)", target_url, resp_routers.status_code)
                    return routes

                resp_services = await client.get(f"{target_url}/api/http/services")
                services_map = {}
                if resp_services.status_code == 200:
                    for s in resp_services.json():
                        s_name = s.get("name")
                        servers = s.get("loadBalancer", {}).get("servers", [])
                        if servers and isinstance(servers[0], dict) and "url" in servers[0]:
                            services_map[s_name] = servers[0]["url"]

                routers_data = resp_routers.json()
                for r in routers_data:
                    rule = r.get("rule", "")
                    hosts = re.findall(r"Host\(`([^`]+)`\)", rule)
                    if not hosts:
                        continue

                    primary_host = hosts[0]
                    all_hosts_str = ", ".join(hosts)
                    r_raw_name = r.get("name", "").split("@")[0]
                    
                    # Target backend service URL
                    svc_key = r.get("service")
                    target_url = services_map.get(svc_key, svc_key or "dynamic")

                    # Middlewares
                    middlewares = r.get("middlewares", [])
                    middlewares_str = ", ".join(middlewares)

                    sso_protected = "npu-sso" in middlewares_str.lower()
                    ip_whitelist = "npu-ip-whitelist" in middlewares_str.lower()

                    tags = []
                    if sso_protected:
                        tags.append({"name": "SSO", "slug": "sso"})
                    if ip_whitelist:
                        tags.append({"name": "NPU Whitelist", "slug": "npu-whitelist"})
                    if not sso_protected and not ip_whitelist:
                        tags.append({"name": "Public Ingress", "slug": "public-ingress"})

                    friendly_name = r_raw_name.replace("-", " ").replace(".", " ").title()
                    service_name = f"{friendly_name} Ingress"
                    description = f"Traefik Ingress: {primary_host} -> {target_url}"

                    routes.append({
                        "name": service_name,
                        "fqdn": all_hosts_str,
                        "primary_fqdn": primary_host,
                        "public_url": f"https://{primary_host}",
                        "target": target_url,
                        "protocol": "tcp",
                        "ports": [443],
                        "description": description,
                        "container_name": r_raw_name,
                        "sso_protected": sso_protected,
                        "ip_whitelist": ip_whitelist,
                        "middlewares": middlewares_str,
                        "tags": tags,
                        "source": "remote-api",
                    })
        except Exception as e:
            logger.warning("Error querying remote Traefik at %s: %s", api_url, e)

        return routes

    async def get_oracle_routes(self, path: Optional[str] = None, conf_dir: Optional[str] = None) -> List[Dict[str, Any]]:
        """Combines local Docker + file routes."""
        docker_routes = await self.discover_docker_routes(traefik_dir=path)
        file_routes = self.discover_file_routes(conf_dir=conf_dir, traefik_dir=path)

        seen_fqdns = set()
        merged = []

        for r in file_routes + docker_routes:
            key = r["primary_fqdn"]
            if key not in seen_fqdns:
                seen_fqdns.add(key)
                merged.append(r)

        return merged

    async def sync_instance(
        self,
        instance_name: str,
        netbox_vm_id: int,
        instance_conf: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Synchronizes a specific Traefik instance into NetBox under netbox_vm_id.
        """
        if not instance_conf:
            for inst in app_config.traefik.get("instances", []):
                if inst.get("name") == instance_name or inst.get("netbox_vm_id") == netbox_vm_id:
                    instance_conf = inst
                    break

        inst_type = (instance_conf or {}).get("type", "api" if "remote" in instance_name else "docker")
        api_url = (instance_conf or {}).get("api_url", REMOTE_DEFAULT_TRAEFIK_API)
        path = (instance_conf or {}).get("path", TRAEFIK_DIR)
        conf_dir = (instance_conf or {}).get("conf_dir", TRAEFIK_CONF_DIR)

        if inst_type == "api":
            routes = await self.discover_remote_traefik_routes(api_url)
        else:
            routes = await self.get_oracle_routes(path=path, conf_dir=conf_dir)

        if not netbox_driver.is_configured():
            return {"status": "skipped", "reason": "NetBox not configured", "instance": instance_name}

        headers = {
            "Authorization": f"Token {netbox_driver.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        created = []
        updated = []
        unchanged = []

        try:
            client = netbox_driver._get_client()
            resp = await client.get(
                f"{netbox_driver.base_url}/api/ipam/services/?virtual_machine_id={netbox_vm_id}&limit=100",
                headers=headers,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch NetBox services for VM {netbox_vm_id}: {resp.status_code} {resp.text}")

            existing_list = resp.json().get("results", [])
                
            existing_by_fqdn = {}
            existing_by_name = {}
            for svc in existing_list:
                cfields = svc.get("custom_fields", {})
                fqdn_val = cfields.get("fqdn")
                if fqdn_val:
                    for f_part in fqdn_val.split(","):
                        existing_by_fqdn[f_part.strip().lower()] = svc
                existing_by_name[svc.get("name", "").lower()] = svc

            for r in routes:
                primary_fqdn = r["primary_fqdn"].lower()
                existing_svc = existing_by_fqdn.get(primary_fqdn) or existing_by_name.get(r["name"].lower())

                custom_fields_payload = {
                    "fqdn": r["fqdn"],
                    "public_url": r["public_url"],
                    "sso_protected": r["sso_protected"],
                    "ip_whitelist": r["ip_whitelist"],
                    "middlewares": r["middlewares"],
                }

                service_payload = {
                    "name": r["name"],
                    "parent_object_type": "virtualization.virtualmachine",
                    "parent_object_id": netbox_vm_id,
                    "protocol": r["protocol"],
                    "ports": r["ports"],
                    "description": r["description"],
                    "tags": r["tags"],
                    "custom_fields": custom_fields_payload,
                }

                if existing_svc:
                    svc_id = existing_svc["id"]
                    curr_desc = existing_svc.get("description", "")
                    curr_ports = existing_svc.get("ports", [])
                    curr_cf = existing_svc.get("custom_fields", {})
                    curr_tags = [t.get("slug") for t in existing_svc.get("tags", [])]
                    new_tags = [t["slug"] for t in r["tags"]]

                    needs_update = (
                        curr_desc != r["description"]
                        or curr_ports != r["ports"]
                        or curr_cf.get("public_url") != r["public_url"]
                        or curr_cf.get("fqdn") != r["fqdn"]
                        or curr_cf.get("sso_protected") != r["sso_protected"]
                        or curr_cf.get("ip_whitelist") != r["ip_whitelist"]
                        or curr_cf.get("middlewares") != r["middlewares"]
                        or set(curr_tags) != set(new_tags)
                    )

                    if needs_update:
                        patch_resp = await client.patch(
                            f"{netbox_driver.base_url}/api/ipam/services/{svc_id}/",
                            headers=headers,
                            json={
                                "description": r["description"],
                                "ports": r["ports"],
                                "tags": r["tags"],
                                "custom_fields": custom_fields_payload,
                            },
                        )
                        if patch_resp.status_code == 200:
                            updated.append({
                                "id": svc_id,
                                "name": r["name"],
                                "fqdn": r["primary_fqdn"],
                                "sso": r["sso_protected"],
                                "whitelist": r["ip_whitelist"],
                            })
                            logger.info("[%s] Updated NetBox Service %s (ID: %d)", instance_name, r["name"], svc_id)
                        else:
                            logger.warning("[%s] Failed to update Service %d: %s", instance_name, svc_id, patch_resp.text)
                    else:
                        unchanged.append({
                            "id": svc_id,
                            "name": r["name"],
                            "fqdn": r["primary_fqdn"],
                            "sso": r["sso_protected"],
                            "whitelist": r["ip_whitelist"],
                        })
                else:
                    post_resp = await client.post(
                        f"{netbox_driver.base_url}/api/ipam/services/",
                        headers=headers,
                        json=service_payload,
                    )
                    if post_resp.status_code in (200, 201):
                        new_id = post_resp.json().get("id")
                        created.append({
                            "id": new_id,
                            "name": r["name"],
                            "fqdn": r["primary_fqdn"],
                            "sso": r["sso_protected"],
                            "whitelist": r["ip_whitelist"],
                        })
                        logger.info("[%s] Created NetBox Service %s (ID: %d)", instance_name, r["name"], new_id)
                    else:
                        logger.warning("[%s] Failed to create Service %s: %s", instance_name, r["name"], post_resp.text)

        except Exception as e:
            logger.exception("Error syncing Traefik routes for %s: %s", instance_name, e)
            raise

        return {
            "status": "success",
            "instance": instance_name,
            "netbox_vm_id": netbox_vm_id,
            "total_discovered": len(routes),
            "created_count": len(created),
            "updated_count": len(updated),
            "unchanged_count": len(unchanged),
            "created": created,
            "updated": updated,
            "unchanged": unchanged,
        }

    async def sync_all_instances(self) -> Dict[str, Any]:
        """Synchronizes all configured Traefik instances from config.yml."""
        traefik_cfg = app_config.traefik
        if not traefik_cfg.get("enabled", True):
            logger.info("Traefik sync is disabled in config.yml.")
            return {"status": "disabled"}

        instances = traefik_cfg.get("instances", [])
        if not instances:
            instances = [
                {"name": "traefik-oracle", "netbox_vm_id": 7, "type": "docker", "path": "/cloud/traefik", "conf_dir": "/cloud/traefik/conf"},
                {"name": "traefik-remote", "netbox_vm_id": 6, "type": "api", "api_url": "http://192.168.1.50:8080"},
            ]

        results = {}
        for inst in instances:
            name = inst.get("name", "unknown")
            vm_id = inst.get("netbox_vm_id")
            if not vm_id:
                logger.warning("Traefik instance '%s' is missing netbox_vm_id in config.yml. Skipping.", name)
                continue
            try:
                res = await self.sync_instance(name, netbox_vm_id=vm_id, instance_conf=inst)
                results[name] = res
            except Exception as e:
                logger.error("Failed to sync Traefik instance '%s': %s", name, e)
                results[name] = {"status": "error", "error": str(e)}

        return results


traefik_sync_driver = TraefikSyncDriver()
