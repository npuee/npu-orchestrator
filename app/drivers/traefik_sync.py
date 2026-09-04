import glob
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple
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
    1. Local Docker socket mode / file configs (NetBox Virtual Machine)
    2. Remote REST API mode (NetBox Bare-Metal Device or Host)
    and idempotently synchronizes them into NetBox as Application Services.
    """

    def __init__(self):
        self._cached_service_tags: Optional[List[Dict[str, str]]] = None

    def evaluate_middlewares(self, middlewares: Any) -> Tuple[bool, bool]:
        """
        Evaluates whether the given middlewares match configured patterns for
        ip_whitelist and sso_protected.
        Supports Option 2 schema (traefik.middlewares.<name>.patterns) as well as
        legacy schema (traefik.middleware_patterns.<name>).
        """
        traefik_cfg = app_config.traefik if app_config else {}
        mw_cfg = traefik_cfg.get("middlewares", {})

        # Check Option 2: middlewares.ip_whitelist.patterns / middlewares.whitelist.patterns
        whitelist_patterns = []
        if isinstance(mw_cfg.get("ip_whitelist"), dict):
            whitelist_patterns = mw_cfg["ip_whitelist"].get("patterns", [])
        elif isinstance(mw_cfg.get("whitelist"), dict):
            whitelist_patterns = mw_cfg["whitelist"].get("patterns", [])
        if not whitelist_patterns:
            legacy_pats = traefik_cfg.get("middleware_patterns", {})
            whitelist_patterns = legacy_pats.get("ip_whitelist") or legacy_pats.get("whitelist") or ["whitelist", "allowlist", "npu-ip-whitelist"]

        # Check Option 2: middlewares.sso.patterns / middlewares.sso_protected.patterns
        sso_patterns = []
        if isinstance(mw_cfg.get("sso"), dict):
            sso_patterns = mw_cfg["sso"].get("patterns", [])
        elif isinstance(mw_cfg.get("sso_protected"), dict):
            sso_patterns = mw_cfg["sso_protected"].get("patterns", [])
        if not sso_patterns:
            legacy_pats = traefik_cfg.get("middleware_patterns", {})
            sso_patterns = legacy_pats.get("sso") or legacy_pats.get("sso_protected") or ["sso", "forward-auth", "authelia", "authentik", "npu-sso"]

        if isinstance(middlewares, (list, tuple, set)):
            m_str = " ".join(str(m) for m in middlewares).lower()
        else:
            m_str = str(middlewares or "").lower()

        is_whitelist = any(str(pat).lower() in m_str for pat in whitelist_patterns)
        is_sso = any(str(pat).lower() in m_str for pat in sso_patterns)
        return is_whitelist, is_sso

    def get_configured_tag_names(self) -> List[str]:
        """
        Returns the list of configured service tag strings.
        Accepts both 'service_tags' and 'service_tag' from config.yml,
        supporting a single string, comma-separated string, or a YAML list.
        """
        traefik_cfg = app_config.traefik if app_config else {}
        if "service_tags" in traefik_cfg:
            raw = traefik_cfg.get("service_tags")
        elif "service_tag" in traefik_cfg:
            raw = traefik_cfg.get("service_tag")
        else:
            raw = ["traefik"]

        if raw is None or raw is False:
            return []

        if isinstance(raw, str):
            tags = [t.strip() for t in raw.split(",") if t.strip()]
        elif isinstance(raw, (list, tuple, set)):
            tags = [str(t).strip() for t in raw if str(t).strip()]
        else:
            tags = [str(raw).strip()]
        return [t for t in tags if t]

    def get_service_tags(self) -> List[Dict[str, str]]:
        """
        Returns the tag list for discovered Traefik services.
        Supports single or multiple tags configured via service_tags / service_tag in config.yml.
        """
        if self._cached_service_tags is not None:
            return self._cached_service_tags

        tag_names = self.get_configured_tag_names()
        result = []
        for t in tag_names:
            slug = re.sub(r"[^a-z0-9_-]", "-", t.lower()).strip("-")
            name = t.replace("-", " ").replace("_", " ").title()
            if slug and not any(x["slug"] == slug for x in result):
                result.append({"name": name, "slug": slug})
        return result

    async def ensure_service_tags(self, client: httpx.AsyncClient, headers: Dict[str, str]) -> List[Dict[str, str]]:
        """
        Verifies that all configured service tags exist in NetBox, creating any missing ones.
        Returns the canonical list of tag dicts.
        """
        tag_names = self.get_configured_tag_names()
        if not tag_names:
            self._cached_service_tags = []
            return []

        resolved_tags = []
        for t in tag_names:
            slug = re.sub(r"[^a-z0-9_-]", "-", t.lower()).strip("-")
            display_name = t.replace("-", " ").replace("_", " ").title()
            if not slug:
                continue

            try:
                resp = await client.get(f"{netbox_driver.base_url}/api/extras/tags/?slug={slug}", headers=headers)
                if resp.status_code == 200 and resp.json().get("results"):
                    matched = resp.json()["results"][0]
                    resolved_tags.append({"name": matched["name"], "slug": matched["slug"]})
                    continue

                # Create missing tag in NetBox
                create_payload = {
                    "name": display_name,
                    "slug": slug,
                    "color": "2496ed",
                    "description": "Traefik reverse proxy ingress route",
                }
                create_resp = await client.post(f"{netbox_driver.base_url}/api/extras/tags/", headers=headers, json=create_payload)
                if create_resp.status_code in (200, 201):
                    res_data = create_resp.json()
                    resolved_tags.append({"name": res_data.get("name", display_name), "slug": res_data.get("slug", slug)})
                    logger.info("Created NetBox Tag '%s' (slug: %s)", display_name, slug)
                else:
                    resolved_tags.append({"name": display_name, "slug": slug})
            except Exception as e:
                logger.warning("Failed to verify/create NetBox tag '%s': %s", slug, e)
                resolved_tags.append({"name": display_name, "slug": slug})

        self._cached_service_tags = resolved_tags
        return self._cached_service_tags

    async def ensure_service_tag(self, client: httpx.AsyncClient, headers: Dict[str, str]) -> List[Dict[str, str]]:
        """Backwards-compatible alias for ensure_service_tags."""
        return await self.ensure_service_tags(client, headers)

    @staticmethod
    def format_service_name(raw_name: str) -> str:
        """
        Formats a clean, all-lowercase service name without 'Ingress' suffix.
        Strips file extensions (.yml, .yaml), domain suffixes, replaces punctuation/dashes
        with spaces, and trims whitespace.
        """
        name = re.sub(r"\.ya?ml$", "", raw_name, flags=re.IGNORECASE)
        name = re.sub(r"\.[a-z0-9-]+\.(?:[a-z]{2,})$", "", name, flags=re.IGNORECASE)
        name = name.replace("-", " ").replace("_", " ").replace(".", " ")
        return re.sub(r"\s+", " ", name).strip().lower()

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
                            ip_whitelist, sso_protected = self.evaluate_middlewares(combined_middlewares)
                            tags = self.get_service_tags()

                            nets = c.get("NetworkSettings", {}).get("Networks", {})
                            c_ip = ""
                            if nets:
                                c_ip = list(nets.values())[0].get("IPAddress", "")
                            
                            service_name = self.format_service_name(c_name)
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
                        ip_whitelist, sso_protected = self.evaluate_middlewares(combined_middlewares)
                        tags = self.get_service_tags()

                        svc_name = r_conf.get("service")
                        target_url = "dynamic"
                        if svc_name and svc_name in services:
                            servers = services[svc_name].get("loadBalancer", {}).get("servers", [])
                            if servers and isinstance(servers[0], dict) and "url" in servers[0]:
                                target_url = servers[0]["url"]

                        fname = os.path.basename(ypath)
                        service_name = self.format_service_name(fname)
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
                    ip_whitelist, sso_protected = self.evaluate_middlewares(middlewares)
                    tags = self.get_service_tags()

                    service_name = self.format_service_name(r_raw_name)
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
        netbox_vm_id: Optional[int] = None,
        netbox_device_id: Optional[int] = None,
        instance_conf: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Synchronizes a specific Traefik instance into NetBox under either:
        - netbox_device_id (dcim.device, e.g. bare-metal Proxmox VE host)
        - netbox_vm_id (virtualization.virtualmachine, e.g. Cloud/OCI VM)
        """
        if not instance_conf:
            for inst in app_config.traefik.get("instances", []):
                if inst.get("name") == instance_name:
                    instance_conf = inst
                    break
                if netbox_vm_id and inst.get("netbox_vm_id") == netbox_vm_id:
                    instance_conf = inst
                    break
                if netbox_device_id and inst.get("netbox_device_id") == netbox_device_id:
                    instance_conf = inst
                    break

        vm_id = netbox_vm_id or (instance_conf or {}).get("netbox_vm_id")
        device_id = netbox_device_id or (instance_conf or {}).get("netbox_device_id")

        if device_id:
            parent_type = "dcim.device"
            parent_id = int(device_id)
            filter_param = f"device_id={parent_id}"
            parent_label = f"Device #{parent_id}"
        elif vm_id:
            parent_type = "virtualization.virtualmachine"
            parent_id = int(vm_id)
            filter_param = f"virtual_machine_id={parent_id}"
            parent_label = f"VM #{parent_id}"
        else:
            raise ValueError(f"Traefik instance '{instance_name}' must specify either 'netbox_vm_id' or 'netbox_device_id'.")

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
                f"{netbox_driver.base_url}/api/ipam/services/?{filter_param}&limit=100",
                headers=headers,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch NetBox services for {parent_label}: {resp.status_code} {resp.text}")

            existing_list = resp.json().get("results", [])
                
            # Ensure configured service tags exist in NetBox and attach to all routes
            ensured_tags = await self.ensure_service_tags(client, headers)
            for r in routes:
                r["tags"] = ensured_tags

            # Load custom fields mapping from config.yml (supporting Option 2 schema and legacy custom_fields)
            traefik_cfg = app_config.traefik if app_config else {}
            mw_cfg = traefik_cfg.get("middlewares", {})
            svc_fields = traefik_cfg.get("service_fields", {})
            legacy_cf = traefik_cfg.get("custom_fields", {})

            field_public_url = svc_fields.get("public_url") or legacy_cf.get("public_url", "public_url")
            field_fqdn = svc_fields.get("fqdn") or legacy_cf.get("fqdn", "fqdn")
            field_middlewares = svc_fields.get("middlewares") or legacy_cf.get("middlewares", "middlewares")

            field_sso = (
                (mw_cfg.get("sso", {}) if isinstance(mw_cfg.get("sso"), dict) else {}).get("netbox_field")
                or (mw_cfg.get("sso_protected", {}) if isinstance(mw_cfg.get("sso_protected"), dict) else {}).get("netbox_field")
                or legacy_cf.get("sso_protected", "sso_protected")
            )
            field_whitelist = (
                (mw_cfg.get("ip_whitelist", {}) if isinstance(mw_cfg.get("ip_whitelist"), dict) else {}).get("netbox_field")
                or (mw_cfg.get("whitelist", {}) if isinstance(mw_cfg.get("whitelist"), dict) else {}).get("netbox_field")
                or legacy_cf.get("ip_whitelist", "ip_whitelist")
            )

            existing_by_fqdn = {}
            existing_by_name = {}
            for svc in existing_list:
                cfields = svc.get("custom_fields", {})
                fqdn_val = cfields.get(field_fqdn) if field_fqdn else None
                if not fqdn_val:
                    fqdn_val = cfields.get("fqdn")
                if fqdn_val:
                    for f_part in str(fqdn_val).split(","):
                        existing_by_fqdn[f_part.strip().lower()] = svc
                existing_by_name[svc.get("name", "").lower()] = svc

            for r in routes:
                primary_fqdn = r["primary_fqdn"].lower()
                existing_svc = existing_by_fqdn.get(primary_fqdn) or existing_by_name.get(r["name"].lower())

                custom_fields_payload = {}
                if field_fqdn:
                    custom_fields_payload[field_fqdn] = r["fqdn"]
                if field_public_url:
                    custom_fields_payload[field_public_url] = r["public_url"]
                if field_sso:
                    custom_fields_payload[field_sso] = r["sso_protected"]
                if field_whitelist:
                    custom_fields_payload[field_whitelist] = r["ip_whitelist"]
                if field_middlewares:
                    custom_fields_payload[field_middlewares] = r["middlewares"]

                service_payload = {
                    "name": r["name"],
                    "parent_object_type": parent_type,
                    "parent_object_id": parent_id,
                    "protocol": r["protocol"],
                    "ports": r["ports"],
                    "description": r["description"],
                    "tags": r["tags"],
                    "custom_fields": custom_fields_payload,
                }

                if existing_svc:
                    svc_id = existing_svc["id"]
                    curr_name = existing_svc.get("name", "")
                    curr_desc = existing_svc.get("description", "")
                    curr_ports = existing_svc.get("ports", [])
                    curr_cf = existing_svc.get("custom_fields", {})
                    curr_tags = [t.get("slug") for t in existing_svc.get("tags", [])]
                    new_tags = [t["slug"] for t in r["tags"]]

                    needs_update = (
                        curr_name != r["name"]
                        or curr_desc != r["description"]
                        or curr_ports != r["ports"]
                        or (field_public_url and curr_cf.get(field_public_url) != r["public_url"])
                        or (field_fqdn and curr_cf.get(field_fqdn) != r["fqdn"])
                        or (field_sso and curr_cf.get(field_sso) != r["sso_protected"])
                        or (field_whitelist and curr_cf.get(field_whitelist) != r["ip_whitelist"])
                        or (field_middlewares and curr_cf.get(field_middlewares) != r["middlewares"])
                        or set(curr_tags) != set(new_tags)
                    )

                    if needs_update:
                        patch_resp = await client.patch(
                            f"{netbox_driver.base_url}/api/ipam/services/{svc_id}/",
                            headers=headers,
                            json={
                                "name": r["name"],
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
                            logger.info("[%s] Updated NetBox Service %s (ID: %d) on %s", instance_name, r["name"], svc_id, parent_label)
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
                        logger.info("[%s] Created NetBox Service %s (ID: %d) on %s", instance_name, r["name"], new_id, parent_label)
                    else:
                        logger.warning("[%s] Failed to create Service %s on %s: %s", instance_name, r["name"], parent_label, post_resp.text)

        except Exception as e:
            logger.exception("Error syncing Traefik routes for %s: %s", instance_name, e)
            raise

        return {
            "status": "success",
            "instance": instance_name,
            "parent_type": parent_type,
            "parent_id": parent_id,
            "netbox_vm_id": vm_id,
            "netbox_device_id": device_id,
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
                {"name": "traefik-local", "netbox_vm_id": 1, "type": "docker", "path": "/etc/traefik", "conf_dir": "/etc/traefik/conf"},
                {"name": "traefik-remote", "netbox_device_id": 1, "type": "api", "api_url": "http://192.168.1.50:8080"},
            ]

        results = {}
        for inst in instances:
            name = inst.get("name", "unknown")
            vm_id = inst.get("netbox_vm_id")
            device_id = inst.get("netbox_device_id")
            if not vm_id and not device_id:
                logger.warning("Traefik instance '%s' is missing both netbox_vm_id and netbox_device_id in config.yml. Skipping.", name)
                continue
            try:
                res = await self.sync_instance(name, netbox_vm_id=vm_id, netbox_device_id=device_id, instance_conf=inst)
                results[name] = res
            except Exception as e:
                logger.error("Failed to sync Traefik instance '%s': %s", name, e)
                results[name] = {"status": "error", "error": str(e)}

        return results


traefik_sync_driver = TraefikSyncDriver()
