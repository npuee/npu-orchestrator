import logging
import httpx
from typing import Optional, Dict, Any, List
from app.core.config import settings
from app.core.app_config import app_config

logger = logging.getLogger("orchestrator.netbox")


class NetBoxDriver:
    def __init__(self):
        self.base_url = settings.NETBOX_URL.rstrip("/") if settings.NETBOX_URL else None
        self.token = settings.NETBOX_TOKEN
        self._client: Optional[httpx.AsyncClient] = None

    def is_configured(self) -> bool:
        return bool(self.base_url and self.token)

    def _get_client(self) -> httpx.AsyncClient:
        """Returns a shared, pooled httpx.AsyncClient with HTTP Keep-Alive connection pooling."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=5.0),
                limits=httpx.Limits(
                    max_keepalive_connections=10,
                    max_connections=20,
                    keepalive_expiry=30.0,
                ),
            )
        return self._client

    async def close(self):
        """Gracefully closes the pooled HTTP client session."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def update_virtual_machine(
        self,
        vm_id: int,
        status: Optional[str] = None,
        start_on_boot: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        comments: Optional[str] = None,
        tenant: Optional[int] = None,
        site: Optional[int] = None,
        cluster: Optional[int] = None,
        vcpus: Optional[int] = None,
        memory: Optional[int] = None,
        role: Optional[int] = None,
    ) -> bool:
        """
        Updates a NetBox VirtualMachine record (e.g., setting status, start_on_boot, custom fields, tenant, site, cluster, vcpus, memory, role).
        """
        if not self.is_configured():
            logger.debug("NetBox integration not configured, skipping status update")
            return False

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload: Dict[str, Any] = {}
        if status is not None:
            payload["status"] = status
        if start_on_boot is not None:
            payload["start_on_boot"] = start_on_boot
        if custom_fields is not None:
            payload["custom_fields"] = custom_fields
        if comments is not None:
            payload["comments"] = comments
        if tenant is not None:
            payload["tenant"] = tenant
        if site is not None:
            payload["site"] = site
        if cluster is not None:
            payload["cluster"] = cluster
        if vcpus is not None:
            payload["vcpus"] = vcpus
        if memory is not None:
            payload["memory"] = memory
        if role is not None:
            payload["role"] = role

        url = f"{self.base_url}/api/virtualization/virtual-machines/{vm_id}/"
        
        try:
            client = self._get_client()
            resp = await client.patch(url, headers=headers, json=payload)
            if resp.status_code in (200, 201):
                logger.info("Updated NetBox VM ID %d successfully", vm_id)
                return True
            logger.warning(
                "Failed to update NetBox VM ID %d: HTTP %d %s",
                vm_id,
                resp.status_code,
                resp.text,
            )
            return False
        except Exception as e:
            logger.error("Exception updating NetBox VM %d: %s", vm_id, e)
            return False

    async def add_journal_entry(self, assigned_object_type: str, assigned_object_id: int, comment: str) -> bool:
        """
        Adds an audit journal entry to a NetBox object.
        assigned_object_type: e.g. "virtualization.virtualmachine"
        """
        if not self.is_configured():
            return False

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "assigned_object_type": assigned_object_type,
            "assigned_object_id": assigned_object_id,
            "comments": comment,
            "kind": "info",
        }
        url = f"{self.base_url}/api/extras/journal-entries/"

        try:
            client = self._get_client()
            resp = await client.post(url, headers=headers, json=payload)
            return resp.status_code in (200, 201)
        except Exception as e:
            logger.warning("Could not create NetBox journal entry: %s", e)
            return False

    async def ensure_vm_interface_and_ip(
        self,
        vm_id: int,
        hostname: str,
        ip_address: str,
        interface_name: str = "eth0",
        domain: Optional[str] = None,
    ) -> bool:
        """
        Ensures a VM has an interface (e.g. eth0), creates/assigns the IP address in NetBox IPAM,
        sets the IP's DNS name, and sets it as the primary IPv4 address.
        """
        if not self.is_configured():
            return False

        domain = domain or app_config.dns.get("default_zone", "homelab.local")

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        ip_cidr = ip_address if "/" in ip_address else f"{ip_address}/24"
        dns_fqdn = f"{hostname}.{domain}".lower()

        try:
            client = self._get_client()
            # 1. Check existing interfaces for this VM
            iface_id = None
            resp_ifaces = await client.get(
                f"{self.base_url}/api/virtualization/interfaces/?virtual_machine_id={vm_id}",
                headers=headers,
            )
            if resp_ifaces.status_code == 200:
                results = resp_ifaces.json().get("results", [])
                if results:
                    iface_id = results[0]["id"]

            # 2. Create interface if none exists
            if not iface_id:
                iface_payload = {
                    "virtual_machine": vm_id,
                    "name": interface_name,
                    "type": "virtual",
                    "enabled": True,
                }
                resp_create_iface = await client.post(
                    f"{self.base_url}/api/virtualization/interfaces/",
                    headers=headers,
                    json=iface_payload,
                )
                if resp_create_iface.status_code in (200, 201):
                    iface_id = resp_create_iface.json().get("id")
                    logger.info("Created interface '%s' (ID: %d) on NetBox VM %d", interface_name, iface_id, vm_id)
                else:
                    logger.warning("Could not create interface on NetBox VM %d: %s", vm_id, resp_create_iface.text)

            if not iface_id:
                return False

            # 3. Create or find IP address in IPAM with dns_name
            ip_id = None
            resp_ip = await client.get(
                f"{self.base_url}/api/ipam/ip-addresses/?address={ip_cidr}",
                headers=headers,
            )
            if resp_ip.status_code == 200 and resp_ip.json().get("results"):
                ip_id = resp_ip.json()["results"][0]["id"]
                # Update assignment and dns_name
                await client.patch(
                    f"{self.base_url}/api/ipam/ip-addresses/{ip_id}/",
                    headers=headers,
                    json={
                        "assigned_object_type": "virtualization.vminterface",
                        "assigned_object_id": iface_id,
                        "dns_name": dns_fqdn,
                        "description": f"Primary IP for {dns_fqdn}",
                        "status": "active",
                    },
                )
            else:
                ip_payload = {
                    "address": ip_cidr,
                    "assigned_object_type": "virtualization.vminterface",
                    "assigned_object_id": iface_id,
                    "dns_name": dns_fqdn,
                    "description": f"Primary IP for {dns_fqdn}",
                    "status": "active",
                }
                resp_create_ip = await client.post(
                    f"{self.base_url}/api/ipam/ip-addresses/",
                    headers=headers,
                    json=ip_payload,
                )
                if resp_create_ip.status_code in (200, 201):
                    ip_id = resp_create_ip.json().get("id")
                    logger.info("Assigned IP %s (%s, ID: %d) to interface %d on VM %d", ip_cidr, dns_fqdn, ip_id, iface_id, vm_id)

            # 4. Set primary_ip4 on the VM
            if ip_id:
                await client.patch(
                    f"{self.base_url}/api/virtualization/virtual-machines/{vm_id}/",
                    headers=headers,
                    json={"primary_ip4": ip_id},
                )
                logger.info("Set primary_ip4 on NetBox VM %d to %s (%s)", vm_id, ip_cidr, dns_fqdn)
                return True

        except Exception as e:
            logger.error("Exception ensuring interface and IP for NetBox VM %d: %s", vm_id, e)

        return False

    async def get_or_allocate_available_ip(
        self,
        prefix_cidr: Optional[str] = None,
        hostname: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[str]:
        """
        Atomically allocates the next available IP address from the specified NetBox IPAM prefix.
        Returns the clean IP address string (e.g. '192.168.1.50').
        """
        if not self.is_configured():
            return None

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            client = self._get_client()
            target_prefix = prefix_cidr or app_config.defaults.get("subnet")
            if not target_prefix:
                site_id = app_config.defaults.get("site_id")
                resp_s = await client.get(f"{self.base_url}/api/ipam/prefixes/?site_id={site_id}&status=active", headers=headers)
                if resp_s.status_code == 200:
                    res_s = resp_s.json().get("results", [])
                    if res_s:
                        target_prefix = res_s[0].get("prefix")

            if not target_prefix:
                target_prefix = "192.168.1.0/24"

            # 1. Resolve Prefix ID
            resp_p = await client.get(
                f"{self.base_url}/api/ipam/prefixes/?prefix={target_prefix}",
                headers=headers,
            )
            prefix_id = None
            if resp_p.status_code == 200:
                results = resp_p.json().get("results", [])
                if results:
                    prefix_id = results[0]["id"]

            if not prefix_id:
                logger.warning("Could not find NetBox IPAM prefix '%s'", target_prefix)
                return None

            # 2. Atomically allocate next available IP
            desc = description or (f"Provisioned for {hostname}" if hostname else "NPU Orchestrator Allocation")
            payload = {
                "description": desc,
                "status": "active",
            }
            if hostname:
                default_domain = app_config.dns.get("default_zone")
                if default_domain:
                    payload["dns_name"] = f"{hostname}.{default_domain}".lower()
                else:
                    payload["dns_name"] = hostname.lower()

            resp_alloc = await client.post(
                f"{self.base_url}/api/ipam/prefixes/{prefix_id}/available-ips/",
                headers=headers,
                json=payload,
            )
            if resp_alloc.status_code in (200, 201):
                alloc_data = resp_alloc.json()
                raw_address = alloc_data.get("address", "")
                clean_ip = raw_address.split("/")[0] if raw_address else None
                logger.info("Allocated next available IP '%s' (ID: %s) from NetBox IPAM prefix '%s'", clean_ip, alloc_data.get("id"), prefix_cidr)
                return clean_ip
            else:
                logger.warning("Failed to allocate available IP from prefix %d: %s", prefix_id, resp_alloc.text)
                return None
        except Exception as exc:
            logger.warning("Error allocating available IP from NetBox: %s", exc)
            return None

    async def create_or_update_dns_record(
        self,
        hostname: str,
        ip_address: str,
        zone_name: Optional[str] = None,
    ) -> bool:
        """
        Creates or updates a DNS A record in NetBox DNS plugin for zone_name.
        Also automatically provisions the PTR reverse record in the corresponding reverse zone.
        """
        if not self.is_configured():
            return False

        zone_name = zone_name or app_config.dns.get("default_zone", "homelab.local")

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        ip_clean = ip_address.split("/")[0]

        try:
            client = self._get_client()
            # 1. Resolve Zone ID for zone_name
            zone_id = None
            resp_zone = await client.get(
                f"{self.base_url}/api/plugins/netbox-dns/zones/?name={zone_name}",
                headers=headers,
            )
            if resp_zone.status_code == 200:
                results = resp_zone.json().get("results", [])
                if results:
                    zone_id = results[0]["id"]

            if not zone_id:
                logger.warning("DNS zone '%s' not found in NetBox DNS plugin", zone_name)
                return False

            # 2. Check if record already exists
            resp_rec = await client.get(
                f"{self.base_url}/api/plugins/netbox-dns/records/?zone_id={zone_id}&name={hostname}&type=A",
                headers=headers,
            )
            existing_records = resp_rec.json().get("results", []) if resp_rec.status_code == 200 else []

            record_payload = {
                "zone": zone_id,
                "name": hostname,
                "type": "A",
                "value": ip_clean,
                "status": "active",
                "ttl": 3600,
                "disable_ptr": False,
                "description": f"Auto-synced for {hostname}",
            }

            if existing_records:
                rec_id = existing_records[0]["id"]
                resp_upd = await client.patch(
                    f"{self.base_url}/api/plugins/netbox-dns/records/{rec_id}/",
                    headers=headers,
                    json=record_payload,
                )
                if resp_upd.status_code in (200, 201):
                    logger.info("Updated DNS A record '%s.%s' -> %s (ID: %d)", hostname, zone_name, ip_clean, rec_id)
                    return True
            else:
                resp_crt = await client.post(
                    f"{self.base_url}/api/plugins/netbox-dns/records/",
                    headers=headers,
                    json=record_payload,
                )
                if resp_crt.status_code in (200, 201):
                    rec_id = resp_crt.json().get("id")
                    logger.info("Created DNS A record '%s.%s' -> %s (ID: %d)", hostname, zone_name, ip_clean, rec_id)
                    return True
                else:
                    logger.warning("Could not create DNS A record for %s: %s", hostname, resp_crt.text)

        except Exception as e:
            logger.error("Exception managing NetBox DNS record for %s: %s", hostname, e)

        return False

    async def delete_dns_record(
        self,
        hostname: str,
        zone_name: Optional[str] = None,
    ) -> bool:
        """
        Deletes the DNS A record and associated PTR records for hostname in NetBox DNS.
        """
        if not self.is_configured():
            return False

        zone_name = zone_name or app_config.dns.get("default_zone", "homelab.local")

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            client = self._get_client()
            resp_zone = await client.get(
                f"{self.base_url}/api/plugins/netbox-dns/zones/?name={zone_name}",
                headers=headers,
            )
            if resp_zone.status_code == 200 and resp_zone.json().get("results"):
                zone_id = resp_zone.json()["results"][0]["id"]
                resp_rec = await client.get(
                    f"{self.base_url}/api/plugins/netbox-dns/records/?zone_id={zone_id}&name={hostname}",
                    headers=headers,
                )
                if resp_rec.status_code == 200:
                    for rec in resp_rec.json().get("results", []):
                        rec_id = rec["id"]
                        await client.delete(
                            f"{self.base_url}/api/plugins/netbox-dns/records/{rec_id}/",
                            headers=headers,
                        )
                        logger.info("Deleted DNS record %s (ID: %d)", hostname, rec_id)
                    return True
        except Exception as e:
            logger.error("Exception deleting NetBox DNS record for %s: %s", hostname, e)

        return False

    async def rename_dns_record(
        self,
        old_hostname: str,
        new_hostname: str,
        ip_address: Optional[str] = None,
        zone_name: Optional[str] = None,
    ) -> bool:
        """
        Renames a DNS A record (and its PTR record) from old_hostname to new_hostname.
        """
        if not self.is_configured():
            return False

        zone_name = zone_name or app_config.dns.get("default_zone", "homelab.local")

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            client = self._get_client()
            resp_zone = await client.get(
                f"{self.base_url}/api/plugins/netbox-dns/zones/?name={zone_name}",
                headers=headers,
            )
            if not (resp_zone.status_code == 200 and resp_zone.json().get("results")):
                logger.warning("DNS zone '%s' not found during rename", zone_name)
                return False
            zone_id = resp_zone.json()["results"][0]["id"]

            # Find old record
            resp_old = await client.get(
                f"{self.base_url}/api/plugins/netbox-dns/records/?zone_id={zone_id}&name={old_hostname}&type=A",
                headers=headers,
            )
            target_ip = ip_address
            if resp_old.status_code == 200:
                old_records = resp_old.json().get("results", [])
                for rec in old_records:
                    if not target_ip and rec.get("value"):
                        target_ip = rec.get("value")
                    await client.delete(
                        f"{self.base_url}/api/plugins/netbox-dns/records/{rec['id']}/",
                        headers=headers,
                    )
                    logger.info("Deleted old DNS A record '%s' (ID: %d)", old_hostname, rec["id"])

            if target_ip:
                # Create new DNS record
                return await self.create_or_update_dns_record(
                    hostname=new_hostname,
                    ip_address=target_ip,
                    zone_name=zone_name,
                )
            else:
                logger.warning("No IP address available to create renamed DNS record for '%s'", new_hostname)
                return False

        except Exception as e:
            logger.error("Exception renaming DNS record '%s' -> '%s': %s", old_hostname, new_hostname, e)
            return False

    async def cleanup_vm_ip_and_interfaces(self, vm_id: int) -> bool:
        """
        Frees/deletes the IP addresses and interfaces associated with a decommissioned VM.
        """
        if not self.is_configured():
            return False

        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            client = self._get_client()
            resp_ifaces = await client.get(
                f"{self.base_url}/api/virtualization/interfaces/?virtual_machine_id={vm_id}",
                headers=headers,
            )
            if resp_ifaces.status_code == 200:
                for iface in resp_ifaces.json().get("results", []):
                    iface_id = iface["id"]
                    # Delete interface (NetBox also disassociates assigned IP addresses)
                    await client.delete(
                        f"{self.base_url}/api/virtualization/interfaces/{iface_id}/",
                        headers=headers,
                    )
                    logger.info("Deleted interface ID %d from VM %d", iface_id, vm_id)
            return True
        except Exception as e:
            logger.error("Exception cleaning up interfaces for VM %d: %s", vm_id, e)

        return False

    async def get_virtual_machine_type(self, type_id: int) -> Optional[Dict[str, Any]]:
        """Fetches details of a VirtualMachineType by ID."""
        if not self.is_configured():
            return None
        headers = {
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        }
        try:
            client = self._get_client()
            resp = await client.get(
                f"{self.base_url}/api/virtualization/virtual-machine-types/{type_id}/",
                headers=headers,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning("Could not fetch VirtualMachineType %d: %s", type_id, e)
        return None

    async def get_platforms(self) -> List[Dict[str, Any]]:
        """Fetches all platforms from NetBox."""
        if not self.is_configured():
            return []
        headers = {"Authorization": f"Token {self.token}", "Accept": "application/json"}
        try:
            client = self._get_client()
            resp = await client.get(f"{self.base_url}/api/dcim/platforms/?limit=100", headers=headers)
            if resp.status_code == 200:
                return resp.json().get("results", [])
        except Exception as e:
            logger.warning("Could not fetch NetBox platforms: %s", e)
        return []

    async def create_platform(self, name: str, slug: str, description: str = "") -> Optional[Dict[str, Any]]:
        """Creates a new Platform in NetBox."""
        if not self.is_configured():
            return None
        headers = {"Authorization": f"Token {self.token}", "Content-Type": "application/json"}
        payload = {"name": name, "slug": slug, "description": description}
        try:
            client = self._get_client()
            resp = await client.post(f"{self.base_url}/api/dcim/platforms/", json=payload, headers=headers)
            if resp.status_code in (200, 201):
                return resp.json()
            logger.warning("Failed to create platform %s (HTTP %d): %s", name, resp.status_code, resp.text)
        except Exception as e:
            logger.error("Error creating platform %s: %s", name, e)
        return None

    async def update_platform(self, platform_id: int, data: Dict[str, Any]) -> bool:
        """Updates an existing Platform in NetBox."""
        if not self.is_configured():
            return False
        headers = {"Authorization": f"Token {self.token}", "Content-Type": "application/json"}
        try:
            client = self._get_client()
            resp = await client.patch(f"{self.base_url}/api/dcim/platforms/{platform_id}/", json=data, headers=headers)
            return resp.status_code in (200, 201)
        except Exception as e:
            logger.error("Error updating platform %d: %s", platform_id, e)
        return False

    async def delete_platform(self, platform_id: int) -> bool:
        """Deletes a Platform from NetBox."""
        if not self.is_configured():
            return False
        headers = {"Authorization": f"Token {self.token}"}
        try:
            client = self._get_client()
            resp = await client.delete(f"{self.base_url}/api/dcim/platforms/{platform_id}/", headers=headers)
            return resp.status_code in (200, 204)
        except Exception as e:
            logger.error("Error deleting platform %d: %s", platform_id, e)
        return False

    async def get_virtual_machine_types(self) -> List[Dict[str, Any]]:
        """Fetches all virtual machine types / blueprints from NetBox."""
        if not self.is_configured():
            return []
        headers = {"Authorization": f"Token {self.token}", "Accept": "application/json"}
        try:
            client = self._get_client()
            resp = await client.get(f"{self.base_url}/api/virtualization/virtual-machine-types/?limit=100", headers=headers)
            if resp.status_code == 200:
                return resp.json().get("results", [])
        except Exception as e:
            logger.warning("Could not fetch NetBox virtual machine types: %s", e)
        return []

    async def create_virtual_machine_type(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Creates a new VirtualMachineType in NetBox."""
        if not self.is_configured():
            return None
        headers = {"Authorization": f"Token {self.token}", "Content-Type": "application/json"}
        try:
            client = self._get_client()
            resp = await client.post(f"{self.base_url}/api/virtualization/virtual-machine-types/", json=data, headers=headers)
            if resp.status_code in (200, 201):
                return resp.json()
            logger.warning("Failed to create virtual machine type %s (HTTP %d): %s", data.get("name"), resp.status_code, resp.text)
        except Exception as e:
            logger.error("Error creating virtual machine type: %s", e)
        return None

    async def update_virtual_machine_type(self, type_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Updates a VirtualMachineType in NetBox via PATCH."""
        if not self.is_configured():
            return None
        headers = {"Authorization": f"Token {self.token}", "Content-Type": "application/json"}
        try:
            client = self._get_client()
            resp = await client.patch(f"{self.base_url}/api/virtualization/virtual-machine-types/{type_id}/", json=data, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("Failed to update virtual machine type %d (HTTP %d): %s", type_id, resp.status_code, resp.text)
        except Exception as e:
            logger.error("Error updating virtual machine type %d: %s", type_id, e)
        return None

    async def delete_virtual_machine_type(self, type_id: int) -> bool:
        """Deletes a VirtualMachineType in NetBox."""
        if not self.is_configured():
            return False
        headers = {"Authorization": f"Token {self.token}"}
        try:
            client = self._get_client()
            resp = await client.delete(f"{self.base_url}/api/virtualization/virtual-machine-types/{type_id}/", headers=headers)
            if resp.status_code in (200, 204):
                return True
            logger.warning("Failed to delete virtual machine type %d (HTTP %d): %s", type_id, resp.status_code, resp.text)
        except Exception as e:
            logger.error("Error deleting virtual machine type %d: %s", type_id, e)
        return False


netbox_driver = NetBoxDriver()
