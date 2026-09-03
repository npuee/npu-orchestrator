import re
import logging
from typing import List, Dict, Any, Optional
from app.drivers.proxmox import proxmox_driver
from app.drivers.netbox import netbox_driver

logger = logging.getLogger("orchestrator.drivers.template_sync")


class TemplateSyncDriver:
    """
    Automated discovery, synchronization, and lifecycle reconciliation engine between
    Proxmox VE templates (QEMU VMs and LXC Containers) and NetBox Platforms.
    """

    def parse_ct_template_filename(self, volid: str) -> Dict[str, str]:
        """
        Parses an LXC vztmpl filename into OS family, version, and formatted NetBox platform details.
        Example volid: 'backups:vztmpl/ubuntu-24.04-standard_24.04-2_amd64.tar.zst'
        """
        filename = volid.split("/")[-1].lower()

        # Ubuntu matching: ubuntu-24.04-standard...
        m_ubuntu = re.search(r"ubuntu-(\d+\.\d+)", filename)
        if m_ubuntu:
            ver = m_ubuntu.group(1)
            codename = "LTS"
            if ver == "24.04":
                codename = "LTS (Noble)"
            elif ver == "22.04":
                codename = "LTS (Jammy)"
            elif ver == "26.04":
                codename = "LTS (Resolute)"
            return {
                "os_family": "Ubuntu",
                "version": ver,
                "platform_name": f"Ubuntu {ver} {codename} LXC",
                "platform_slug": f"pve-lxc-ubuntu-{ver.replace('.', '-')}",
                "short_name": f"Ubuntu {ver} LXC",
            }

        # Debian matching: debian-12-standard...
        m_debian = re.search(r"debian-(\d+)", filename)
        if m_debian:
            ver = m_debian.group(1)
            codename = {"11": "Bullseye", "12": "Bookworm", "13": "Trixie"}.get(ver, "")
            name_suffix = f" ({codename})" if codename else ""
            return {
                "os_family": "Debian",
                "version": ver,
                "platform_name": f"Debian {ver}{name_suffix} LXC",
                "platform_slug": f"pve-lxc-debian-{ver}",
                "short_name": f"Debian {ver} LXC",
            }

        # Alpine matching: alpine-3.20-default...
        m_alpine = re.search(r"alpine-(\d+\.\d+)", filename)
        if m_alpine:
            ver = m_alpine.group(1)
            return {
                "os_family": "Alpine",
                "version": ver,
                "platform_name": f"Alpine Linux {ver} LXC",
                "platform_slug": f"pve-lxc-alpine-{ver.replace('.', '-')}",
                "short_name": f"Alpine {ver} LXC",
            }

        # Rocky Linux: rockylinux-9-...
        m_rocky = re.search(r"rocky(?:linux)?-(\d+)", filename)
        if m_rocky:
            ver = m_rocky.group(1)
            return {
                "os_family": "Rocky Linux",
                "version": ver,
                "platform_name": f"Rocky Linux {ver} LXC",
                "platform_slug": f"pve-lxc-rocky-{ver}",
                "short_name": f"Rocky {ver} LXC",
            }

        # AlmaLinux: almalinux-9-...
        m_alma = re.search(r"alma(?:linux)?-(\d+)", filename)
        if m_alma:
            ver = m_alma.group(1)
            return {
                "os_family": "AlmaLinux",
                "version": ver,
                "platform_name": f"AlmaLinux {ver} LXC",
                "platform_slug": f"pve-lxc-alma-{ver}",
                "short_name": f"Alma {ver} LXC",
            }

        # Arch Linux: archlinux-...
        if "archlinux" in filename or "arch" in filename:
            return {
                "os_family": "Arch Linux",
                "version": "Rolling",
                "platform_name": "Arch Linux LXC",
                "platform_slug": "pve-lxc-arch",
                "short_name": "Arch Linux LXC",
            }

        # Fedora: fedora-40-...
        m_fedora = re.search(r"fedora-(\d+)", filename)
        if m_fedora:
            ver = m_fedora.group(1)
            return {
                "os_family": "Fedora",
                "version": ver,
                "platform_name": f"Fedora {ver} LXC",
                "platform_slug": f"pve-lxc-fedora-{ver}",
                "short_name": f"Fedora {ver} LXC",
            }

        # Generic fallback
        clean_name = filename.split("_")[0].replace("-", " ").title()
        slug = f"pve-lxc-{re.sub(r'[^a-z0-9]+', '-', clean_name.lower()).strip('-')}"[:50]
        return {
            "os_family": clean_name,
            "version": "Unknown",
            "platform_name": f"{clean_name} LXC",
            "platform_slug": slug,
            "short_name": clean_name,
        }

    def discover_ct_templates(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Scans all storage pools on the Proxmox cluster node where content='vztmpl'.
        Returns discovered templates with parsed OS metadata.
        """
        pve = proxmox_driver.get_client()
        target_node = proxmox_driver.resolve_node(node)
        discovered = []

        try:
            storages = pve.nodes(target_node).storage.get()
            for s in storages:
                content = s.get("content", "")
                sname = s.get("storage")
                if "vztmpl" in content:
                    try:
                        items = pve.nodes(target_node).storage(sname).content.get(content="vztmpl")
                        for it in items:
                            volid = it.get("volid", "")
                            if not volid:
                                continue
                            parsed = self.parse_ct_template_filename(volid)
                            discovered.append({
                                "volid": volid,
                                "storage": sname,
                                "size_bytes": it.get("size", 0),
                                "format": it.get("format", ""),
                                "node": target_node,
                                **parsed,
                            })
                    except Exception as exc:
                        logger.warning("Error querying vztmpl on storage '%s': %s", sname, exc)
        except Exception as e:
            logger.error("Failed to discover Proxmox CT templates: %s", e)

        return discovered

    def discover_vm_templates(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries Proxmox cluster nodes for QEMU VM templates.
        Returns templates formatted for NetBox Platform registration.
        """
        raw_templates = proxmox_driver.list_templates(node)
        discovered = []
        for t in raw_templates:
            vmid = t["vmid"]
            raw_name = t["name"]
            cat = t.get("category", "linux")

            # Format a clean human-readable name: e.g. "Ubuntu 24.04 (VMID: 9024)"
            display_name = raw_name.replace("-", " ").title()
            if "24.04" in raw_name:
                display_name = "Ubuntu 24.04 LTS (Noble)"
            elif "26.04" in raw_name:
                display_name = "Ubuntu 26.04 LTS (Resolute)"
            elif "2025" in raw_name:
                display_name = "Windows Server 2025"

            clean_slug_name = re.sub(r"[^a-z0-9]+", "-", raw_name.lower()).strip("-")
            slug = f"pve-vm-{vmid}-{clean_slug_name}"[:50]

            discovered.append({
                "vmid": vmid,
                "name": f"{display_name} (VMID: {vmid})",
                "slug": slug,
                "node": t.get("node"),
                "category": cat,
                "description": f"[Proxmox VM Template: {vmid}] {display_name} (Node: {t.get('node')})",
            })
        return discovered

    async def sync_all_templates(self, node: Optional[str] = None) -> Dict[str, Any]:
        """
        Unified Proxmox ➔ NetBox Platform Synchronization & Lifecycle Engine.
        1. Auto-discovers QEMU VM templates & LXC CT templates on Proxmox.
        2. Auto-registers or updates Platforms in NetBox.
        3. Detects orphaned platforms when templates are deleted from Proxmox:
           - Deletes platform if 0 VMs are using it.
           - Marks [Deprecated] if existing VMs still reference it.
        """
        # Discover templates from Proxmox
        vm_templates = await asyncio.to_thread(self.discover_vm_templates, node)
        ct_templates = await asyncio.to_thread(self.discover_ct_templates, node)

        active_vmids = {t["vmid"] for t in vm_templates}
        active_volids = {t["volid"] for t in ct_templates}

        # Fetch existing NetBox platforms
        existing_platforms = await netbox_driver.get_platforms()
        platform_by_slug = {p["slug"]: p for p in existing_platforms}
        platform_by_name = {p["name"].lower(): p for p in existing_platforms}

        # Map by Proxmox metadata if present in description
        platform_by_vmid: Dict[int, Dict[str, Any]] = {}
        platform_by_volid: Dict[str, Dict[str, Any]] = {}
        for p in existing_platforms:
            desc = p.get("description") or ""
            m_v = re.search(r"\[Proxmox VM Template:\s*(\d+)\]", desc)
            if m_v:
                platform_by_vmid[int(m_v.group(1))] = p
            m_c = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", desc)
            if m_c:
                platform_by_volid[m_c.group(1).strip()] = p

        platforms_created = []
        platforms_updated = []
        platforms_deleted = []
        platforms_deprecated = []

        # ── 1. Reconcile QEMU VM Templates ─────────────────────────────────────
        for t in vm_templates:
            vmid = t["vmid"]
            p_name = t["name"]
            p_slug = t["slug"]
            p_desc = t["description"]

            existing = platform_by_vmid.get(vmid) or platform_by_slug.get(p_slug)
            if not existing:
                logger.info("Creating NetBox Platform for Proxmox VM template: %s", p_name)
                created = await netbox_driver.create_platform(name=p_name, slug=p_slug, description=p_desc)
                if created:
                    platforms_created.append({"name": p_name, "vmid": vmid, "type": "vm"})
            else:
                # If deprecated previously, restore name
                curr_name = existing.get("name", "")
                if curr_name.startswith("[Deprecated]"):
                    restored_name = curr_name.replace("[Deprecated] ", "").strip()
                    await netbox_driver.update_platform(existing["id"], {"name": restored_name, "description": p_desc})
                    platforms_updated.append({"name": restored_name, "vmid": vmid, "status": "restored"})

        # ── 2. Reconcile LXC CT Templates ──────────────────────────────────────
        for t in ct_templates:
            volid = t["volid"]
            p_name = t["platform_name"]
            p_slug = t["platform_slug"]
            p_desc = f"[Proxmox LXC Template: {volid}]"

            existing = platform_by_volid.get(volid) or platform_by_slug.get(p_slug)
            if not existing:
                logger.info("Creating NetBox Platform for Proxmox LXC template: %s", p_name)
                created = await netbox_driver.create_platform(name=p_name, slug=p_slug, description=p_desc)
                if created:
                    platforms_created.append({"name": p_name, "volid": volid, "type": "lxc"})
            else:
                # Update description if missing template tag
                curr_desc = existing.get("description") or ""
                if "[Proxmox LXC Template:" not in curr_desc or curr_desc.startswith("[Deprecated]"):
                    await netbox_driver.update_platform(existing["id"], {
                        "name": p_name,
                        "description": p_desc,
                    })
                    platforms_updated.append({"name": p_name, "volid": volid, "status": "updated"})

        # ── 3. Reconcile Orphaned NetBox Platforms ─────────────────────────────
        # Refresh platforms list to evaluate deletions
        current_platforms = await netbox_driver.get_platforms()
        for p in current_platforms:
            desc = p.get("description") or ""
            p_name = p.get("name", "")
            p_id = p.get("id")
            vm_usage = p.get("virtualmachine_count", 0) + p.get("device_count", 0)

            # Check VM template orphans
            m_v = re.search(r"\[Proxmox VM Template:\s*(\d+)\]", desc)
            if m_v:
                vmid = int(m_v.group(1))
                if vmid not in active_vmids:
                    if vm_usage == 0:
                        deleted = await netbox_driver.delete_platform(p_id)
                        if deleted:
                            platforms_deleted.append({"name": p_name, "vmid": vmid, "type": "vm"})
                            logger.info("Deleted orphaned NetBox Platform '%s' (VMID %d removed from Proxmox)", p_name, vmid)
                    elif not p_name.startswith("[Deprecated]"):
                        dep_name = f"[Deprecated] {p_name}"[:100]
                        dep_desc = f"[⚠️ Proxmox template {vmid} deleted] {desc}"[:200]
                        await netbox_driver.update_platform(p_id, {"name": dep_name, "description": dep_desc})
                        platforms_deprecated.append({"name": p_name, "active_vms": vm_usage, "type": "vm"})

            # Check LXC template orphans
            m_c = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", desc)
            if m_c:
                volid = m_c.group(1).strip()
                if volid not in active_volids:
                    if vm_usage == 0:
                        deleted = await netbox_driver.delete_platform(p_id)
                        if deleted:
                            platforms_deleted.append({"name": p_name, "volid": volid, "type": "lxc"})
                            logger.info("Deleted orphaned NetBox Platform '%s' (LXC %s removed from Proxmox)", p_name, volid)
                    elif not p_name.startswith("[Deprecated]"):
                        dep_name = f"[Deprecated] {p_name}"[:100]
                        dep_desc = f"[⚠️ Proxmox template deleted] {desc}"[:200]
                        await netbox_driver.update_platform(p_id, {"name": dep_name, "description": dep_desc})
                        platforms_deprecated.append({"name": p_name, "active_vms": vm_usage, "type": "lxc"})

        return {
            "status": "success",
            "discovered_vm_templates": len(vm_templates),
            "discovered_lxc_templates": len(ct_templates),
            "platforms_created": platforms_created,
            "platforms_updated": platforms_updated,
            "platforms_deleted": platforms_deleted,
            "platforms_deprecated": platforms_deprecated,
            "summary": (
                f"Synchronized {len(vm_templates)} VM template(s) and {len(ct_templates)} LXC template(s). "
                f"Created {len(platforms_created)}, deleted {len(platforms_deleted)} orphaned, "
                f"deprecated {len(platforms_deprecated)}."
            ),
        }

    async def sync_ct_templates(self, node: Optional[str] = None) -> Dict[str, Any]:
        """Legacy alias routing to sync_all_templates."""
        return await self.sync_all_templates(node)


template_sync_driver = TemplateSyncDriver()
