import logging
import re
from typing import Optional, List, Dict, Any, Tuple
from app.drivers.proxmox.client import ProxmoxClientManager
from app.core.exceptions import TemplateNotFoundError

logger = logging.getLogger("orchestrator.proxmox.templates")


class ProxmoxTemplateManager:
    """Manages Proxmox template discovery, categorization, and platform correlation."""

    def __init__(self, client_mgr: ProxmoxClientManager):
        self.client_mgr = client_mgr

    def list_templates(self, node: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Discovers all template/source VMs on the cluster or a specific node, categorized into Linux and Windows.
        Convention:
        - Linux templates start with 90 (e.g. 9000-9099 / 90xx)
        - Windows templates start with 92 (e.g. 9200-9299 / 92xx)
        """
        pve = self.client_mgr.get_client()
        target_nodes = [self.client_mgr.resolve_node(node)] if node else self.client_mgr.get_online_nodes()
        if not target_nodes:
            target_nodes = [self.client_mgr.resolve_node(None)]

        templates = []
        for target_node in target_nodes:
            try:
                vms = pve.nodes(target_node).qemu.get()
                for vm in vms:
                    vmid = int(vm.get("vmid", 0))
                    vmid_str = str(vmid)
                    name = vm.get("name", f"vm-{vmid}")
                    is_template_flag = vm.get("template") == 1

                    is_linux_range = vmid_str.startswith("90")
                    is_win_range = vmid_str.startswith("92")

                    if is_template_flag or is_linux_range or is_win_range:
                        if is_win_range or "win" in name.lower() or "windows" in name.lower():
                            category = "windows"
                        else:
                            category = "linux"

                        templates.append({
                            "vmid": vmid,
                            "name": name,
                            "node": target_node,
                            "category": category,
                            "status": vm.get("status", "unknown"),
                            "cores": vm.get("cpus"),
                            "memory_mb": int(vm.get("maxmem", 0)) // (1024 * 1024) if vm.get("maxmem") else None,
                        })
            except Exception as e:
                logger.error("Error querying VMs on node %s: %s", target_node, e)

        return sorted(templates, key=lambda x: x["vmid"])

    def find_default_template(self, category: str = "linux", node: Optional[str] = None) -> Tuple[int, str]:
        """
        Finds the default/latest template for the category.
        Linux: starts with 90 (e.g. 9024, 9026)
        Windows: starts with 92 (e.g. 9225)
        """
        templates = self.list_templates(node)

        if category == "linux":
            linux_matches = [t for t in templates if str(t["vmid"]).startswith("90") or t["category"] == "linux"]
            if linux_matches:
                chosen = linux_matches[-1]
                return chosen["vmid"], chosen["name"]
            raise TemplateNotFoundError("starting with 90", category="linux", node=node)

        elif category == "windows":
            win_matches = [t for t in templates if str(t["vmid"]).startswith("92") or t["category"] == "windows"]
            if win_matches:
                chosen = win_matches[-1]
                return chosen["vmid"], chosen["name"]
            logger.warning("No Windows templates starting with 92 found, falling back to 9225")
            return 9225, "Default Windows Template"

        raise TemplateNotFoundError(f"category '{category}'", category=category, node=node)

    def resolve_template_for_platform(
        self,
        platform_slug: Optional[str] = None,
        platform_name: Optional[str] = None,
        platform_description: Optional[str] = None,
        requested_template_id: Optional[int] = None,
        node: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        """
        Correlates a NetBox Platform with the corresponding Proxmox template.
        Checks for deterministic metadata [Proxmox VM Template: <vmid>] before falling back to heuristics.
        Returns: (template_id, template_name, category)
        """
        # If user explicitly specified a template_id, check and return it
        if requested_template_id:
            templates = self.list_templates(node)
            for t in templates:
                if t["vmid"] == requested_template_id:
                    return t["vmid"], t["name"], t["category"]
            category = "windows" if str(requested_template_id).startswith("92") else "linux"
            return requested_template_id, f"template-{requested_template_id}", category

        # 0. Deterministic Match for LXC Platform
        desc_info = f"{platform_description or ''} {platform_slug or ''}"
        if "[Proxmox LXC Template:" in desc_info or (platform_slug and platform_slug.startswith("pve-lxc-")):
            m_lxc = re.search(r"\[Proxmox LXC Template:\s*([^\]]+)\]", desc_info)
            volid_name = m_lxc.group(1).strip() if m_lxc else (platform_name or "LXC Template")
            return 0, volid_name, "lxc"

        # 1. Deterministic Match from Platform Description or Slug for VM
        m_vmid = re.search(r"\[Proxmox VM Template:\s*(\d+)\]", desc_info)
        if not m_vmid:
            m_vmid = re.search(r"pve-vm-(\d+)-", desc_info)
        if m_vmid:
            target_vmid = int(m_vmid.group(1))
            templates = self.list_templates(node)
            for t in templates:
                if t["vmid"] == target_vmid:
                    return t["vmid"], t["name"], t["category"]
            cat = "windows" if str(target_vmid).startswith("92") else "linux"
            return target_vmid, f"template-{target_vmid}", cat

        combined_info = f"{platform_slug or ''} {platform_name or ''}".lower()
        templates = self.list_templates(node)

        # 1. Exact / Substring Version Match
        if "24" in combined_info or "noble" in combined_info:
            for t in templates:
                if "24" in t["name"] or t["vmid"] == 9024:
                    return t["vmid"], t["name"], "linux"

        if "26" in combined_info or "resolute" in combined_info:
            for t in templates:
                if "26" in t["name"] or t["vmid"] == 9026:
                    return t["vmid"], t["name"], "linux"

        if "2025" in combined_info or "win" in combined_info or "windows" in combined_info:
            for t in templates:
                if "2025" in t["name"] or t["vmid"] == 9225 or t["category"] == "windows":
                    return t["vmid"], t["name"], "windows"

        # 2. General Category Fallback
        if any(term in combined_info for term in ["ubuntu", "debian", "linux"]):
            tpl_id, tpl_name = self.find_default_template("linux", node)
            return tpl_id, tpl_name, "linux"
        elif any(term in combined_info for term in ["windows", "win", "server"]):
            tpl_id, tpl_name = self.find_default_template("windows", node)
            return tpl_id, tpl_name, "windows"

        # Default fallback to latest Linux template
        tpl_id, tpl_name = self.find_default_template("linux", node)
        return tpl_id, tpl_name, "linux"

    def find_lxc_template(self, node: str) -> str:
        """Find an available LXC OS template across all storage pools supporting vztmpl."""
        pve = self.client_mgr.get_client()
        target_node = self.client_mgr.resolve_node(node)
        candidate_storages = []
        try:
            storages = pve.nodes(target_node).storage.get()
            for s in storages:
                if "vztmpl" in s.get("content", ""):
                    candidate_storages.append(s.get("storage"))
        except Exception as exc:
            logger.warning("Failed to query storage pools on node '%s': %s", target_node, exc)

        for fallback_storage in ["extra-storage", "backups", "local", "zfs-storage"]:
            if fallback_storage not in candidate_storages:
                candidate_storages.append(fallback_storage)

        first_volid = None
        for s in candidate_storages:
            try:
                r = pve.nodes(target_node).storage(s).content.get(content="vztmpl")
                for item in r:
                    volid = item.get("volid", "")
                    if not volid:
                        continue
                    if "ubuntu" in volid.lower():
                        return volid
                    if not first_volid:
                        first_volid = volid
            except Exception:
                continue

        if first_volid:
            return first_volid

        return "backups:vztmpl/ubuntu-24.04-standard_24.04-2_amd64.tar.zst"
