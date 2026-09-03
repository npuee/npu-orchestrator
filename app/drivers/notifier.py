import logging
import httpx
from typing import Optional, List, Dict, Any
from app.core.config import settings

logger = logging.getLogger("orchestrator.notifier")


class NotificationDriver:
    async def send_signal_message(
        self,
        message: str,
        recipients: Optional[List[str]] = None,
    ) -> bool:
        """
        Dispatches a Signal message through the configured backend (e.g., api.example.com/signal or signal-cli REST).
        """
        if not settings.SIGNAL_ENABLED or not settings.SIGNAL_API_URL:
            logger.debug("Signal notifications disabled or no URL configured")
            return False

        target_recipients = recipients or settings.SIGNAL_RECIPIENTS
        
        payload: Dict[str, Any] = {
            "message": message,
        }
        if settings.SIGNAL_SENDER:
            payload["number"] = settings.SIGNAL_SENDER
        if target_recipients:
            payload["recipients"] = target_recipients

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(settings.SIGNAL_API_URL, json=payload)
                if resp.status_code in (200, 201, 202):
                    logger.info("Signal notification sent successfully")
                    return True
                logger.warning("Signal API returned HTTP %d: %s", resp.status_code, resp.text)
                return False
        except Exception as e:
            logger.error("Failed to send Signal notification: %s", e)
            return False

    async def notify_job_success(self, job_id: str, action: str, details: Dict[str, Any]):
        vmid = details.get("vmid", "N/A")
        hostname = details.get("hostname", "N/A")
        ip = details.get("ip_address", "N/A")
        category = details.get("category", "VM").upper()
        
        if action == "decommission_vm":
            message = f"""🗑️ [Orchestrator] VM Permanently Purged
━━━━━━━━━━━━━━━━━━━━
VM ID:    {vmid}
Hostname: {hostname}
Node:     {details.get('node', 'proxmox')}
Status:   Disks & Storage Purged
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        elif action == "quarantine_vm":
            message = f"""🛑 [Orchestrator] VM Quarantined (Decommissioned)
━━━━━━━━━━━━━━━━━━━━
VM ID:    {vmid}
Hostname: {hostname}
Node:     {details.get('node', 'proxmox')}
Status:   Stopped & Network Isolated (Disks Preserved)
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        elif action == "power_start":
            message = f"""⚡ [Orchestrator] VM Started (Power On)
━━━━━━━━━━━━━━━━━━━━
VM ID:    {vmid}
Hostname: {hostname}
Node:     {details.get('node', 'proxmox')}
Status:   Running
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        elif action == "power_stop":
            message = f"""💤 [Orchestrator] VM Stopped (Power Off)
━━━━━━━━━━━━━━━━━━━━
VM ID:    {vmid}
Hostname: {hostname}
Node:     {details.get('node', 'proxmox')}
Status:   Stopped (Disks Safe)
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        elif action == "vm_rename":
            old_name = details.get("old_name", "unknown")
            message = f"""🏷️ [Orchestrator] VM Renamed & DNS Updated
━━━━━━━━━━━━━━━━━━━━
VM ID:    {vmid}
Old Name: {old_name}
New Name: {hostname}
Node:     {details.get('node', 'proxmox')}
Status:   Proxmox name & NetBox DNS synced
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        else:
            message = f"""🚀 [Orchestrator] Provisioning Completed
━━━━━━━━━━━━━━━━━━━━
Action:   {action} ({category})
VM ID:    {vmid}
Hostname: {hostname}
IP:       {ip}
Status:   Running
Job ID:   {job_id}
━━━━━━━━━━━━━━━━━━━━"""
        await self.send_signal_message(message)

    async def notify_job_failure(self, job_id: str, action: str, error: str, metadata: Optional[Dict[str, Any]] = None):
        meta_str = ""
        if metadata:
            meta_str = f"\nTarget:   {metadata.get('hostname', 'unknown')}"

        message = f"""⚠️ [Orchestrator] Task Failed!
━━━━━━━━━━━━━━━━━━━━
Action:   {action}{meta_str}
Job ID:   {job_id}
Error:    {error}
━━━━━━━━━━━━━━━━━━━━"""
        await self.send_signal_message(message)


notifier = NotificationDriver()
