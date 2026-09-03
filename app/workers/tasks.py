"""
Re-export façade for backward compatibility.
Workers logic is partitioned into domain-specific modules:
  - app.workers.provisioning (Linux, Windows, LXC creation)
  - app.workers.lifecycle    (Power, VM sync, Decommissioning)
  - app.workers.dispatcher   (NetBox webhook ingestion & event diffing)
"""

from app.workers.provisioning import (
    run_linux_provision_task,
    run_windows_provision_task,
    run_lxc_provision_task,
)
from app.workers.lifecycle import (
    run_power_sync_task,
    run_decommission_task,
    run_vm_sync_task,
    _active_decommissioning_vms,
    _active_power_sync_vms,
)
from app.workers.dispatcher import (
    process_netbox_webhook_event,
    _active_provisioning_vms,
)

__all__ = [
    "run_linux_provision_task",
    "run_windows_provision_task",
    "run_lxc_provision_task",
    "run_power_sync_task",
    "run_decommission_task",
    "run_vm_sync_task",
    "process_netbox_webhook_event",
    "_active_provisioning_vms",
    "_active_decommissioning_vms",
    "_active_power_sync_vms",
]
