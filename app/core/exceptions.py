from typing import Optional, Dict, Any


class OrchestratorException(Exception):
    """Base exception for all NPU Infrastructure Orchestrator errors."""

    def __init__(
        self,
        message: str,
        error_code: str = "INTERNAL_ERROR",
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}


# --- Proxmox VE Exceptions ---
class ProxmoxException(OrchestratorException):
    """Base exception for Proxmox VE hypervisor and cluster interactions."""

    def __init__(
        self,
        message: str,
        error_code: str = "PROXMOX_ERROR",
        status_code: int = 502,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, error_code=error_code, status_code=status_code, details=details)


class ProxmoxTaskFailedError(ProxmoxException):
    """Raised when a Proxmox task UPID finishes with a non-OK exit status."""

    def __init__(self, upid: str, exit_status: str, node: str):
        super().__init__(
            message=f"Proxmox task {upid} on node '{node}' failed with exitstatus: {exit_status}",
            error_code="PROXMOX_TASK_FAILED",
            status_code=502,
            details={"upid": upid, "exit_status": exit_status, "node": node},
        )


class ProxmoxTaskTimeoutError(ProxmoxException):
    """Raised when polling a Proxmox task UPID exceeds the configured timeout."""

    def __init__(self, upid: str, timeout_seconds: int, node: str):
        super().__init__(
            message=f"Proxmox task {upid} on node '{node}' timed out after {timeout_seconds}s",
            error_code="PROXMOX_TASK_TIMEOUT",
            status_code=504,
            details={"upid": upid, "timeout_seconds": timeout_seconds, "node": node},
        )


class TemplateNotFoundError(ProxmoxException):
    """Raised when a requested or platform-correlated template cannot be found."""

    def __init__(self, identifier: str, category: str = "linux", node: Optional[str] = None):
        super().__init__(
            message=f"No {category} template matching '{identifier}' found on node '{node or 'any'}'",
            error_code="TEMPLATE_NOT_FOUND",
            status_code=404,
            details={"identifier": identifier, "category": category, "node": node},
        )


class WorkloadNotFoundError(ProxmoxException):
    """Raised when a VM or LXC container is expected to exist but is missing."""

    def __init__(self, vmid: int, node: Optional[str] = None):
        super().__init__(
            message=f"VM/CT {vmid} does not exist on cluster node '{node or 'cluster'}'",
            error_code="WORKLOAD_NOT_FOUND",
            status_code=404,
            details={"vmid": vmid, "node": node},
        )


# --- NetBox Exceptions ---
class NetBoxException(OrchestratorException):
    """Base exception for NetBox DCIM / IPAM interactions."""

    def __init__(
        self,
        message: str,
        error_code: str = "NETBOX_ERROR",
        status_code: int = 502,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, error_code=error_code, status_code=status_code, details=details)


class NetBoxObjectNotFoundError(NetBoxException):
    """Raised when a referenced NetBox object (site, tenant, VM, prefix) does not exist."""

    def __init__(self, object_type: str, identifier: Any):
        super().__init__(
            message=f"NetBox {object_type} matching '{identifier}' was not found",
            error_code="NETBOX_OBJECT_NOT_FOUND",
            status_code=404,
            details={"object_type": object_type, "identifier": str(identifier)},
        )


class IPAllocationError(NetBoxException):
    """Raised when an IP address cannot be allocated from IPAM."""

    def __init__(self, message: str, prefix_id: Optional[int] = None):
        super().__init__(
            message=message,
            error_code="IP_ALLOCATION_FAILED",
            status_code=409,
            details={"prefix_id": prefix_id},
        )


# --- Provisioning & Validation Exceptions ---
class ProvisioningValidationException(OrchestratorException):
    """Raised when provisioning parameters fail schema or boundary validation."""

    def __init__(self, message: str, parameter: Optional[str] = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            status_code=400,
            details={"parameter": parameter} if parameter else {},
        )


class WorkloadAlreadyExistsError(OrchestratorException):
    """Raised when attempting to create a workload with a name or VMID already in use."""

    def __init__(self, identifier: str, existing_vmid: int):
        super().__init__(
            message=f"Workload '{identifier}' already exists with VMID {existing_vmid}",
            error_code="WORKLOAD_ALREADY_EXISTS",
            status_code=409,
            details={"identifier": identifier, "existing_vmid": existing_vmid},
        )
