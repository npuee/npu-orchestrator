from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List


class NetBoxNestedObject(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    slug: Optional[str] = None
    value: Optional[str] = None


class NetBoxIPAddress(BaseModel):
    id: Optional[int] = None
    address: Optional[str] = None  # e.g. 192.168.1.105/24
    display: Optional[str] = None


class NetBoxVirtualMachineData(BaseModel):
    id: Optional[int] = None
    name: str
    status: Optional[Any] = None
    site: Optional[NetBoxNestedObject] = None
    cluster: Optional[NetBoxNestedObject] = None
    platform: Optional[NetBoxNestedObject] = None
    vcpus: Optional[float] = None
    memory: Optional[int] = None  # in MB
    disk: Optional[int] = None    # in GB
    primary_ip: Optional[NetBoxIPAddress] = None
    primary_ip4: Optional[NetBoxIPAddress] = None
    custom_fields: Dict[str, Any] = Field(default_factory=dict)
    tags: List[Any] = Field(default_factory=list)
    comments: Optional[str] = None


class NetBoxWebhookPayload(BaseModel):
    event: str  # "created", "updated", "deleted"
    timestamp: Optional[str] = None
    model: str  # "virtualmachine", "device", "ipaddress"
    username: Optional[str] = None
    request_id: Optional[str] = None
    data: Dict[str, Any]
    snapshots: Optional[Dict[str, Any]] = None
