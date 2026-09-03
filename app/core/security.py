import hmac
import hashlib
from typing import Optional
from fastapi import HTTPException, Security, status, Header
from fastapi.security.api_key import APIKeyHeader
from app.core.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_netbox_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    """
    Validates NetBox webhook HMAC SHA-512 signature using constant-time comparison.
    If no secret is configured, passes through.
    """
    if not settings.NETBOX_WEBHOOK_SECRET:
        return True
    
    if not signature_header:
        return False
    
    secret_bytes = settings.NETBOX_WEBHOOK_SECRET.encode("utf-8")
    expected_mac = hmac.new(secret_bytes, raw_body, hashlib.sha512).hexdigest()
    
    return hmac.compare_digest(expected_mac, signature_header.strip())


async def require_api_key(api_key: Optional[str] = Security(api_key_header)):
    """
    Optional API Key check for manual provisioning routes.
    If settings.API_KEY is unset, all requests are allowed.
    """
    if not settings.API_KEY:
        return True
    
    if not api_key or not hmac.compare_digest(api_key, settings.API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return True
