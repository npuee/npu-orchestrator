import hmac
import hashlib
import logging
from typing import Optional
from fastapi import HTTPException, Security, status, Header
from fastapi.security.api_key import APIKeyHeader
from app.core.config import settings

logger = logging.getLogger("orchestrator.security")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_netbox_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    """
    Validates NetBox webhook HMAC SHA-512 signature using constant-time comparison.
    Fails closed if NETBOX_WEBHOOK_SECRET is unset (unless DEBUG=True in development).
    """
    if not settings.NETBOX_WEBHOOK_SECRET:
        if settings.DEBUG:
            logger.warning("NETBOX_WEBHOOK_SECRET unset; permitting unverified webhook due to DEBUG=True mode.")
            return True
        logger.error("Rejected NetBox webhook: NETBOX_WEBHOOK_SECRET is not configured in .env (failing closed).")
        return False
    
    if not signature_header:
        logger.warning("Rejected NetBox webhook: missing X-Hook-Signature header.")
        return False
    
    secret_bytes = settings.NETBOX_WEBHOOK_SECRET.encode("utf-8")
    expected_mac = hmac.new(secret_bytes, raw_body, hashlib.sha512).hexdigest()
    
    return hmac.compare_digest(expected_mac, signature_header.strip())


async def require_api_key(api_key: Optional[str] = Security(api_key_header)):
    """
    Validates operator API key for protected routes using constant-time comparison.
    Fails closed if API_KEY is unset (unless DEBUG=True in development).
    """
    if not settings.API_KEY:
        if settings.DEBUG:
            logger.warning("API_KEY unset; permitting request due to DEBUG=True mode.")
            return True
        logger.error("Protected route rejected: API_KEY is not configured in .env (failing closed).")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server security misconfiguration: API_KEY is not configured in .env"
        )
    
    if not api_key or not hmac.compare_digest(api_key, settings.API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return True


VALID_SSH_KEY_PREFIXES = (
    "ssh-rsa",
    "ssh-dss",
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "sk-ssh-ed25519@openssh.com",
    "sk-ecdsa-sha2-nistp256@openssh.com",
)


def sanitize_ssh_public_keys(raw_keys: Optional[str]) -> Optional[str]:
    """
    Sanitizes, deduplicates, and validates OpenSSH public keys.
    Filters out empty lines, comments, and non-standard key formats to prevent hypervisor API errors.
    Returns clean, newline-separated OpenSSH public keys or None if empty.
    """
    if not raw_keys or not str(raw_keys).strip():
        return None

    import urllib.parse
    clean_keys = []
    seen = set()

    # Decode in case input was previously URL-encoded
    decoded = urllib.parse.unquote(str(raw_keys))

    for line in decoded.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2 and any(parts[0] == prefix or parts[0].startswith(prefix) for prefix in VALID_SSH_KEY_PREFIXES):
            # Key body is type + base64 data (optionally preserve comment if present)
            normalized = f"{parts[0]} {parts[1]}" + (f" {parts[2]}" if len(parts) >= 3 else "")
            if normalized not in seen:
                seen.add(normalized)
                clean_keys.append(normalized)

    if clean_keys:
        return "\n".join(clean_keys)
    return None
