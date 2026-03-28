"""
Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
PROPRIETARY AND CONFIDENTIAL - See LICENSE file for terms.

Encrypt sensitive fields in Spiral action configs before DB storage.
"""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Sensitive field names that should be encrypted in node configs
SENSITIVE_FIELDS = frozenset(
    {
        "api_key",
        "api_token",
        "api_secret",
        "access_token",
        "refresh_token",
        "bearer_token",
        "secret_key",
        "secret",
        "signature_key",
        "password",
        "auth_token",
        "account_sid",
        "webhook_secret",
        "client_secret",
        "private_key",
        "connection_string",
    }
)

# Prefix to identify already-encrypted values
_ENC_PREFIX = "enc:fernet:"

try:
    import base64
    import hashlib

    from cryptography.fernet import Fernet

    def _get_fernet() -> Fernet:
        key_source = os.getenv("CREDENTIAL_ENCRYPTION_KEY") or os.getenv("SECRET_KEY")
        if not key_source:
            raise RuntimeError(
                "CREDENTIAL_ENCRYPTION_KEY or SECRET_KEY env var is required for credential encryption"
            )
        # Use PBKDF2 with a fixed application salt for proper key derivation
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            key_source.encode(),
            b"helix-spirals-credential-encryption-v1",
            iterations=600_000,
        )
        return Fernet(base64.urlsafe_b64encode(derived))

    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False
    logger.warning("cryptography not installed — credential encryption disabled")


def encrypt_value(plaintext: str) -> str:
    """Encrypt a single string value. Returns prefixed ciphertext."""
    if not _CRYPTO_AVAILABLE or not plaintext or plaintext.startswith(_ENC_PREFIX):
        return plaintext
    try:
        f = _get_fernet()
        token = f.encrypt(plaintext.encode()).decode()
        return f"{_ENC_PREFIX}{token}"
    except Exception as e:
        logger.warning("Failed to encrypt value: %s", e)
        return plaintext


def decrypt_value(ciphertext: str) -> str:
    """Decrypt a single prefixed ciphertext. Returns plaintext."""
    if not _CRYPTO_AVAILABLE or not ciphertext or not ciphertext.startswith(_ENC_PREFIX):
        return ciphertext
    try:
        f = _get_fernet()
        token = ciphertext[len(_ENC_PREFIX) :]
        return f.decrypt(token.encode()).decode()
    except Exception as e:
        logger.warning("Failed to decrypt value (key rotation?): %s", e)
        return ""


def encrypt_config(config: dict[str, Any]) -> dict[str, Any]:
    """Deep-walk a config dict and encrypt all sensitive fields."""
    if not _CRYPTO_AVAILABLE:
        return config
    return _walk(config, encrypt_value)


def decrypt_config(config: dict[str, Any]) -> dict[str, Any]:
    """Deep-walk a config dict and decrypt all sensitive fields."""
    if not _CRYPTO_AVAILABLE:
        return config
    return _walk(config, decrypt_value)


def _walk(obj: Any, transform_fn) -> Any:
    """Recursively walk a dict/list and transform sensitive string values."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k in SENSITIVE_FIELDS and isinstance(v, str):
                result[k] = transform_fn(v)
            else:
                result[k] = _walk(v, transform_fn)
        return result
    elif isinstance(obj, list):
        return [_walk(item, transform_fn) for item in obj]
    return obj
