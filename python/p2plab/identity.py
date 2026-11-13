"""Node identity, compatible with Go key files.

A Go ed25519 private key file holds 64 bytes: the 32-byte seed followed by
the 32-byte public key. Python reads and writes the same format, so a key
file works with either implementation.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def public_bytes(key: Ed25519PrivateKey) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


def node_id(key: Ed25519PrivateKey) -> bytes:
    """NodeID = SHA-256 of the raw 32-byte public key, same as Go."""
    return hashlib.sha256(public_bytes(key)).digest()


def load_or_create(path: str | Path) -> tuple[Ed25519PrivateKey, bytes]:
    path = Path(path)
    if path.exists():
        data = path.read_bytes()
        if len(data) != 64:
            raise ValueError(f"invalid key file: expected 64 bytes, got {len(data)}")
        key = Ed25519PrivateKey.from_private_bytes(data[:32])
    else:
        key = Ed25519PrivateKey.generate()
        seed = key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(seed + public_bytes(key))
        path.chmod(0o600)
    return key, node_id(key)