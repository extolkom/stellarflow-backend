"""
src/crypto/signer.py
~~~~~~~~~~~~~~~~~~~~
Context-managed signing primitive that enforces strict key-lifetime isolation.
"""

from __future__ import annotations

import ctypes
import logging
import os
from types import TracebackType
from typing import Optional, Type

logger = logging.getLogger(__name__)

__all__ = ["SecureKeyHandle", "SigningError"]


def _zero_wipe(buf: bytearray) -> None:
    """Overwrite *buf* in-place with zeros."""
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        ctypes.memset(addr, 0, len(buf))
    finally:
        for i in range(len(buf)):
            buf[i] = 0


def _lock_memory(buf: bytearray) -> None:
    """
    Best-effort memory lock.

    Prevents pages containing private-key material from being swapped to disk.
    Uses mlock on Unix-like systems and VirtualLock on Windows.
    """
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        length = ctypes.c_size_t(len(buf))

        if os.name == "nt":
            kernel32 = ctypes.windll.kernel32
            kernel32.VirtualLock(ctypes.c_void_p(addr), length)
        else:
            libc = ctypes.CDLL(None)
            if hasattr(libc, "mlock"):
                libc.mlock(ctypes.c_void_p(addr), length)
    except Exception:  # noqa: BLE001
        # Memory locking may fail because of OS limits or permissions.
        # This hardening is best-effort and must not break signing.
        pass


def _unlock_memory(buf: bytearray) -> None:
    """Best-effort unlock for previously locked key memory."""
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        length = ctypes.c_size_t(len(buf))

        if os.name == "nt":
            kernel32 = ctypes.windll.kernel32
            kernel32.VirtualUnlock(ctypes.c_void_p(addr), length)
        else:
            libc = ctypes.CDLL(None)
            if hasattr(libc, "munlock"):
                libc.munlock(ctypes.c_void_p(addr), length)
    except Exception:  # noqa: BLE001
        pass


def _wipe_bytes_view(view: bytes) -> None:
    """Best-effort wipe of a temporary bytes copy."""
    if not view:
        return

    try:
        buf = (ctypes.c_char * len(view)).from_buffer_copy(view)
        ctypes.memset(ctypes.addressof(buf), 0, len(view))
    except Exception:  # noqa: BLE001
        pass


class SigningError(Exception):
    """Raised when signing fails or the key handle is no longer usable."""


class SecureKeyHandle:
    """
    Context manager that keeps private-key material isolated.

    The raw key is copied into a mutable bytearray, memory-locked on a
    best-effort basis, and wiped when the signing scope closes.
    """

    __slots__ = ("_buf", "_active", "_wiped")

    def __init__(self, raw_key: bytes) -> None:
        if not raw_key:
            raise ValueError("raw_key must be non-empty bytes.")

        self._buf: bytearray = bytearray(raw_key)
        _lock_memory(self._buf)

        self._active: bool = False
        self._wiped: bool = False

    def __enter__(self) -> "SecureKeyHandle":
        self._active = True
        logger.debug("[SecureKeyHandle] Signing scope opened.")
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self._active = False
        self._do_wipe()
        return False

    def __del__(self) -> None:
        try:
            self._do_wipe()
        except Exception:  # noqa: BLE001
            pass

    def _do_wipe(self) -> None:
        """Idempotently wipe and unlock the internal key buffer."""
        if self._wiped:
            return

        self._wiped = True

        try:
            _zero_wipe(self._buf)
        finally:
            _unlock_memory(self._buf)

        logger.debug("[SecureKeyHandle] Signing scope closed — key wiped.")

    def sign(self, tx_hash: bytes) -> bytes:
        """Sign a 32-byte transaction hash."""
        if not self._active:
            raise SigningError(
                "SecureKeyHandle.sign() called outside an active signing scope. "
                "Use 'with SecureKeyHandle(...) as handle:' and call sign() inside."
            )

        if self._wiped:
            raise SigningError(
                "SecureKeyHandle.sign() called after the handle has been wiped."
            )

        if len(tx_hash) != 32:
            raise ValueError(f"tx_hash must be exactly 32 bytes, got {len(tx_hash)}.")

        return self._sign_internal(tx_hash)

    def _sign_internal(self, tx_hash: bytes) -> bytes:
        key_bytes: bytes = bytes(self._buf)

        try:
            try:
                return self._try_stellar_sdk(key_bytes, tx_hash)
            except ImportError:
                return self._try_pynacl(key_bytes, tx_hash)
        finally:
            _wipe_bytes_view(key_bytes)
            del key_bytes

    @staticmethod
    def _try_stellar_sdk(key_bytes: bytes, tx_hash: bytes) -> bytes:
        from stellar_sdk import Keypair  # type: ignore[import]  # noqa: PLC0415

        try:
            keypair = Keypair.from_raw_ed25519_seed(key_bytes)
            return bytes(keypair.sign(tx_hash))
        except Exception as exc:
            raise SigningError("Signing failed (stellar_sdk path).") from exc

    @staticmethod
    def _try_pynacl(key_bytes: bytes, tx_hash: bytes) -> bytes:
        try:
            from nacl.signing import SigningKey  # type: ignore[import]  # noqa: PLC0415
        except ImportError:
            raise SigningError(
                "Neither 'stellar_sdk' nor 'PyNaCl' is installed. "
                "Install one to enable signing."
            )

        try:
            sk = SigningKey(key_bytes)
            return bytes(sk.sign(tx_hash).signature)
        except Exception as exc:
            raise SigningError("Signing failed (PyNaCl path).") from exc