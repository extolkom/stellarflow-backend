import logging
import threading
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class NonceTracker:
    """Thread-safe per-account nonce tracker for parallel transaction channels.

    Each account address owns an independent Lock, so concurrent transactions
    across different accounts proceed without contention while a single
    account's nonces remain strictly sequential and duplicate-free.

    Complexity
    ----------
    Time  : O(1) amortised per nonce acquisition, sync, or invalidation.
    Space : O(n) where n is the number of unique account addresses tracked.
    """

    _instance: Optional["NonceTracker"] = None
    _init_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> "NonceTracker":
        # Double-checked locking: fast path avoids acquiring _init_lock once
        # the singleton is fully constructed.
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._account_locks: Dict[str, threading.Lock] = {}
                    instance._nonces: Dict[str, int] = {}
                    # Protects _account_locks dict during lazy lock creation.
                    instance._map_lock = threading.Lock()
                    cls._instance = instance
        return cls._instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_lock(self, address: str) -> threading.Lock:
        """Return the per-account lock, creating it lazily on first access.

        Double-checked locking ensures _map_lock is acquired only on the
        initial creation, keeping the common path (lock already exists)
        entirely contention-free.
        """
        lock = self._account_locks.get(address)
        if lock is None:
            with self._map_lock:
                lock = self._account_locks.get(address)
                if lock is None:
                    lock = threading.Lock()
                    self._account_locks[address] = lock
        return lock

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_next_nonce(self, address: str, seed: Optional[int] = None) -> int:
        """Return the next unique, monotonically-increasing nonce for *address*.

        On the first call for an account a *seed* (the current on-chain
        sequence number) must be supplied. Subsequent calls increment the
        cached value atomically without further network I/O.

        Args:
            address: Account identifier (e.g. a Stellar public key).
            seed:    Bootstrap nonce when no local cache exists. Required on
                     the first call; ignored once a value is cached.

        Returns:
            An integer nonce guaranteed to be unique and sequential for
            *address* across all concurrent callers.

        Raises:
            ValueError: If no cached nonce exists and no *seed* was supplied.
        """
        lock = self._get_lock(address)
        with lock:
            try:
                cached = self._nonces.get(address)
                if cached is None:
                    if seed is None:
                        raise ValueError(
                            f"No cached nonce for '{address}' and no seed supplied."
                        )
                    self._nonces[address] = seed
                    logger.info("[NonceTracker] Seeded nonce for %s → %d", address, seed)
                    return seed

                next_nonce = cached + 1
                self._nonces[address] = next_nonce
                return next_nonce
            except Exception:
                # Drop the cache on any error so the next caller is forced to
                # re-sync from the ledger instead of propagating a stale value.
                self._nonces.pop(address, None)
                raise

    def sync_nonce(self, address: str, nonce: int) -> None:
        """Overwrite the cached nonce with a known-good ledger value.

        Call this after a tx_bad_seq error to realign the local counter with
        the chain's authoritative sequence number.

        Time: O(1).
        """
        lock = self._get_lock(address)
        with lock:
            self._nonces[address] = nonce
            logger.info("[NonceTracker] Synced nonce for %s → %d", address, nonce)

    def invalidate(self, address: Optional[str] = None) -> None:
        """Evict the cached nonce for *address*, or all accounts when omitted.

        The next call to get_next_nonce will require a seed or an external
        sync from the ledger.

        Implementation note: for a full clear, a snapshot of existing accounts
        is taken under _map_lock which is then released before acquiring
        individual per-account locks. This prevents a deadlock that would arise
        if _map_lock were held while waiting for per-account locks that other
        threads may already hold.

        Time: O(1) for a single address; O(n) for a full clear.
        """
        if address is not None:
            lock = self._get_lock(address)
            with lock:
                self._nonces.pop(address, None)
            logger.info(
                "[NonceTracker] Nonce invalidated for %s. Re-sync required.", address
            )
            return

        # Snapshot account locks without holding _map_lock during the clear.
        with self._map_lock:
            snapshot = list(self._account_locks.items())

        for addr, lock in snapshot:
            with lock:
                self._nonces.pop(addr, None)

        logger.info("[NonceTracker] All cached nonces cleared. Re-sync required.")


# Module-level singleton – import and use directly.
nonce_tracker = NonceTracker()
