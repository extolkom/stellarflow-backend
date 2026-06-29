import asyncio
import logging
import os
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set, Any

import aiohttp
import requests

logger = logging.getLogger("Network.RPCSup")

# Threshold Parameters
LIGHTWEIGHT_PING_TIMEOUT = 0.8  # Max acceptable time window (800ms) before degradation warning
MOVING_AVG_WINDOW_SIZE = 4      # Number of historic latency checks to weigh mathematically

class HorizonNodeProfile:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.latency_history: List[float] = []
        self.is_healthy = True

    @property
    def moving_average_latency(self) -> float:
        """Calculates historical moving average execution latency parameters."""
        if not self.latency_history:
            return 0.0
        return sum(self.latency_history) / len(self.latency_history)

    def record_metric(self, latency_ms: float):
        """Appends latency sample to bounded historic window tracking loops."""
        self.latency_history.append(latency_ms)
        if len(self.latency_history) > MOVING_AVG_WINDOW_SIZE:
            self.latency_history.pop(0)


class PredictiveRPCSupervisor:
    def __init__(self, primary_endpoints: List[Dict[str, str]], fallback_endpoints: List[Dict[str, str]]):
        """
        Orchestrates network health scoring topologies across core and backup infrastructure arrays.
        Input format example: [{"name": "horizon-main", "url": "https://horizon.stellar.org"}]
        """
        self.primary_pool = [HorizonNodeProfile(node["name"], node["url"]) for node in primary_endpoints]
        self.fallback_pool = [HorizonNodeProfile(node["name"], node["url"]) for node in fallback_endpoints]
        self.active_node: HorizonNodeProfile = self.primary_pool[0]

    async def run_predictive_ping_cycle(self) -> None:
        """
        Executes parallel, lightweight validation pings across the cluster.
        Updates health statuses without introducing blocking execution lags to outer worker frameworks.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            all_nodes = self.primary_pool + self.fallback_pool
            
            for node in all_nodes:
                tasks.append(self._probe_node_health(session, node))
            
            await asyncio.gather(*tasks)
        
        self._evaluate_routing_topology()

    async def _probe_node_health(self, session: aiohttp.ClientSession, node: HorizonNodeProfile) -> None:
        """
        Dispatches lightweight low-overhead endpoint probes to track real-time communication shifts.
        """
        # Horizon base path used for lightweight connection checks
        probe_url = f"{node.url.rstrip('/')}/"
        start_time = time.monotonic()
        
        try:
            async with asyncio.timeout(LIGHTWEIGHT_PING_TIMEOUT):
                async with session.get(probe_url) as response:
                    if response.status == 200:
                        latency_ms = (time.monotonic() - start_time) * 1000
                        node.record_metric(latency_ms)
                        
                        # Mark degraded if moving average indicates systematic latency decline
                        if node.moving_average_latency > (LIGHTWEIGHT_PING_TIMEOUT * 1000):
                            if node.is_healthy:
                                logger.warning(f"Predictive Warning: Performance degradation detected on {node.name}. Latency: {node.moving_average_latency:.1f}ms")
                            node.is_healthy = False
                        else:
                            node.is_healthy = True
                        return

                    node.is_healthy = False
                    logger.debug(f"Node {node.name} returned non-200 footprint status: {response.status}")
                    
        except (asyncio.TimeoutError, aiohttp.ClientError):
            node.is_healthy = False
            node.record_metric(LIGHTWEIGHT_PING_TIMEOUT * 1000 * 2) # Penalize metric tracking log
            logger.warn(f"Predictive Supervisor flagged node [{node.name}] as UNHEALTHY (Timeout/Network breakdown)")

    def _evaluate_routing_topology(self) -> None:
        """
        Dynamically shifts layout traffic pointers to healthier candidate environments.
        """
        # If active node is healthy and performing nominal processing, preserve active route
        if self.active_node.is_healthy:
            return

        logger.warn(f"Active Horizon Endpoint [{self.active_node.name}] degraded. Initializing preemptive failover routine...")
        
        # 1. Scan primary pool for an alternate healthy node
        for primary in self.primary_pool:
            if primary.is_healthy:
                self.active_node = primary
                logger.info(f"Traffic routing safely shifted to alternate primary node: [{self.active_node.name}]")
                return

        # 2. Fallback to secondary isolated backup arrays if full primary tier crashes
        for fallback in self.fallback_pool:
            if fallback.is_healthy:
                self.active_node = fallback
                logger.critical(f"EMERGENCY: Primary Horizon node array completely degraded! Failover routed to backup: [{self.active_node.name}]")
                return

        logger.error("CRITICAL FAILURE: Comprehensive Horizon node matrix completely unreachable. No healthy nodes found.")


class NonceWindow:
    """Sliding window nonce tracker for Stellar transaction sequencing.

    Each account maintains a window of in-flight nonces. The base sequence
    advances as nonces are acknowledged, freeing slots for new acquisitions.

    Thread-safe: all public methods are protected by a per-window lock.

    Parameters
    ----------
    window_size:
        Maximum number of concurrent in-flight nonces per account.
    """

    DEFAULT_WINDOW_SIZE: int = 64

    def __init__(self, window_size: int = DEFAULT_WINDOW_SIZE) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self._window_size = window_size
        self._lock = threading.Lock()
        self._base: Dict[str, int] = {}
        self._issued: Dict[str, Set[int]] = defaultdict(set)
        self._max_issued: Dict[str, int] = {}

    @property
    def window_size(self) -> int:
        return self._window_size

    def acquire(self, account: str, seed: Optional[int] = None) -> int:
        """Acquire the next available nonce for *account*.

        Parameters
        ----------
        account:
            Stellar public key address.
        seed:
            If provided, seeds the window base to this value on first use.

        Returns
        -------
        int
            The nonce to use for the next transaction.

        Raises
        ------
        ValueError
            If the window has not been seeded and *seed* is not provided.
        RuntimeError
            If all window slots are in flight.
        """
        with self._lock:
            if account not in self._base:
                if seed is None:
                    raise ValueError(f"NonceWindow for {account!r} is unseeded — no seed supplied")
                self._base[account] = seed
                self._max_issued[account] = seed - 1

            in_flight = len(self._issued[account])
            if in_flight >= self._window_size:
                raise RuntimeError(f"Nonce window for {account!r} is exhausted")

            base = self._base[account]
            nonce = base + in_flight
            self._issued[account].add(nonce)
            self._max_issued[account] = max(self._max_issued[account], nonce)
            return nonce

    def acknowledge(self, account: str, nonce: int) -> None:
        """Acknowledge completion of a nonce, potentially sliding the window base.

        The base slides forward past any previously-issued nonces that are no
        longer in flight.

        Parameters
        ----------
        account:
            Stellar public key address.
        nonce:
            The nonce that completed.
        """
        with self._lock:
            if account not in self._base:
                return

            self._issued[account].discard(nonce)

            base = self._base[account]
            max_issued = self._max_issued[account]
            while base <= max_issued and base not in self._issued[account]:
                base += 1
            self._base[account] = base

    def available_slots(self, account: str) -> int:
        """Return the number of available nonce slots for *account*.

        Returns 0 if the window has not been seeded.
        """
        with self._lock:
            if account not in self._base:
                return 0
            base = self._base[account]
            max_issued = self._max_issued[account]
            span = max_issued - base + 1
            return max(0, self._window_size - span)

    def sync(self, account: str, base: int) -> None:
        """Reset the window to a specific base value.

        Unlike :meth:`sync_nonce`, this sets the base directly without
        adjustment. Used for testing and manual reset scenarios.

        Parameters
        ----------
        account:
            Stellar public key address.
        base:
            The base sequence to use.
        """
        with self._lock:
            self._base[account] = base
            self._issued[account] = set()
            self._max_issued[account] = base - 1

    def invalidate(self, account: Optional[str] = None) -> None:
        """Invalidate nonce state.

        If *account* is provided, only that account's window is cleared.
        If *account* is ``None``, all accounts are cleared.

        Parameters
        ----------
        account:
            Optional specific account to invalidate.
        """
        with self._lock:
            if account is None:
                self._base.clear()
                self._issued.clear()
                self._max_issued.clear()
            else:
                self._base.pop(account, None)
                self._issued.pop(account, None)
                self._max_issued.pop(account, None)


class NonceTracker:
    """Convenience wrapper around :class:`NonceWindow` with a default singleton."""

    def __init__(self, window_size: int = NonceWindow.DEFAULT_WINDOW_SIZE) -> None:
        self._window = NonceWindow(window_size=window_size)

    def acquire(self, account: str, seed: Optional[int] = None) -> int:
        return self._window.acquire(account, seed=seed)

    def get_next_nonce(self, account: str, seed: Optional[int] = None) -> int:
        """Alias for :meth:`Acquire` to match the TxManager interface."""
        return self._window.acquire(account, seed=seed)

    def acknowledge(self, account: str, nonce: int) -> None:
        self._window.acknowledge(account, nonce)

    def available_slots(self, account: str) -> int:
        return self._window.available_slots(account)

    def sync(self, account: str, base: int) -> None:
        self._window.sync(account, base)

    def sync_nonce(self, account: str, base: int) -> None:
        """Sync with ledger-confirmed nonce.

        The *base* is the last confirmed nonce from the ledger. The next
        nonce issued will be ``base + 1``.
        """
        self._window.sync(account, base + 1)

    def invalidate(self, account: Optional[str] = None) -> None:
        self._window.invalidate(account)


nonce_tracker = NonceTracker()
nonce_window = NonceWindow()


class RPCNodeFailoverSupervisor:
    """Proactive RPC node failover supervisor that monitors node connectivity.

    It maintains a list of endpoints and runs a background thread to check their
    latency and health using lightweight JSON-RPC requests. If the active node
    experiences a latency drop or fails, the supervisor instantly shifts the
    active traffic to the fastest available secondary node.

    Complexity:
    Time: O(1) for active endpoint lookup, O(N) for checking N endpoints.
    Space: O(N) to store latency stats for N endpoints.
    """

    def __init__(
        self,
        endpoints: Optional[List[str]] = None,
        check_interval_sec: float = 2.0,
        latency_threshold_ms: float = 500.0,
        ping_timeout_sec: float = 1.0,
    ) -> None:
        self.check_interval_sec = check_interval_sec
        self.latency_threshold_ms = latency_threshold_ms
        self.ping_timeout_sec = ping_timeout_sec

        if endpoints is None:
            primary = os.environ.get("RPC_URL")
            fallbacks = os.environ.get("FALLBACK_RPC_URLS")
            loaded = []
            if primary:
                loaded.append(primary.strip())
            if fallbacks:
                for f in fallbacks.split(","):
                    if f.strip():
                        loaded.append(f.strip())
            if not loaded:
                loaded = [
                    "https://rpc.testnet.stellar.org",
                    "https://rpc.mainnet.stellar.org",
                ]
            self.endpoints = loaded
        else:
            self.endpoints = list(endpoints)

        self._lock = threading.Lock()
        self._active_endpoint = self.endpoints[0] if self.endpoints else ""
        self._latencies: Dict[str, float] = {ep: 0.0 for ep in self.endpoints}
        self._healthy_endpoints: set = set(self.endpoints)

        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background monitoring thread."""
        with self._lock:
            if self._monitor_thread is not None and self._monitor_thread.is_alive():
                return
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(
                target=self._run_monitor,
                name="RPCNodeFailoverSupervisor-Monitor",
                daemon=True,
            )
            self._monitor_thread.start()
            logger.info("[RPCNodeFailoverSupervisor] Started proactive background monitoring.")

    def stop(self) -> None:
        """Stop the background monitoring thread."""
        self._stop_event.set()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=1.0)
            self._monitor_thread = None
            logger.info("[RPCNodeFailoverSupervisor] Stopped background monitoring.")

    def get_active_endpoint(self) -> str:
        """Return the currently selected active RPC endpoint."""
        with self._lock:
            return self._active_endpoint

    def _ping_node(self, endpoint: str) -> Optional[float]:
        """Perform a fast, lightweight check on a single node and return its latency in ms."""
        try:
            start = time.time()
            response = requests.post(
                endpoint,
                json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
                timeout=self.ping_timeout_sec,
            )
            latency_ms = (time.time() - start) * 1000.0
            if response.status_code == 200:
                data = response.json()
                if "result" in data or "error" in data:
                    return latency_ms
            return None
        except Exception:
            return None

    def _run_monitor(self) -> None:
        """Main loop for the background monitoring thread."""
        while not self._stop_event.is_set():
            temp_latencies = {}
            temp_healthy = set()

            for ep in self.endpoints:
                latency = self._ping_node(ep)
                if latency is not None:
                    temp_latencies[ep] = latency
                    temp_healthy.add(ep)
                else:
                    temp_latencies[ep] = float("inf")

            with self._lock:
                self._latencies.update(temp_latencies)
                self._healthy_endpoints = temp_healthy

                active_ok = False
                active_latency = self._latencies.get(self._active_endpoint, float("inf"))

                if (
                    self._active_endpoint in self._healthy_endpoints
                    and active_latency <= self.latency_threshold_ms
                ):
                    active_ok = True

                if not active_ok:
                    best_endpoint = self._active_endpoint
                    best_latency = active_latency

                    for ep in self.endpoints:
                        ep_latency = self._latencies.get(ep, float("inf"))
                        if ep in self._healthy_endpoints and ep_latency < best_latency:
                            best_endpoint = ep
                            best_latency = ep_latency

                    if best_endpoint != self._active_endpoint:
                        logger.warning(
                            "[RPCNodeFailoverSupervisor] Shifted traffic from %s (latency: %.1fms) to %s (latency: %.1fms)",
                            self._active_endpoint,
                            active_latency,
                            best_endpoint,
                            best_latency,
                        )
                        self._active_endpoint = best_endpoint

            self._stop_event.wait(self.check_interval_sec)


rpc_supervisor = RPCNodeFailoverSupervisor()


__all__ = [
    "NonceTracker",
    "NonceWindow",
    "nonce_tracker",
    "nonce_window",
    "RPCNodeFailoverSupervisor",
    "rpc_supervisor",
]
