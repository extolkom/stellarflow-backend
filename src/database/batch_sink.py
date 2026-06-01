import threading
import logging
import sqlite3
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class BatchSink:
    """Thread‑safe micro‑batch aggregator for telemetry data.

    Usage:
        # Obtain a sqlite3.Connection (ensure ``isolation_level=None`` for explicit transactions)
        conn = sqlite3.connect('your.db', isolation_level=None)
        sink = BatchSink(conn, table_name='telemetry', flush_interval=2.0)
        sink.save({"asset_id": "abc", "price": 123.45, "ts": 1700000000})
        ...
        sink.shutdown()  # flush remaining data and stop background thread
    """

    def __init__(self, connection: sqlite3.Connection, table_name: str = "telemetry", flush_interval: float = 2.0):
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("connection must be an instance of sqlite3.Connection")
        self._conn = connection
        self._table = table_name
        self._interval = flush_interval
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="BatchSink-Flusher")
        self._thread.start()
        logger.debug("BatchSink initialized for table %s with %s‑second interval", self._table, self._interval)

    def save(self, data: Dict[str, Any]) -> None:
        """Add a telemetry record to the in‑memory buffer.

        The method is safe to call from multiple threads.
        """
        if not isinstance(data, dict):
            raise TypeError("data must be a dict mapping column names to values")
        with self._lock:
            self._buffer.append(data)
        logger.debug("Saved record to buffer; current size=%d", len(self._buffer))

    def _run(self) -> None:
        """Background worker that periodically flushes the buffer.

        It wakes up every ``self._interval`` seconds and attempts to write any
        accumulated records to the database.
        """
        while not self._stop_event.wait(self._interval):
            try:
                self._flush()
            except Exception as exc:  # pragma: no cover – defensive
                logger.exception("Unexpected error while flushing BatchSink: %s", exc)

    def _flush(self) -> None:
        """Perform a bulk ``executemany`` insert of buffered rows.

        The operation is atomic: the buffer is cleared *after* a successful copy
        of its contents, guaranteeing that no record is lost on failure.
        """
        # Snapshot and clear the buffer under lock
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer.copy()
            self._buffer.clear()
        logger.debug("Flushing %d records to table %s", len(batch), self._table)

        # Determine column ordering from the first record – all records must share the same schema
        columns = list(batch[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_clause = ", ".join(columns)
        sql = f"INSERT INTO {self._table} ({column_clause}) VALUES ({placeholders})"
        values = [tuple(row[col] for col in columns) for row in batch]

        # Use an explicit transaction for speed and atomicity
        try:
            self._conn.execute("BEGIN")
            self._conn.executemany(sql, values)
            self._conn.execute("COMMIT")
            logger.debug("Successfully flushed %d records", len(batch))
        except Exception:
            # Roll back to keep DB consistent and re‑queue the data
            self._conn.execute("ROLLBACK")
            with self._lock:
                # prepend failed batch so they are not lost
                self._buffer = batch + self._buffer
            logger.exception("Failed to flush BatchSink; records re‑queued")
            raise

    def shutdown(self) -> None:
        """Stop the background thread and flush any remaining data.
        """
        self._stop_event.set()
        self._thread.join()
        # Final flush – ignore errors to avoid blocking shutdown
        try:
            self._flush()
        except Exception as exc:  # pragma: no cover – defensive
            logger.exception("Error during final BatchSink shutdown flush: %s", exc)
        logger.info("BatchSink shutdown complete; %d records remaining in buffer", len(self._buffer))
