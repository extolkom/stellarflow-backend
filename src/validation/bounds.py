import threading
import collections
from statistics import median
from typing import Deque, Dict, Any, List, Optional

# --- Configuration constants -------------------------------------------------
DEFAULT_WINDOW_SIZE: int = 100  # Number of recent prices to keep for median calculation
DEFAULT_VARIANCE_THRESHOLD: float = 0.15  # 15% price variance allowed

# --- Internal state -----------------------------------------------------------
# A thread‑safe container that holds recent price samples. Using a deque for O(1)
# appends and pops while keeping a fixed maximum length.
_price_buffer: Deque[float] = collections.deque(maxlen=DEFAULT_WINDOW_SIZE)
_buffer_lock = threading.Lock()


def _current_median() -> Optional[float]:
    """Return the median of the buffered prices.

    The function assumes the caller holds ``_buffer_lock``. ``None`` is returned
    when the buffer is empty.
    """
    if not _price_buffer:
        return None
    # ``median`` works on any iterable of numbers.
    return median(_price_buffer)


def is_price_acceptable(price: float,
                        window: int = DEFAULT_WINDOW_SIZE,
                        variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD) -> bool:
    """Determine whether *price* is within the allowed variance of the rolling median.

    The function updates the internal rolling buffer **after** the check so the
    current price does not influence its own validation.

    Parameters
    ----------
    price: float
        The incoming price value to validate.
    window: int, optional
        Number of recent samples to keep for median calculation. If the internal
        buffer size differs, it will be resized accordingly.
    variance_threshold: float, optional
        Maximum relative deviation (e.g. ``0.15`` for 15%).

    Returns
    -------
    bool
        ``True`` if the price is within the threshold, ``False`` otherwise.
    """
    if price is None:
        return False

    with _buffer_lock:
        # Adjust buffer size if a custom window is requested.
        if _price_buffer.maxlen != window:
            # Preserve the most recent values up to the new limit.
            old = list(_price_buffer)
            _price_buffer.clear()
            _price_buffer.extend(old[-window:])
            _price_buffer = collections.deque(_price_buffer, maxlen=window)  # type: ignore

        median_value = _current_median()
        # When we have no historic data, accept the price and seed the buffer.
        if median_value is None:
            _price_buffer.append(price)
            return True

        # Compute relative deviation.
        deviation = abs(price - median_value) / median_value
        if deviation > variance_threshold:
            # Price is an outlier – do **not** add it to the buffer.
            return False

        # Acceptable – store it for future calculations.
        _price_buffer.append(price)
        return True


def filter_record(record: Dict[str, Any],
                  price_key: str = "price",
                  window: int = DEFAULT_WINDOW_SIZE,
                  variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD) -> Optional[Dict[str, Any]]:
    """Return the *record* if its price passes the median‑variance check, otherwise ``None``.

    The function is tolerant to missing or non‑numeric price fields – such
    records are treated as invalid and dropped.
    """
    price = record.get(price_key)
    try:
        price_val = float(price)
    except Exception:
        return None

    if is_price_acceptable(price_val, window=window, variance_threshold=variance_threshold):
        return record
    return None


def filter_records(records: List[Dict[str, Any]],
                  price_key: str = "price",
                  window: int = DEFAULT_WINDOW_SIZE,
                  variance_threshold: float = DEFAULT_VARIANCE_THRESHOLD) -> List[Dict[str, Any]]:
    """Filter a list of telemetry records, keeping only those within the variance limit.

    This helper is convenient for batch processing pipelines.
    """
    out: List[Dict[str, Any]] = []
    for rec in records:
        filtered = filter_record(rec, price_key=price_key, window=window, variance_threshold=variance_threshold)
        if filtered is not None:
            out.append(filtered)
    return out

__all__: List[str] = [
    "is_price_acceptable",
    "filter_record",
    "filter_records",
]
