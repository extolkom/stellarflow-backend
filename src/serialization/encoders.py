import orjson
from typing import Any, Dict, List, Union

# Types that are commonly used in the project for telemetry or internal messages.
JSONType = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

def encode_to_bytes(data: JSONType) -> bytes:
    """Encode *data* to a compact JSON ``bytes`` representation using ``orjson``.

    - ``orjson`` produces the smallest possible UTF‑8 JSON output and is
      significantly faster than the standard ``json`` module.
    - ``orjson.dumps`` returns ``bytes`` directly, which is ideal for
      transmission over message queues or sockets.
    - ``option=orjson.OPT_UTC_Z"`` ensures all ``datetime`` objects are
      serialized as ISO‑8601 UTC strings.
    """
    # ``orjson`` automatically handles most built‑in Python types.
    # For objects that ``orjson`` cannot serialize, callers should convert them
    # to a serializable form (e.g., via ``.dict()`` or ``.isoformat()``) before
    # invoking this function.
    return orjson.dumps(data, option=orjson.OPT_UTC_Z)

def encode_to_str(data: JSONType) -> str:
    """Encode *data* to a JSON string using ``orjson``.

    This is a thin wrapper around :func:`encode_to_bytes` for callers that
    prefer a ``str`` rather than ``bytes``.
    """
    return encode_to_bytes(data).decode("utf-8")

# Example usage (to be removed or adapted by the caller):
# payload = {"asset": "BTC", "price": 12345.67, "timestamp": datetime.utcnow()}
# encoded = encode_to_bytes(payload)
# send_to_queue(encoded)
