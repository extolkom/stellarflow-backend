from __future__ import annotations

import struct
from typing import NamedTuple

# Binary format for a telemetry payload frame.
# Fields (all little-endian, unaligned):
#   asset_id  : 8s  — 8-byte ASCII asset identifier (e.g. b"NGN/XLM\x00")
#   price     : q   — int64 scaled price (10^7 fixed-point)
#   timestamp : Q   — uint64 Unix timestamp in milliseconds
#   sequence  : I   — uint32 sequence / nonce counter
#   flags     : H   — uint16 status flags bitmask
#
# Total frame size: 8 + 8 + 8 + 4 + 2 = 30 bytes (unaligned, no padding)
_FRAME_FMT = "<8sqQIH"
_FRAME_SIZE = struct.calcsize(_FRAME_FMT)  # 30 bytes


class TelemetryFrame(NamedTuple):
    asset_id: bytes   # exactly 8 bytes
    price: int        # int64 — scaled to 10^7
    timestamp: int    # uint64 — milliseconds since epoch
    sequence: int     # uint32
    flags: int        # uint16


def pack_frame(frame: TelemetryFrame) -> bytes:
    """Pack a :class:`TelemetryFrame` into a compact 30-byte binary buffer.

    Uses ``struct.pack`` with a little-endian, unaligned format so the output
    is the smallest possible raw byte array with no JSON overhead.
    """
    asset_bytes = frame.asset_id[:8].ljust(8, b"\x00")
    return struct.pack(_FRAME_FMT, asset_bytes, frame.price, frame.timestamp, frame.sequence, frame.flags)


def unpack_frame(data: bytes) -> TelemetryFrame:
    """Unpack a 30-byte binary buffer back into a :class:`TelemetryFrame`."""
    asset_id, price, timestamp, sequence, flags = struct.unpack(_FRAME_FMT, data[:_FRAME_SIZE])
    return TelemetryFrame(
        asset_id=asset_id.rstrip(b"\x00"),
        price=price,
        timestamp=timestamp,
        sequence=sequence,
        flags=flags,
    )


def pack_frames(frames: list[TelemetryFrame]) -> bytes:
    """Pack a batch of frames into a single contiguous byte array."""
    return b"".join(pack_frame(f) for f in frames)


def unpack_frames(data: bytes) -> list[TelemetryFrame]:
    """Unpack a contiguous byte array produced by :func:`pack_frames`."""
    return [
        unpack_frame(data[i: i + _FRAME_SIZE])
        for i in range(0, len(data), _FRAME_SIZE)
        if len(data) - i >= _FRAME_SIZE
    ]


__all__ = [
    "TelemetryFrame",
    "pack_frame",
    "unpack_frame",
    "pack_frames",
    "unpack_frames",
    "FRAME_SIZE",
]

FRAME_SIZE = _FRAME_SIZE
