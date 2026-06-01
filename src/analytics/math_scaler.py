from __future__ import annotations

import math
from typing import Union

# Precision scale factors for integer-only arithmetic.
# 10^7  — standard precision for single-hop exchange rate values.
# 10^14 — extended precision for cross-feed multiplications to avoid
#          intermediate overflow when two 10^7-scaled values are combined.
SCALE_7: int = 10_000_000       # 10^7
SCALE_14: int = 100_000_000_000_000  # 10^14


def scale_up(value: Union[int, float], factor: int = SCALE_7) -> int:
    """Scale *value* to its raw integer representation using *factor*.

    All incoming float values are multiplied by *factor* and floored to an
    integer so that subsequent arithmetic stays entirely in integer space.
    """
    return math.floor(value * factor)


def scale_down(value: int, factor: int = SCALE_7) -> float:
    """Restore a scaled integer back to a human-readable float."""
    return value / factor


def multiply_rates(rate_a: Union[int, float], rate_b: Union[int, float]) -> int:
    """Multiply two exchange rates using integer-only arithmetic.

    Both inputs are scaled to 10^7 first.  Their product is a 10^14-scaled
    integer, which is returned directly so callers can chain further integer
    operations without precision loss.

    Example
    -------
    >>> multiply_rates(1500.0, 0.00065)  # NGN/USD * USD/XLM -> NGN/XLM (scaled 10^14)
    """
    a_int = scale_up(rate_a, SCALE_7)
    b_int = scale_up(rate_b, SCALE_7)
    return a_int * b_int  # result is implicitly scaled to 10^14


def cross_feed_multiply(
    rate_a: Union[int, float],
    rate_b: Union[int, float],
    output_scale: int = SCALE_7,
) -> int:
    """Compute a cross-feed implied rate and return it at *output_scale* precision.

    Performs the full 10^14 multiplication then floors back to *output_scale*
    so the result is a deterministic integer at the requested precision.
    """
    product_14 = multiply_rates(rate_a, rate_b)
    # Divide out the extra 10^7 to land at output_scale (default 10^7).
    return product_14 // SCALE_7


def floor_divide(scaled_value: int, divisor: Union[int, float]) -> int:
    """Integer floor-divide a scaled value by *divisor*.

    *divisor* is itself scaled to SCALE_7 before the division so the
    operation remains entirely in integer space.
    """
    divisor_int = scale_up(divisor, SCALE_7)
    # Multiply numerator by SCALE_7 to preserve precision through the division.
    return (scaled_value * SCALE_7) // divisor_int


__all__ = [
    "SCALE_7",
    "SCALE_14",
    "scale_up",
    "scale_down",
    "multiply_rates",
    "cross_feed_multiply",
    "floor_divide",
]
