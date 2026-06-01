from __future__ import annotations

from typing import Dict, List

# Telemetry calculation loops for historical node uptime percentages.
# Raw float outputs are rounded to 2 decimal places to keep log sizes clean.


def calculate_uptime_percentage(hits: int, total: int) -> float:
    """Return node uptime as a clean 2-decimal percentage."""
    if total == 0:
        return 0.0
    return round((hits / total) * 100, 2)


def calculate_window_percentages(
    windows: Dict[str, Dict[str, int]]
) -> Dict[str, float]:
    """Calculate uptime percentages for multiple nodes across variable timeframes.

    Parameters
    ----------
    windows:
        Mapping of node_id -> {"hits": int, "total": int}

    Returns
    -------
    Dict[str, float]
        node_id -> rounded uptime percentage
    """
    return {
        node_id: calculate_uptime_percentage(
            data.get("hits", 0), data.get("total", 0)
        )
        for node_id, data in windows.items()
    }


def summarise_metrics(records: List[Dict[str, int]]) -> List[float]:
    """Return a list of rounded uptime percentages from a batch of records."""
    return [
        calculate_uptime_percentage(r.get("hits", 0), r.get("total", 0))
        for r in records
    ]


__all__ = [
    "calculate_uptime_percentage",
    "calculate_window_percentages",
    "summarise_metrics",
]