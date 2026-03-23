"""Fallback league mapping when the main project is not on PYTHONPATH.

Replace this module or prepend your repo so `map_league` matches your DB.
"""

from typing import Optional, Set

_unmapped: Set[str] = set()


def map_league(comp_name: str, internal_code: str, source: str = "") -> Optional[str]:
    """Return an internal league code, or None to skip the match (tier-1 rows only).

    Standalone default: accept all competitions so odds are scraped.
    """
    if not (comp_name or "").strip():
        return "UNKNOWN"
    return comp_name.strip()[:64]


def get_unmapped_tournaments() -> Set[str]:
    return set(_unmapped)


def reset_unmapped_tournaments() -> None:
    _unmapped.clear()
