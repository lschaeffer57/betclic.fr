"""Inject stub for scrapers.league_mapping before loading betclic.py."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parent.parent
BETCLIC_FILE = ROOT / "betclic.py"


def _install_scrapers_stub() -> None:
    scrapers = ModuleType("scrapers")
    lm = ModuleType("scrapers.league_mapping")

    def map_league(comp_name: str, internal_code: str, source: str = "") -> str:
        return "TEST"

    lm.map_league = map_league
    lm.get_unmapped_tournaments = lambda: []
    lm.reset_unmapped_tournaments = lambda: None
    sys.modules["scrapers"] = scrapers
    sys.modules["scrapers.league_mapping"] = lm


@pytest.fixture(scope="session")
def betclic():
    _install_scrapers_stub()
    spec = importlib.util.spec_from_file_location("betclic_scraper", BETCLIC_FILE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {BETCLIC_FILE}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
