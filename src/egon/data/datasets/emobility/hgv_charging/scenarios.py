"""Scenario mapping for HGV charging, kept in its own module to avoid a
circular import between the ``hgv_charging`` package ``__init__`` and its
submodules (``fill_tables``, ``spatial_assignment``, ``etrago_integration``),
all of which need this mapping at import time.
"""

from egon.data import config

# Mapping: egon-data scenario name → input-data scenario string
SCENARIO_MAP = {
    "reGon2037": "C 2037",
    "reGon2045": "C 2045",
}


def active_scenario_map() -> dict:
    """SCENARIO_MAP filtered to the scenarios configured via --scenarios."""
    active = set(config.settings()["egon-data"]["--scenarios"])
    return {k: v for k, v in SCENARIO_MAP.items() if k in active}
