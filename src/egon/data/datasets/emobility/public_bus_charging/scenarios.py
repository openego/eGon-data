"""
Scenario mapping for public bus charging.

Bus data exists for exactly three scenarios. ``eGon2035`` is deliberately
left unchanged and ``eGon100RE`` is discarded, so neither gets bus loads --
a run of either with no bus demand is correct, not a missing-data bug.
"""

from egon.data import config

#: eGon-data scenario name -> input CSV file name, relative to the input dir.
SCENARIO_FILES = {
    "status2024": "2024_depot_power_2024_reference_hourly.csv.gz",
    "reGon2037": "depot_power_2037_hourly.csv.gz",
    "reGon2045": "2045_depot_power_2011_hourly.csv.gz",
}


def active_scenarios():
    """Scenarios that have bus data and are configured for this run.

    Returns
    -------
    list of str
        Configured scenarios that carry public bus data, in the order given
        by :data:`SCENARIO_FILES`.
    """
    configured = set(config.settings()["egon-data"]["--scenarios"])
    return [s for s in SCENARIO_FILES if s in configured]
