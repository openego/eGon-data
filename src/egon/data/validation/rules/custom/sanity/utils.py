"""Utility functions for sanity check validation rules."""

from egon.data import config, db
from egon.data.datasets.scenario_parameters import get_sector_parameters


def get_cbat_pbat_ratio(scenario: str = "eGon2035"):
    """
    Mean ratio between the storage capacity and the power of the pv rooftop
    system

    Parameters
    ----------
    scenario : str
        Scenario name (default: "eGon2035")

    Returns
    -------
    int
        Mean ratio between the storage capacity and the power of the pv
        rooftop system. First tries to get from etrago_storage table,
        falls back to scenario parameters if table is empty.
    """
    sources = config.datasets()["home_batteries"]["sources"]

    sql = f"""
    SELECT max_hours
    FROM {sources["etrago_storage"]["schema"]}
    .{sources["etrago_storage"]["table"]}
    WHERE carrier = 'home_battery'
    LIMIT 1
    """

    df = db.select_dataframe(sql, warning=False)

    if df.empty:
        # No home_battery data in storage table - get from scenario parameters
        # This matches the source in datasets/storages/home_batteries.py:112-114
        return get_sector_parameters("electricity", scenario)[
            "efficiency"
        ]["battery"]["max_hours"]

    return int(df.iat[0, 0])
