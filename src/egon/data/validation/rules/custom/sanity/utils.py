"""Utility functions for sanity check validation rules."""

from egon.data import config, db


def get_cbat_pbat_ratio():
    """
    Mean ratio between the storage capacity and the power of the pv rooftop
    system

    Returns
    -------
    int
        Mean ratio between the storage capacity and the power of the pv
        rooftop system
    """
    sources = config.datasets()["home_batteries"]["sources"]

    sql = f"""
    SELECT max_hours
    FROM {sources["etrago_storage"]["schema"]}
    .{sources["etrago_storage"]["table"]}
    WHERE carrier = 'home_battery'
    """

    return int(db.select_dataframe(sql).iat[0, 0])
