"""The central module containing all code dealing with heat sector in etrago
"""
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.insert_etrago_buses import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)


def insert_H2_overground_storage():
    """Insert H2 steel tank storage for every H2 bus.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    """
    # The targets of etrago_hydrogen also serve as source here ಠ_ಠ
    sources = config.datasets()["etrago_hydrogen"]["targets"]
    targets = config.datasets()["etrago_hydrogen"]["targets"]

    # Place storage at every H2 bus
    storages = db.select_geodataframe(
        f"""
        SELECT bus_id, scn_name, geom
        FROM {sources['hydrogen_buses']['schema']}.
        {sources['hydrogen_buses']['table']} WHERE carrier LIKE 'H2%%'""",
        index_col="bus_id",
    )

    carrier = "H2_overground"
    # Add missing column
    storages["bus"] = storages.index
    storages["carrier"] = carrier

    # Does e_nom_extenable = True render e_nom useless?
    storages["e_nom"] = 0
    storages["e_nom_extendable"] = True

    # "Synergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    storages["capital_cost"] = 8.4 * 1e3

    # Remove useless columns
    storages.drop(columns=["geom"], inplace=True)

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_store WHERE "carrier" = '{carrier}';
        """
    )

    # Select next id value
    new_id = db.next_etrago_id("store")
    storages["store_id"] = range(new_id, new_id + len(storages))
    storages = storages.reset_index(drop=True)
    print(storages)

    # Insert data to db
    storages.to_sql(
        targets['hydrogen_stores']['table'],
        db.engine(),
        schema=targets['hydrogen_stores']['schema'],
        index=False,
        if_exists="append",
    )
