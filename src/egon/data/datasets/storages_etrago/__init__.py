"""
The central module containing all code dealing with existing storage units for
eTraGo.
"""

import geopandas as gpd
from egon.data import db, config
from egon.data.datasets import Dataset



class StorageEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="StorageEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(),
        )


def insert_PSH():

    # Get datasets configuration
    sources = config.datasets()["storage_etrago"]["sources"]
    targets = config.datasets()["storage_etrago"]["targets"]

    engine = db.engine()

    # Delete outdated data on PSH from database
    db.execute_sql(
        f"""
        DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scn_name = 'eGon2035'
        """
    )

    # Select data on PSH units from database
    psh = db.select_dataframe(
        f"""SELECT scenario as scn_name, id as storage_id, bus_id as bus, carrier, el_capacity as p_nom
        FROM {sources['storage']['schema']}.{sources['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scenario= 'eGon2035'
        """
    )

    # Add missing PSH specific information suitable for eTraGo
    psh["control"] = "PV"
    psh["p_nom_extendable"] = False
    psh["marginal_cost_fixed"] = 0
    psh["capital_cost"] = 0 # as PSH is not extendable
    psh["state_of_charge_initial"] = 0
    psh["max_hours"] = 6 # max_hours as an average for existing German PSH, as in open_eGo
    psh["efficiency_store"] = 0.88 # according to Acatech2015
    psh["efficiency_dispatch"] = 0.89 # according to Acatech2015
    psh["standing_loss"] = 0.00052 # according to Acatech2015

    # Write data to db
    psh.to_sql(
        targets["storage"]["table"],
        engine,
        schema=targets["storage"]["schema"],
        if_exists="append",
        index=psh.index,
    )

