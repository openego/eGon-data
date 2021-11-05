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

    sources = config.datasets()["storage_etrago"]["sources"]
    targets = config.datasets()["storage_etrago"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scn_name = 'eGon2035'
        """
    )

    psh = db.select_dataframe(
        f"""SELECT scenario as scn_name, id as storage_id, bus_id as bus, el_capacity as p_nom, geom
        FROM {sources['storage']['schema']}.{sources['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scenario= 'eGon2035'
        """
    )

    psh["control"] = "PV"
    psh["p_nom_extendable"] = False
    psh["marginal_cost_fixed"] = 0
    psh["capital_cost"] = 0
    psh[""]
