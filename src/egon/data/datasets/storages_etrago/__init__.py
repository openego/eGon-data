"""
The central module containing all code dealing with existing storage units for
eTraGo.
"""

import geopandas as gpd
from egon.data import db, config
import egon.data.datasets.scenario_parameters.parameters as scenario_parameters
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
    EgonScenario,
)



class StorageEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="StorageEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(),
        )


def insert_PHES():

    # Get datasets configuration
    sources = config.datasets()["storage_etrago"]["sources"]
    targets = config.datasets()["storage_etrago"]["targets"]

    engine = db.engine()

    # Delete outdated data on pumped hydro units (PHES) from database
    db.execute_sql(
        f"""
        DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scn_name = 'eGon2035'
        """
    )

    # Select data on PSH units from database
    phes = db.select_dataframe(
        f"""SELECT scenario as scn_name, id as storage_id, bus_id as bus, carrier, el_capacity as p_nom
        FROM {sources['storage']['schema']}.{sources['storage']['table']}
        WHERE carrier = 'pumped_hydro'
        AND scenario= 'eGon2035'
        """
    )


    # Add missing PHES specific information suitable for eTraGo selected from scenario_parameter table

    phes["p_nom_extendable"] = scenario_parameters.electricity("eGon2035")["phes_p_nom_extendable"]
    phes["marginal_cost_fixed"] = scenario_parameters.electricity("eGon2035")["re_marginal_cost_fixed"]
    phes["max_hours"] = scenario_parameters.electricity("eGon2035")["phes_max_hours"]
    phes["efficiency_store"] = scenario_parameters.electricity("eGon2035")["phes_efficiency_store"]
    phes["efficiency_dispatch"] = scenario_parameters.electricity("eGon2035")["phes_efficiency_dispatch"]
    phes["standing_loss"] = scenario_parameters.electricity("eGon2035")["phes_standing_loss"]

    # Write data to db
    phes.to_sql(
        targets["storage"]["table"],
        engine,
        schema=targets["storage"]["schema"],
        if_exists="append",
        index=phes.index,
    )

