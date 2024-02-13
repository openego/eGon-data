"""
The central module containing all code dealing with existing storage units for
eTraGo.
"""

import geopandas as gpd
import pandas as pd
from egon.data import db, config
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
)


class StorageEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="StorageEtrago",
            version="0.0.9",
            dependencies=dependencies,
            tasks=(insert_PHES, extendable_batteries),
        )


def insert_PHES():
    # Get datasets configuration
    sources = config.datasets()["storage_etrago"]["sources"]
    targets = config.datasets()["storage_etrago"]["targets"]

    engine = db.engine()

    scenario = config.settings()["egon-data"]["--scenarios"]
    for scn in scenario:
        # Delete outdated data on pumped hydro units (PHES) inside Germany from database
        db.execute_sql(
            f"""
            DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
            WHERE carrier = 'pumped_hydro'
            AND scn_name = '{scn}'
            AND bus IN (SELECT bus_id FROM {sources['bus']['schema']}.{sources['bus']['table']}
                           WHERE scn_name = '{scn}'
                           AND country = 'DE');
            """
        )

        # Select data on PSH units from database
        phes = db.select_dataframe(
            f"""SELECT scenario as scn_name, bus_id as bus, carrier, el_capacity as p_nom
            FROM {sources['storage']['schema']}.{sources['storage']['table']}
            WHERE carrier = 'pumped_hydro'
            AND scenario= '{scn}'
            """
        )

        # Select unused index of buses
        next_bus_id = db.next_etrago_id("storage")

        # Add missing PHES specific information suitable for eTraGo selected from scenario_parameter table
        parameters = get_sector_parameters(
            "electricity", scn
        )["efficiency"]["pumped_hydro"]
        phes["storage_id"] = range(next_bus_id, next_bus_id + len(phes))
        phes["max_hours"] = parameters["max_hours"]
        phes["efficiency_store"] = parameters["store"]
        phes["efficiency_dispatch"] = parameters["dispatch"]
        phes["standing_loss"] = parameters["standing_loss"]
        phes["cyclic_state_of_charge"] = parameters["cyclic_state_of_charge"]

        # Write data to db
        phes.to_sql(
            targets["storage"]["table"],
            engine,
            schema=targets["storage"]["schema"],
            if_exists="append",
            index=phes.index,
        )


def extendable_batteries_per_scenario(scenario):
    # Get datasets configuration
    sources = config.datasets()["storage_etrago"]["sources"]
    targets = config.datasets()["storage_etrago"]["targets"]

    engine = db.engine()

    # Delete outdated data on extendable battetries inside Germany from database
    db.execute_sql(
        f"""
        DELETE FROM {targets['storage']['schema']}.{targets['storage']['table']}
        WHERE carrier = 'battery'
        AND scn_name = '{scenario}'
        AND bus IN (SELECT bus_id FROM {sources['bus']['schema']}.{sources['bus']['table']}
                       WHERE scn_name = '{scenario}'
                       AND country = 'DE');
        """
    )

    extendable_batteries = db.select_dataframe(
        f"""
        SELECT bus_id as bus, scn_name FROM
        {sources['bus']['schema']}.
        {sources['bus']['table']}
        WHERE carrier = 'AC'
        AND scn_name = '{scenario}'
        AND (bus_id IN (SELECT bus_id
                       FROM {sources['ehv-substation']['schema']}.{sources['ehv-substation']['table']})
        OR bus_id IN (SELECT bus_id
                       FROM {sources['hv-substation']['schema']}.{sources['hv-substation']['table']}
        ))
        """
    )

    # Select information on allocated capacities for home batteries from database
    home_batteries = db.select_dataframe(
        f"""
        SELECT el_capacity as p_nom_min, bus_id as bus FROM
        {sources['storage']['schema']}.
        {sources['storage']['table']}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}';
        """
    )

    # Update index
    extendable_batteries[
        "storage_id"
    ] = extendable_batteries.index + db.next_etrago_id("storage")

    # Set parameters
    extendable_batteries["p_nom_extendable"] = True

    extendable_batteries["capital_cost"] = get_sector_parameters(
        "electricity", scenario
    )["capital_cost"]["battery"]

    extendable_batteries["lifetime"] = get_sector_parameters(
        "electricity", scenario
    )["lifetime"]["battery storage"]

    extendable_batteries["max_hours"] = get_sector_parameters(
        "electricity", scenario
    )["efficiency"]["battery"]["max_hours"]

    extendable_batteries["efficiency_store"] = get_sector_parameters(
        "electricity", scenario
    )["efficiency"]["battery"]["store"]

    extendable_batteries["efficiency_dispatch"] = get_sector_parameters(
        "electricity", scenario
    )["efficiency"]["battery"]["dispatch"]

    extendable_batteries["standing_loss"] = get_sector_parameters(
        "electricity", scenario
    )["efficiency"]["battery"]["standing_loss"]

    extendable_batteries["cyclic_state_of_charge"] = get_sector_parameters(
        "electricity", scenario
    )["efficiency"]["battery"]["cyclic_state_of_charge"]

    extendable_batteries["carrier"] = "battery"

    # Merge dataframes to fill p_nom_min column
    extendable_batteries = extendable_batteries.merge(
        right=home_batteries, left_on="bus", right_on="bus", how="outer"
    )
    extendable_batteries["p_nom_min"] = extendable_batteries[
        "p_nom_min"
    ].fillna(0)

    # Write data to db
    extendable_batteries.to_sql(
        targets["storage"]["table"],
        engine,
        schema=targets["storage"]["schema"],
        if_exists="append",
        index=False,
    )


def extendable_batteries():
    for scn in config.settings()["egon-data"]["--scenarios"]:
        extendable_batteries_per_scenario(scn)
