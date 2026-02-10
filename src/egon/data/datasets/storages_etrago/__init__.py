"""
The central module containing all code dealing with existing storage units for
eTraGo.
"""

import geopandas as gpd
import pandas as pd
from egon.data import db, config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
)


class StorageEtrago(Dataset):
    """
    Adds pumped hydro storage units and extendable batteries to the data base

    This data sets adds storage unit to the data base used for transmission
    grid optimisation with the tool eTraGo. In a first step pumped hydro
    storage units for Germany are taken from an interim table and technical
    parameters such as standing losses, efficiency and max_hours are added.
    Afterwards the data is written to the correct tables which are accessed by
    eTraGo.
    In a next step batteries are added. On the one hand these are home
    batteries, assumptions on their capacity and distribution is taken from an
    other interim table. In addition extendable batteries with an installed
    capacity of 0 are added to every substation to allow a battery expansion in
    eTraGo. For all batteries assumptions on technical parameters are added.
    The resulting data is written to the corresponding tables in the data base.

    *Dependencies*
    * :py:class:`Storages <egon.data.datasets.storages.Storages>`
    * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
    * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`

    *Resulting tables*
    * :py:class:`grid.egon_etrago_storage <egon.data.datasets.etrago_setup.EgonPfHvStorage>` is extended

    """
    sources = DatasetSources(
        tables={
            "storage": "supply.egon_storages",
            "scenario_parameters": "scenario.egon_scenario_parameters",
            "bus": "grid.egon_etrago_bus",
            "ehv-substation": "grid.egon_ehv_substation",
            "hv-substation": "grid.egon_hvmv_substation",
        }
    )
    targets = DatasetTargets(
        tables={
            "storage": "grid.egon_etrago_storage"
        }
    )

    #:
    name: str = "StorageEtrago"
    #:
    version: str = "0.0.13"


    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_PHES, extendable_batteries),
        )
def insert_PHES():
    # Get datasets configuration

    engine = db.engine()

    scenario = config.settings()["egon-data"]["--scenarios"]
    for scn in scenario:
        # Delete outdated data on pumped hydro units (PHES) inside Germany from database
        db.execute_sql(
            f"""
            DELETE FROM {StorageEtrago.targets.tables['storage']}
            WHERE carrier = 'pumped_hydro'
            AND scn_name = '{scn}'
            AND bus IN (SELECT bus_id FROM {StorageEtrago.sources.tables['bus']}
                           WHERE scn_name = '{scn}'
                           AND country = 'DE');
            """
        )

        # Select data on PSH units from database
        phes = db.select_dataframe(
            f"""SELECT scenario as scn_name, bus_id as bus, carrier, el_capacity as p_nom
            FROM {StorageEtrago.sources.tables['storage']}
            WHERE carrier = 'pumped_hydro'
            AND scenario= '{scn}'
            """
        )

        # Select unused index of buses
        next_bus_id = db.next_etrago_id("storage")

        # Add missing PHES specific information suitable for eTraGo selected from scenario_parameter table
        parameters = get_sector_parameters("electricity", scn)["efficiency"][
            "pumped_hydro"
        ]
        phes["storage_id"] = range(next_bus_id, next_bus_id + len(phes))
        phes["max_hours"] = parameters["max_hours"]
        phes["efficiency_store"] = parameters["store"]
        phes["efficiency_dispatch"] = parameters["dispatch"]
        phes["standing_loss"] = parameters["standing_loss"]
        phes["cyclic_state_of_charge"] = parameters["cyclic_state_of_charge"]

        # Write data to db
        phes.to_sql(
            StorageEtrago.targets.get_table_name("storage"),
            engine,
            schema=StorageEtrago.targets.get_table_schema("storage"),
            if_exists="append",
            index=phes.index,
        )


def extendable_batteries_per_scenario(scenario):
    engine = db.engine()

    db.execute_sql(
        f"""
        DELETE FROM {StorageEtrago.targets.tables['storage']}
        WHERE carrier = 'battery'
        AND scn_name = '{scenario}'
        AND bus IN (SELECT bus_id FROM {StorageEtrago.sources.tables['bus']}
                        WHERE scn_name = '{scenario}'
                        AND country = 'DE');
        """
    )

    # 1. Get Grid Buses
    extendable_batteries = db.select_dataframe(
        f"""
        SELECT bus_id as bus, scn_name FROM
        {StorageEtrago.sources.tables['bus']}
        WHERE carrier = 'AC'
        AND scn_name = '{scenario}'
        AND (bus_id IN (SELECT bus_id
                        FROM {StorageEtrago.sources.tables['ehv-substation']})
        OR bus_id IN (SELECT bus_id
                        FROM {StorageEtrago.sources.tables['hv-substation']}
        ))
        """
    )

    home_batteries = db.select_dataframe(
        f"""
        SELECT el_capacity as p_nom_min, bus_id as bus FROM
        {StorageEtrago.sources.tables['storage']}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}';
        """
    )


    extendable_batteries = extendable_batteries.merge(
        right=home_batteries, left_on="bus", right_on="bus", how="outer"
    )

   
    extendable_batteries["scn_name"] = extendable_batteries["scn_name"].fillna(scenario)
    extendable_batteries["p_nom_min"] = extendable_batteries["p_nom_min"].fillna(0)


    
    extendable_batteries["storage_id"] = db.next_etrago_id(
        "storage", len(extendable_batteries.index)
    )

    extendable_batteries["p_nom_extendable"] = True
    extendable_batteries["carrier"] = "battery"

    params = get_sector_parameters("electricity", scenario)

    extendable_batteries["capital_cost"] = params["capital_cost"]["battery"]
    extendable_batteries["lifetime"] = params["lifetime"]["battery storage"]
    extendable_batteries["max_hours"] = params["efficiency"]["battery"]["max_hours"]
    extendable_batteries["efficiency_store"] = params["efficiency"]["battery"]["store"]
    extendable_batteries["efficiency_dispatch"] = params["efficiency"]["battery"]["dispatch"]
    extendable_batteries["standing_loss"] = params["efficiency"]["battery"]["standing_loss"]
    extendable_batteries["cyclic_state_of_charge"] = params["efficiency"]["battery"]["cyclic_state_of_charge"]

    extendable_batteries.to_sql(
        StorageEtrago.targets.get_table_name("storage"),
        engine,
        schema=StorageEtrago.targets.get_table_schema("storage"),
        if_exists="append",
        index=False,
    )


def extendable_batteries():
    for scn in config.settings()["egon-data"]["--scenarios"]:
        extendable_batteries_per_scenario(scn)