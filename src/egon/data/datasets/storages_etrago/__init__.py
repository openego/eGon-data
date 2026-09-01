"""
The central module containing all code dealing with existing storage units for
eTraGo.
"""

import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.scenario_parameters import get_sector_parameters


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

    targets = DatasetTargets(tables={"storage": "grid.egon_etrago_storage"})

    #:
    name: str = "StorageEtrago"
    #:
    version: str = "0.0.10"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_PHES, extendable_batteries),
        )


def insert_PHES():
    engine = db.engine()

    scenario = config.settings()["egon-data"]["--scenarios"]
    for scn in scenario:
        # Delete outdated data on pumped hydro units (PHES) inside Germany from database
        db.execute_sql(f"""
            DELETE FROM {StorageEtrago.targets.tables['storage']}
            WHERE carrier = 'pumped_hydro'
            AND scn_name = '{scn}'
            AND bus IN (SELECT bus_id FROM {StorageEtrago.sources.tables['bus']}
                           WHERE scn_name = '{scn}'
                           AND country = 'DE');
            """)

        # Select data on PSH units from database
        phes = db.select_dataframe(
            f"""SELECT scenario as scn_name, bus_id as bus, carrier, el_capacity as p_nom
            FROM {StorageEtrago.sources.tables['storage']}
            WHERE carrier = 'pumped_hydro'
            AND scenario= '{scn}'
            """
        )

        # Add missing PHES specific information suitable for eTraGo selected from scenario_parameter table
        parameters = get_sector_parameters("electricity", scn)["efficiency"][
            "pumped_hydro"
        ]
        phes["storage_id"] = db.next_etrago_id("storage", len(phes))
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

    # Delete outdated data on extendable batteries inside Germany from
    # database - covers 'BESS'/'home_battery' (the two carriers this
    # function now writes) plus the old undifferentiated
    # 'battery' carrier it used to write, to clean up stale rows left over
    # from before this split.
    db.execute_sql(f"""
        DELETE FROM {StorageEtrago.targets.tables['storage']}
        WHERE carrier IN ('battery', 'BESS', 'home_battery')
        AND scn_name = '{scenario}'
        AND bus IN (SELECT bus_id FROM {StorageEtrago.sources.tables['bus']}
                        WHERE scn_name = '{scenario}'
                        AND country = 'DE');
        """)

    substation_buses = db.select_dataframe(f"""
        SELECT bus_id as bus, scn_name FROM
        {StorageEtrago.sources.tables['bus']}
        WHERE carrier = 'AC'
        AND scn_name = '{scenario}'
        AND (bus_id IN (SELECT bus_id
                        FROM {StorageEtrago.sources.tables['ehv-substation']})
        OR bus_id IN (SELECT bus_id
                        FROM {StorageEtrago.sources.tables['hv-substation']}
        ))
        """)

    # Efficiency/max_hours/standing_loss/cyclic_state_of_charge are shared
    # between BESS and home_battery - identical in the source cost data,
    # Only capital_cost/lifetime differ per carrier.
    efficiency = get_sector_parameters("electricity", scenario)["efficiency"][
        "battery"
    ]
    capital_cost = get_sector_parameters("electricity", scenario)[
        "capital_cost"
    ]
    lifetime = get_sector_parameters("electricity", scenario)["lifetime"]

    carriers = {
        "BESS": {
            "capital_cost": capital_cost["BESS"],
            "lifetime": lifetime["BESS storage"],
        },
        "home_battery": {
            "capital_cost": capital_cost["home_battery"],
            "lifetime": lifetime["home battery storage"],
        },
    }

    extendable_batteries = pd.DataFrame()

    for carrier, params in carriers.items():
        # Aggregate per bus
        allocated_capacity = db.select_dataframe(f"""
            SELECT bus_id as bus, SUM(el_capacity) as p_nom_min FROM
            {StorageEtrago.sources.tables['storage']}
            WHERE carrier = '{carrier}'
            AND scenario = '{scenario}'
            GROUP BY bus_id;
            """)

        batteries = substation_buses.copy()
        batteries["storage_id"] = db.next_etrago_id(
            "storage", len(batteries.index)
        )
        batteries["p_nom_extendable"] = True
        batteries["capital_cost"] = params["capital_cost"]
        batteries["lifetime"] = params["lifetime"]
        batteries["max_hours"] = efficiency["max_hours"]
        batteries["efficiency_store"] = efficiency["store"]
        batteries["efficiency_dispatch"] = efficiency["dispatch"]
        batteries["standing_loss"] = efficiency["standing_loss"]
        batteries["cyclic_state_of_charge"] = efficiency[
            "cyclic_state_of_charge"
        ]
        batteries["carrier"] = carrier

        # Merge to fill p_nom_min column. Left merge: only keep the
        # eligible substation buses - a real/modeled battery bus with no
        # matching substation (e.g. an unmatched MaStR location) must not
        # create a storage row with NULL scn_name/storage_id.
        batteries = batteries.merge(
            right=allocated_capacity,
            left_on="bus",
            right_on="bus",
            how="left",
        )
        batteries["p_nom_min"] = batteries["p_nom_min"].fillna(0)

        extendable_batteries = pd.concat([extendable_batteries, batteries])

    # Write data to db
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
