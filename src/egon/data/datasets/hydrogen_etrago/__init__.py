"""
The central module containing the definitions of the datasets linked to H2

This module contains the definitions of the datasets linked to the
hydrogen sector in eTraGo in Germany.

The H2 buses abroad come from PyPSA-Eur; the technologies linked to the
hydrogen sector are present only in Germany.

"""

from egon.data import config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.hydrogen_etrago.bus import insert_hydrogen_buses
from egon.data.datasets.hydrogen_etrago.h2_grid import (
    download_h2_grid_data,
    insert_h2_pipelines,
)
from egon.data.datasets.hydrogen_etrago.h2_to_ch4 import insert_h2_to_ch4_to_h2
from egon.data.datasets.hydrogen_etrago.power_to_h2 import (
    insert_power_to_h2_to_power,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
    write_saltcavern_potential,
)


def scenarios_with_h2():
    """
    Return the configured scenarios that have a H2 system (not status quo)

    Returns
    -------
    list of str
        Names of the scenarios with a H2 system

    """
    return [
        scn_name
        for scn_name in config.settings()["egon-data"]["--scenarios"]
        if "status" not in scn_name
    ]


def insert_h2_buses():
    """Insert the H2 buses of all scenarios with a H2 system."""
    for scn_name in scenarios_with_h2():
        insert_hydrogen_buses(scn_name)


def insert_h2_grid():
    """Insert the H2 grid of all scenarios with a H2 system."""
    for scn_name in scenarios_with_h2():
        insert_h2_pipelines(scn_name)


def insert_h2_stores():
    """Insert the H2 stores of all scenarios with a H2 system."""
    scenarios = scenarios_with_h2()

    if not scenarios:
        no_h2_stores_required()
        return

    for scn_name in scenarios:
        insert_H2_overground_storage(scn_name)
        insert_H2_saltcavern_storage(scn_name)


def no_h2_stores_required():
    print(
        """
          None of the required scenarios need H2 stores
          """
    )
    return None


class HydrogenBusEtrago(Dataset):
    """
    Insert the H2 buses into the database for Germany

    Insert the H2 buses in Germany into the database by executing
    successively the functions
    :py:func:`calculate_and_map_saltcavern_storage_potential <egon.data.datasets.hydrogen_etrago.storage.calculate_and_map_saltcavern_storage_potential>`
    and :py:func:`insert_hydrogen_buses <egon.data.datasets.hydrogen_etrago.bus.insert_hydrogen_buses>`.

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`

    *Resulting*
      * :py:class:`grid.egon_etrago_bus <egon.data.datasets.etrago_setup.EgonPfHvBus>` is extended

    """

    #:
    name: str = "HydrogenBusEtrago"
    #:
    version: str = "0.0.6"

    sources = DatasetSources(
        tables={
            "saltcavern_data": "grid.egon_saltstructures_storage_potential",
            "buses": "grid.egon_etrago_bus",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
            "vg250_federal_states": "boundaries.vg250_lan",
            "saltcaverns": "boundaries.inspee_saltstructures",
        },
    )

    targets = DatasetTargets(
        tables={
            "hydrogen_buses": "grid.egon_etrago_bus",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
            "storage_potential": "grid.egon_saltstructures_storage_potential",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                write_saltcavern_potential,
                download_h2_grid_data,
                insert_h2_buses,
            ),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation import TableValidation
        from egon.data.validation.rules.custom.sanity.gas import (
            GAS_SCENARIOS,
            GasComponentTotal,
            GasDuplicateBusLocations,
            for_each_scenario,
        )

        return {
            "data_quality": [
                # No row count (depends on --scenarios and the boundary); the
                # upper case columns are covered by the whole table check
                TableValidation(
                    table_name="grid.egon_etrago_ac_h2",
                    data_type_columns={
                        "bus_H2": "bigint",
                        "bus_AC": "bigint",
                        "scn_name": "text",
                    },
                    value_set_columns={"scn_name": list(GAS_SCENARIOS)},
                ),
            ],
            "sanity": [
                # status2024 has no H2 system (see scenarios_with_h2)
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_GRID_BUSES_DE",
                    table="grid.egon_etrago_bus",
                    carriers=["H2_grid"],
                    expected={
                        "status2024": 0,
                        "eGon2035": 274,
                        "reGon2037": 311,
                        "reGon2045": 394,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_BUSES_DE",
                    table="grid.egon_etrago_bus",
                    carriers=["H2"],
                    expected={
                        "status2024": 0,
                        "eGon2035": 7,
                        "reGon2037": 7,
                        "reGon2045": 7,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_SALTCAVERN_BUSES",
                    table="grid.egon_etrago_bus",
                    carriers=["H2_saltcavern"],
                    expected={
                        "status2024": 0,
                        "eGon2035": 33,
                        "reGon2037": 33,
                        "reGon2045": 33,
                    },
                ),
                *for_each_scenario(
                    GasDuplicateBusLocations,
                    "SANITY_GAS_H2_BUS_LOCATIONS",
                    table="grid.egon_etrago_bus",
                    carriers=["H2", "H2_grid"],
                ),
            ],
        }


class HydrogenStoreEtrago(Dataset):
    """
    Insert the H2 stores into the database for Germany

    Insert the H2 stores in Germany into the database for all scenarios:
      * H2 overground stores or steel tanks at each H2 bus with the
        function :py:func:`insert_H2_overground_storage <egon.data.datasets.hydrogen_etrago.storage.insert_H2_overground_storage>`
        for all scenarios,
      * H2 underground stores or saltcavern stores at each H2_saltcavern
        bus with the function :py:func:`insert_H2_saltcavern_storage <egon.data.datasets.hydrogen_etrago.storage.insert_H2_saltcavern_storage>`
        for all scenarios ,

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`
      * :py:class:`HydrogenGridEtrago <HydrogenGridEtrago>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting*
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended

    """

    #:
    name: str = "HydrogenStoreEtrago"
    #:
    version: str = "0.0.8"

    sources = DatasetSources(
        tables={
            "saltcavern_data": "grid.egon_saltstructures_storage_potential",
            "buses": "grid.egon_etrago_bus",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
        },
    )
    targets = DatasetTargets(
        tables={
            "hydrogen_stores": "grid.egon_etrago_store",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_stores,),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            TARGET_SCENARIOS,
            GasBusReferences,
            GasComponentTotal,
            GasOnePortsPerBus,
            GasParameterMatch,
            for_each_scenario,
        )

        return {
            "sanity": [
                # A steel tank at every H2 and H2_grid bus, a saltcavern
                # store at every H2_saltcavern bus (see storage.py)
                *for_each_scenario(
                    GasOnePortsPerBus,
                    "SANITY_GAS_H2_OVERGROUND_STORES",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_store",
                    bus_carriers=["H2", "H2_grid"],
                    carriers=["H2_overground"],
                ),
                *for_each_scenario(
                    GasOnePortsPerBus,
                    "SANITY_GAS_H2_UNDERGROUND_STORES",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_store",
                    bus_carriers=["H2_saltcavern"],
                    carriers=["H2_underground"],
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_UNDERGROUND_E_NOM_MAX",
                    table="grid.egon_etrago_store",
                    carriers=["H2_underground"],
                    measure=("sum", "e_nom_max"),
                    expected={
                        "status2024": 0,
                        "eGon2035": 4590769.164,
                        "reGon2037": 4590769.164,
                        "reGon2045": 4590769.164,
                    },
                ),
                *[
                    rule
                    for carrier in ["H2_overground", "H2_underground"]
                    for rule in for_each_scenario(
                        GasParameterMatch,
                        f"SANITY_GAS_{carrier.upper()}_CAPITAL_COST",
                        scenarios=TARGET_SCENARIOS,
                        table="grid.egon_etrago_store",
                        carriers=[carrier],
                        column="capital_cost",
                        parameter=("gas", "capital_cost", carrier),
                    )
                ],
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_H2_STORES_BUSES",
                    table="grid.egon_etrago_store",
                    carriers=["H2_overground", "H2_underground"],
                ),
            ],
        }


class HydrogenPowerLinkEtrago(Dataset):
    """
    Insert the electrolysis and the fuel cells into the database

    Insert the the electrolysis and the fuel cell links in Germany into
    the database for the scenarios by executing the function
    :py:func:`insert_power_to_h2_to_power <egon.data.datasets.hydrogen_etrago.power_to_h2.insert_power_to_h2_to_power>`

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`
      * :py:class:`HydrogenGridEtrago <HydrogenGridEtrago>`

    *Resulting*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "HydrogenPowerLinkEtrago"
    #:
    version: str = "0.0.9"

    sources = DatasetSources(
        tables={
            "federal_states": "boundaries.vg250_lan",
            "buses": "grid.egon_etrago_bus",
            "links": "grid.egon_etrago_link",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
            "ehv_substation": "grid.egon_ehv_substation",
            "hvmv_substation": "grid.egon_hvmv_substation",
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "district_heating_area": "demand.egon_district_heating_areas",
        },
    )
    targets = DatasetTargets(
        tables={
            "hydrogen_links": "grid.egon_etrago_link",
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "generators": "grid.egon_etrago_generator",
            "buses": "grid.egon_etrago_bus",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_power_to_h2_to_power,),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            TARGET_SCENARIOS,
            ElectrolyserCapPerState,
            GasBusReferences,
            GasParameterMatch,
            H2DemandAboveElectrolyserCap,
            WasteHeatBoundedByElectrolysis,
            for_each_scenario,
        )

        links = [
            "power_to_H2",
            "H2_to_power",
            "PtH2_waste_heat",
        ]
        return {
            "sanity": [
                *for_each_scenario(
                    ElectrolyserCapPerState,
                    "SANITY_GAS_ELECTROLYSER_NEP_CAP",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                ),
                # A warning only: reports federal states that have to
                # import H2 for their industry
                *for_each_scenario(
                    H2DemandAboveElectrolyserCap,
                    "SANITY_GAS_H2_DEMAND_ABOVE_ELECTROLYSER_CAP",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_load",
                ),
                *for_each_scenario(
                    WasteHeatBoundedByElectrolysis,
                    "SANITY_GAS_WASTE_HEAT_BOUND",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                ),
                *[
                    rule
                    for carrier in ["power_to_H2", "H2_to_power"]
                    for rule in for_each_scenario(
                        GasParameterMatch,
                        f"SANITY_GAS_{carrier.upper()}_EFFICIENCY",
                        scenarios=TARGET_SCENARIOS,
                        table="grid.egon_etrago_link",
                        carriers=[carrier],
                        column="efficiency",
                        parameter=("gas", "efficiency", carrier),
                    )
                ],
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_PTH2_LINKS_BUSES",
                    table="grid.egon_etrago_link",
                    carriers=links,
                ),
            ],
        }


class HydrogenMethaneLinkEtrago(Dataset):
    """
    Insert the methanisation and SMR into the database

    Insert the the methanisation and Steam Methane Reaction (SMR) links in
    Germany into the database for the scenarios by executing the function
    :py:func:`insert_h2_to_ch4_to_h2 <egon.data.datasets.hydrogen_etrago.h2_to_ch4.insert_h2_to_ch4_to_h2>`

    *Dependencies*
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`

    *Resulting*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "HydrogenMethaneLinkEtrago"
    #:
    version: str = "0.0.9"

    sources = DatasetSources(
        tables={
            "buses": "grid.egon_etrago_bus",
            "links": "grid.egon_etrago_link",
        },
    )
    targets = DatasetTargets(
        tables={
            "hydrogen_links": "grid.egon_etrago_link",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_to_ch4_to_h2,),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            TARGET_SCENARIOS,
            GasBusReferences,
            GasComponentTotal,
            GasDuplicateLinks,
            GasParameterMatch,
            for_each_scenario,
        )

        links = ["CH4_to_H2", "H2_to_CH4"]
        return {
            "sanity": [
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_SMR_METHANATION_LINKS",
                    table="grid.egon_etrago_link",
                    carriers=links,
                    expected={
                        "status2024": 0,
                        "eGon2035": 24,
                        "reGon2037": 24,
                        "reGon2045": 24,
                    },
                ),
                # One SMR and one methanation link per pair of buses
                *for_each_scenario(
                    GasDuplicateLinks,
                    "SANITY_GAS_SMR_METHANATION_DUPLICATES",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                    carriers=links,
                ),
                *[
                    rule
                    for carrier in links
                    for rule in for_each_scenario(
                        GasParameterMatch,
                        f"SANITY_GAS_{carrier.upper()}_EFFICIENCY",
                        scenarios=TARGET_SCENARIOS,
                        table="grid.egon_etrago_link",
                        carriers=[carrier],
                        column="efficiency",
                        parameter=("gas", "efficiency", carrier),
                    )
                ],
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_SMR_METHANATION_BUSES",
                    table="grid.egon_etrago_link",
                    carriers=links,
                ),
            ],
        }


class HydrogenGridEtrago(Dataset):
    """
    Insert the H2 grid in Germany into the database.

    Insert the H2 links (pipelines) into Germany in the database for the
    scenarios by executing the function
    :py:func:`insert_h2_pipelines
    <egon.data.datasets.hydrogen_etrago.h2_grid.insert_h2_pipelines>`,
    including the NEP measures, the cross-border links, the H2 imports and
    the removal of the CH4 pipelines converted to H2.

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`GasAreas <egon.data.datasets.gas_areas.GasAreas>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`

    *Resulting*
      * :py:class:`grid.egon_etrago_link
      <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
        (H2 links) and reduced (converted CH4 pipelines)
      * :py:class:`grid.egon_etrago_generator
      <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
        (H2 imports)

    """

    #:
    name: str = "HydrogenGridEtrago"
    #:
    version: str = "0.0.6"

    sources = DatasetSources(
        urls={
            "new_constructed_pipes": "https://fnb-gas.de/wp-content/uploads/2024/12/2024_12_10_Wasserstoff-Kernnetz_Anlage3_final_inoffiziell.xlsx",
            "converted_ch4_pipes": "https://fnb-gas.de/wp-content/uploads/2024/12/2024_12_10_Wasserstoff-Kernnetz_Anlage4_final_inoffiziell.xlsx",
            "pipes_of_further_h2_grid_operators": "https://fnb-gas.de/wp-content/uploads/2024/12/2024_12_10_Wasserstoff-Kernnetz_Anlage2_final_inoffiziell.xlsx",
        },
        files={
            "new_constructed_pipes": "Anlage_3_Wasserstoffkernnetz_Neubau_2024_12_10.xlsx",
            "converted_ch4_pipes": "Anlage_4_Wasserstoffkernnetz_Umstellung_2024_12_10.xlsx",
            "pipes_of_further_h2_grid_operators": "Anlage_2_Wasserstoffkernetz_weitere_Leitungen_2024_12_10.xlsx",
        },
        tables={
            "buses": "grid.egon_etrago_bus",
            "links": "grid.egon_etrago_link",
            "saltcavern_data": "grid.egon_saltstructures_storage_potential",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
        },
    )

    targets = DatasetTargets(
        tables={
            "hydrogen_links": "grid.egon_etrago_link",
            "generators": "grid.egon_etrago_generator",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_grid,),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            TARGET_SCENARIOS,
            GasBuildYearWithinScenario,
            GasBusesWithoutLink,
            GasBusReferences,
            GasComponentTotal,
            GasParameterMatch,
            H2GridComponents,
            for_each_scenario,
        )

        return {
            "sanity": [
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_GRID_PIPES_DE",
                    table="grid.egon_etrago_link",
                    carriers=["H2_grid"],
                    expected={
                        "status2024": 0,
                        "eGon2035": 303,
                        "reGon2037": 349,
                        "reGon2045": 495,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_GRID_PIPES_DE_KM",
                    table="grid.egon_etrago_link",
                    carriers=["H2_grid"],
                    measure=("sum", "length"),
                    expected={
                        "status2024": 0,
                        "eGon2035": 8999.161,
                        "reGon2037": 10574.011,
                        "reGon2045": 18124.084,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_GRID_BORDER_LINKS",
                    table="grid.egon_etrago_link",
                    carriers=["H2_grid"],
                    location="cross-border",
                    expected={
                        "status2024": 0,
                        "eGon2035": 17,
                        "reGon2037": 17,
                        "reGon2045": 19,
                    },
                ),
                # Largest pipeline (EHB design capacity)
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_H2_GRID_MAX_P_NOM",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                    carriers=["H2_grid"],
                    measure=("max", "p_nom"),
                    comparison="at_most",
                    expected=22756.794,
                ),
                # The CH4 pipes that remain after the removal of the pipes
                # converted to H2 (and from 2045 on: the NEP methane network)
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_CH4_PIPES_DE_REMAINING",
                    table="grid.egon_etrago_link",
                    carriers=["CH4"],
                    expected={
                        "status2024": 0,
                        "eGon2035": 12,
                        "reGon2037": 12,
                        "reGon2045": 7,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_CH4_PIPES_DE_REMAINING_KM",
                    table="grid.egon_etrago_link",
                    carriers=["CH4"],
                    measure=("sum", "length"),
                    expected={
                        "status2024": 0,
                        "eGon2035": 1417.393,
                        "reGon2037": 1417.393,
                        "reGon2045": 773.438,
                    },
                ),
                # The removal of converted CH4 pipes must not cut off a bus,
                # and every H2_grid bus has a pipeline in the scenario
                *[
                    rule
                    for carrier in ["CH4", "H2_grid"]
                    for rule in for_each_scenario(
                        GasBusesWithoutLink,
                        f"SANITY_GAS_{carrier.upper()}_BUSES_WITHOUT_LINK",
                        scenarios=TARGET_SCENARIOS,
                        table="grid.egon_etrago_bus",
                        bus_carrier=carrier,
                        link_carriers=[carrier],
                    )
                ],
                # Pipes of the core network are commissioned by the
                # scenario year at the latest
                *for_each_scenario(
                    GasBuildYearWithinScenario,
                    "SANITY_GAS_H2_GRID_BUILD_YEAR",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                    carriers=["H2_grid"],
                ),
                # Parts of the H2 grid after connect_h2_grid_islands
                *for_each_scenario(
                    H2GridComponents,
                    "SANITY_GAS_H2_GRID_PARTS",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_link",
                    expected=1,
                ),
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_GRID_PIPES_BUSES",
                    table="grid.egon_etrago_link",
                    carriers=["CH4", "H2_grid"],
                ),
                # H2 imports abroad and in Germany, see h2_grid.insert_h2_imports
                *[
                    rule
                    for rule_id, location, expected in [
                        (
                            "SANITY_GAS_H2_IMPORTS_DE_P_NOM",
                            "DE",
                            {
                                "status2024": 0,
                                "eGon2035": 3389.831,
                                "reGon2037": 3389.831,
                                "reGon2045": 34491.525,
                            },
                        ),
                        (
                            "SANITY_GAS_H2_IMPORTS_ABROAD_P_NOM",
                            "abroad",
                            {
                                "status2024": 0,
                                "eGon2035": 52881.356,
                                "reGon2037": 52881.356,
                                "reGon2045": 117372.881,
                            },
                        ),
                    ]
                    for rule in for_each_scenario(
                        GasComponentTotal,
                        rule_id,
                        table="grid.egon_etrago_generator",
                        carriers=["H2"],
                        location=location,
                        measure=("sum", "p_nom"),
                        expected=expected,
                    )
                ],
                *for_each_scenario(
                    GasParameterMatch,
                    "SANITY_GAS_H2_IMPORTS_MARGINAL_COST",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_generator",
                    carriers=["H2"],
                    column="marginal_cost",
                    parameter=("gas", "marginal_cost", "H2_import"),
                    location=None,
                ),
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_H2_IMPORTS_BUSES",
                    table="grid.egon_etrago_generator",
                    carriers=["H2"],
                ),
            ],
        }
