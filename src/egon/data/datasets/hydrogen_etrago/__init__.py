"""
The central module containing the definitions of the datasets linked to H2

This module contains the definitions of the datasets linked to the
hydrogen sector in eTraGo in Germany.

There is no H2 bus abroad in the scenarios, so technologies linked to the
hydrogen sector are present only in Germany.

"""

from egon.data import config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.hydrogen_etrago.bus import insert_hydrogen_buses
from egon.data.datasets.hydrogen_etrago.h2_grid import insert_h2_pipelines
from egon.data.datasets.hydrogen_etrago.h2_to_ch4 import insert_h2_to_ch4_to_h2
from egon.data.datasets.hydrogen_etrago.power_to_h2 import (
    insert_power_to_h2_to_power,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
    write_saltcavern_potential,
)


class HydrogenBusEtrago(Dataset):
    """
    Insert the H2 buses into the database for Germany

    Insert the H2 buses in Germany into the database.
    :py:func:`calculate_and_map_saltcavern_storage_potential <egon.data.datasets.hydrogen_etrago.storage.calculate_and_map_saltcavern_storage_potential>`,
    :py:func:`insert_hydrogen_buses <egon.data.datasets.hydrogen_etrago.bus.insert_hydrogen_buses>` and

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
    version: str = "0.0.5.dev"

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
                insert_h2_buses_for_scn,
            ),
        )


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
    version: str = "0.0.7.dev"

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
            tasks=(
                insert_H2_overground_storage,
                insert_H2_saltcavern_storage,
            ),
        )


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
    version: str = "0.0.7.dev"

    sources = DatasetSources(
        tables={
            "buses": "grid.egon_etrago_bus",
            "links": "grid.egon_etrago_link",
            "H2_AC_map": "grid.egon_etrago_ac_h2",
            "ehv_substation": "grid.egon_ehv_substation",
            "hvmv_substation": "grid.egon_hvmv_substation",
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "mv_districts": "grid.egon_mv_grid_district",
            "ehv_voronoi": "grid.egon_ehv_substation_voronoi",
            "district_heating_area": "demand.egon_district_heating_areas",
            "o2_load_profile": "demand.egon_demandregio_timeseries_cts_ind",
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
        )


class HydrogenMethaneLinkEtrago(Dataset):
    """
    Insert the methanisation, feed in and SMR into the database

    Insert the the methanisation, feed in and Steam
    Methane Reaction (SMR) links in Germany into the database for the
    scenarios by executing the function
    :py:func:`insert_h2_to_ch4_to_h2 <egon.data.datasets.hydrogen_etrago.h2_to_ch4.insert_h2_to_ch4_to_h2>`

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`
      * :py:class:`HydrogenGridEtrago <HydrogenGridEtrago>`
      * :py:class:`HydrogenPowerLinkEtrago <HydrogenPowerLinkEtrago>`

    *Resulting*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "HydrogenMethaneLinkEtrago"
    #:
    version: str = "0.0.7.dev"

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
        )


class HydrogenGridEtrago(Dataset):
    """
    Insert the H2 grid in Germany into the database.

    Insert the H2 links (pipelines) into Germany in the database for the
    scenarios by executing the function
    :py:func:`insert_h2_pipelines <egon.data.datasets.hydrogen_etrago.h2_grid.insert_h2_pipelines>`.

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`GasAreas <egon.data.datasets.gas_areas.GasAreas>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`


    *Resulting*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "HydrogenGridEtrago"
    #:
    version: str = "0.0.4.dev"

    sources = DatasetSources(
        urls={
            "new_constructed_pipes": "https://fnb-gas.de/wp-content/uploads/2024/07/2024_07_22_Anlage3_FNB_Massnahmenliste_Neubau.xlsx",
            "converted_ch4_pipes": "https://fnb-gas.de/wp-content/uploads/2024/07/2024_07_22_Anlage4_FNB_Massnahmenliste_Umstellung.xlsx",
            "pipes_of_further_h2_grid_operators": "https://fnb-gas.de/wp-content/uploads/2024/07/2024_07_22_Anlage2_Leitungsmeldungen_weiterer_potenzieller_Wasserstoffnetzbetreiber.xlsx",
        },
        files={
            "new_constructed_pipes": "Anlage_3_Wasserstoffkernnetz_Neubau.xlsx",
            "converted_ch4_pipes": "Anlage_4_Wasserstoffkernnetz_Umstellung.xlsx",
            "pipes_of_further_h2_grid_operators": "Anlage_2_Wasserstoffkernetz_weitere_Leitungen.xlsx",
        },
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
            tasks=(insert_h2_pipelines_for_scn,),
        )


def insert_h2_pipelines_for_scn():
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if scn_name in ["eGon2035", "reGon2037", "reGon2045"]:
            insert_h2_pipelines(scn_name)


def insert_h2_buses_for_scn():
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if scn_name in ["eGon2035", "reGon2037", "reGon2045"]:
            insert_hydrogen_buses(scn_name)