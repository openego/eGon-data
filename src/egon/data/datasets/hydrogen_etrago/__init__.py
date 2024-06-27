"""
The central module containing the definitions of the datasets linked to H2

This module contains the definitions of the datasets linked to the
hydrogen sector in eTraGo in Germany.

In the eGon2035 scenario, there is no H2 bus abroad, so technologies
linked to the hydrogen sector are present only in Germany.

In the eGon100RE scenario, the potential and installed capacities abroad
arrise from the PyPSA-eur-sec run. For this reason, this module focuses
only on the hydrogen related components in Germany, and the module
:py:mod:`pypsaeursec <egon.data.datasets.pypsaeursec>` on the hydrogen
related components abroad.

"""
from egon.data.datasets import Dataset
from egon.data.datasets.hydrogen_etrago.bus import insert_hydrogen_buses
from egon.data.datasets.hydrogen_etrago.h2_grid import insert_h2_pipelines
from egon.data.datasets.hydrogen_etrago.h2_to_ch4 import (
    insert_h2_to_ch4_eGon100RE,
    insert_h2_to_ch4_to_h2,
)
from egon.data.datasets.hydrogen_etrago.power_to_h2 import (
    insert_power_to_h2_to_power,
    insert_power_to_h2_to_power_eGon100RE,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    calculate_and_map_saltcavern_storage_potential,
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
    insert_H2_storage_eGon100RE,
)


class HydrogenBusEtrago(Dataset):
    """
    Insert the H2 buses into the database for Germany

    Insert the H2 buses in Germany into the database for the scenarios
    eGon2035 and eGon100RE by executing successively the functions
    :py:func:`calculate_and_map_saltcavern_storage_potential <egon.data.datasets.hydrogen_etrago.storage.calculate_and_map_saltcavern_storage_potential>`,
    :py:func:`insert_hydrogen_buses <egon.data.datasets.hydrogen_etrago.bus.insert_hydrogen_buses>` and
    :py:func:`insert_hydrogen_buses_eGon100RE <egon.data.datasets.hydrogen_etrago.bus.insert_hydrogen_buses_eGon100RE>`.

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
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                calculate_and_map_saltcavern_storage_potential,
                insert_hydrogen_buses,
            ),
        )


class HydrogenStoreEtrago(Dataset):
    """
    Insert the H2 stores into the database for Germany

    Insert the H2 stores in Germany into the database for the scenarios
    eGon2035 and eGon100RE:
      * H2 overground stores or steel tanks at each H2_grid bus with the
        function :py:func:`insert_H2_overground_storage <egon.data.datasets.hydrogen_etrago.storage.insert_H2_overground_storage>`
        for the scenario eGon2035,
      * H2 underground stores or saltcavern stores at each H2_saltcavern
        bus with the function :py:func:`insert_H2_saltcavern_storage <egon.data.datasets.hydrogen_etrago.storage.insert_H2_saltcavern_storage>`
        for the scenario eGon2035,
      * H2 stores (overground and underground) for the scenario eGon100RE
        with the function :py:func:`insert_H2_storage_eGon100RE <egon.data.datasets.hydrogen_etrago.storage.insert_H2_storage_eGon100RE>`.

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
    version: str = "0.0.3"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                insert_H2_overground_storage,
                insert_H2_saltcavern_storage,
                insert_H2_storage_eGon100RE,
            ),
        )


class HydrogenPowerLinkEtrago(Dataset):
    """
    Insert the electrolysis and the fuel cells into the database

    Insert the the electrolysis and the fuel cell links in Germany into
    the database for the scenarios eGon2035 and eGon100RE by executing
    successively the functions :py:func:`insert_power_to_h2_to_power <egon.data.datasets.hydrogen_etrago.power_to_h2.insert_power_to_h2_to_power>`
    and :py:func:`insert_power_to_h2_to_power_eGon100RE <egon.data.datasets.hydrogen_etrago.power_to_h2.insert_power_to_h2_to_power_eGon100RE>`.

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
    version: str = "0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                insert_power_to_h2_to_power,
            ),
        )


class HydrogenMethaneLinkEtrago(Dataset):
    """
    Insert the methanisation, feed in and SMR into the database

    Insert the the methanisation, feed in (only in eGon2035) and Steam
    Methane Reaction (SMR) links in Germany into the database for the
    scenarios eGon2035 and eGon100RE by executing successively the
    functions :py:func:`insert_h2_to_ch4_to_h2 <egon.data.datasets.hydrogen_etrago.h2_to_ch4.insert_h2_to_ch4_to_h2>`
    and :py:func:`insert_h2_to_ch4_eGon100RE <egon.data.datasets.hydrogen_etrago.h2_to_ch4.insert_h2_to_ch4_eGon100RE>`.

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
    version: str = "0.0.5"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_to_ch4_to_h2, insert_h2_to_ch4_eGon100RE),
        )


class HydrogenGridEtrago(Dataset):
    """
    Insert the H2 grid in Germany into the database for eGon100RE

    Insert the H2 links (pipelines) into Germany in the database for the
    scenario eGon100RE by executing the function
    :py:func:`insert_h2_pipelines <egon.data.datasets.hydrogen_etrago.h2_grid.insert_h2_pipelines>`.

    *Dependencies*
      * :py:class:`SaltcavernData <egon.data.datasets.saltcavern.SaltcavernData>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec>`
      * :py:class:`HydrogenBusEtrago <HydrogenBusEtrago>`


    *Resulting*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "HydrogenGridEtrago"
    #:
    version: str = "0.0.2"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_h2_pipelines,),
        )
