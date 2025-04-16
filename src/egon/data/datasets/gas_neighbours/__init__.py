"""
The central module containing definition of the datasets dealing with gas neighbours
"""

from egon.data.datasets import Dataset
from egon.data.datasets.gas_neighbours.eGon100RE import (
    insert_gas_neigbours_eGon100RE,
)
from egon.data.datasets.gas_neighbours.eGon2035 import (
    grid,
    insert_ocgt_abroad,
    tyndp_gas_demand,
    tyndp_gas_generation,
)


class GasNeighbours(Dataset):
    """
    Insert the missing gas data abroad.

    Insert the missing gas data into the database for the scenario
    eGon2035 by executing successively the functions
    :py:func:`tyndp_gas_generation <egon.data.datasets.gas_neighbours.eGon2035.tyndp_gas_generation>`,
    :py:func:`tyndp_gas_demand <egon.data.datasets.gas_neighbours.eGon2035.tyndp_gas_demand>`,
    :py:func:`grid <egon.data.datasets.gas_neighbours.eGon2035.grid>` and
    :py:func:`insert_ocgt_abroad <egon.data.datasets.gas_neighbours.eGon2035.insert_ocgt_abroad>`,
    and for the scenario eGon100RE with the function :py:func:`insert_gas_neigbours_eGon100RE <egon.data.datasets.gas_neighbours.eGon100RE.insert_gas_neigbours_eGon100RE>`.

    *Dependencies*
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec.PypsaEurSec>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`ElectricalNeighbours <egon.data.datasets.electrical_neighbours.ElectricalNeighbours>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`GasAreaseGon100RE <egon.data.datasets.gas_areas.GasAreaseGon100RE>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended

    """

    #:
    name: str = "GasNeighbours"
    #:
    version: str = "0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                tyndp_gas_generation,
                tyndp_gas_demand,
                grid,
                insert_ocgt_abroad,
                insert_gas_neigbours_eGon100RE,
            ),
        )
