"""The central module containing all code dealing with gas neighbours
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
    Inserts generation, demand, grid, OCGTs and gas neighbors into database.

    *Dependencies*
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec.PypsaEurSec>`
      * :py:class:`ElectricalNeighbours <egon.data.datasets.electrical_neighbours.ElectricalNeighbours>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`GasAreaseGon100RE <egon.data.datasets.gas_areas.GasAreaseGon100RE>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
      
    """
    def __init__(self, dependencies):
        super().__init__(
            name="GasNeighbours",
            version="0.0.4",
            dependencies=dependencies,
            tasks=(
                tyndp_gas_generation,
                tyndp_gas_demand,
                grid,
                insert_ocgt_abroad,
                insert_gas_neigbours_eGon100RE,
            ),
        )
