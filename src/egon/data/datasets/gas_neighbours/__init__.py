"""The central module containing all code dealing with gas neighbours
"""

from egon.data.datasets import Dataset
from egon.data.datasets.gas_neighbours.eGon2035 import (
    tyndp_gas_generation,
    tyndp_gas_demand,
    grid,
)
from egon.data.datasets.gas_neighbours.eGon100RE import (
    insert_gas_neigbours_eGon100RE,
)


class GasNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasNeighbours",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                tyndp_gas_generation,
                tyndp_gas_demand,
                grid,
                insert_gas_neigbours_eGon100RE,
            ),
        )
