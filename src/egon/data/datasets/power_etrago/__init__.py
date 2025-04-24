"""The central module containing all code dealing with ocgt in etrago
"""
from egon.data.datasets import Dataset
from egon.data.datasets.power_etrago.match_ocgt import (
    insert_open_cycle_gas_turbines,
)


class OpenCycleGasTurbineEtrago(Dataset):
    """
    Insert open cycle gas turbines (OCGT) into the database.

    *Dependencies*
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasAreasnep2037_2025
      <egon.data.datasets.gas_areas.GasAreaseGon2037_2025>`
      * :py:class:`PowerPlants <egon.data.datasets.power_plants.PowerPlants>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """
    def __init__(self, dependencies):
        super().__init__(
            name="OpenCycleGasTurbineEtrago",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_open_cycle_gas_turbines,),
        )
