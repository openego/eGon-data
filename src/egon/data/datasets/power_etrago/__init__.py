"""
The central module containing all code dealing with open cycle gas turbine
"""
from egon.data.datasets import Dataset
from egon.data.datasets.power_etrago.match_ocgt import (
    insert_open_cycle_gas_turbines,
)


class OpenCycleGasTurbineEtrago(Dataset):
    """
    Insert the open cycle gas turbine links into the database

    Insert the open cycle gas turbine links into the database by using
    the function :py:func:`insert_open_cycle_gas_turbines <egon.data.datasets.power_etrago.match_ocgt.insert_open_cycle_gas_turbines>`.

    *Dependencies*
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`PowerPlants <egon.data.datasets.power_plants.PowerPlants>`
      * :py:class:`mastr_data_setup <egon.data.datasets.mastr.mastr_data_setup>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "OpenCycleGasTurbineEtrago"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_open_cycle_gas_turbines,),
        )
