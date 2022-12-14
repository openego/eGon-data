"""The central module containing all code dealing with ocgt in etrago
"""
from egon.data.datasets import Dataset
from egon.data.datasets.power_etrago.match_ocgt import (
    insert_open_cycle_gas_turbines,
)


class OpenCycleGasTurbineEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="OpenCycleGasTurbineEtrago",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(insert_open_cycle_gas_turbines,),
        )
