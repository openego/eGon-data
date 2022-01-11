"""The central module containing all code dealing with heat sector in etrago
"""
from egon.data.datasets import Dataset
from egon.data.datasets.hydrogen_etrago.bus import insert_hydrogen_buses
from egon.data.datasets.hydrogen_etrago.h2_to_ch4 import insert_h2_to_ch4_to_h2
from egon.data.datasets.hydrogen_etrago.power_to_h2 import (
    insert_power_to_h2_to_power,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    calculate_and_map_saltcavern_storage_potential,
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
)


class HydrogenBusEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenBusEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                calculate_and_map_saltcavern_storage_potential,
                insert_hydrogen_buses,
            ),
        )


class HydrogenStoreEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenStoreEtrago",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(insert_H2_overground_storage, insert_H2_saltcavern_storage),
        )


class HydrogenPowerLinkEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenPowerLinkEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(insert_power_to_h2_to_power),
        )


class HydrogenMethaneLinkEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenMethaneLinkEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(insert_h2_to_ch4_to_h2),
        )
