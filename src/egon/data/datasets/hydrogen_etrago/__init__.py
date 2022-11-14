"""The central module containing all code dealing with heat sector in etrago
"""
from egon.data.datasets import Dataset
from egon.data.datasets.hydrogen_etrago.bus import (
    insert_hydrogen_buses,
    insert_hydrogen_buses_eGon100RE,
)
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
    insert_H2_overground_storage,
    insert_H2_saltcavern_storage,
    insert_H2_storage_eGon100RE,
    write_saltcavern_potential,
)


class HydrogenBusEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenBusEtrago",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                write_saltcavern_potential,
                insert_hydrogen_buses,
                insert_hydrogen_buses_eGon100RE,
            ),
        )


class HydrogenStoreEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenStoreEtrago",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(
                insert_H2_overground_storage,
                insert_H2_saltcavern_storage,
                insert_H2_storage_eGon100RE,
            ),
        )


class HydrogenPowerLinkEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenPowerLinkEtrago",
            version="0.0.4",
            dependencies=dependencies,
            tasks=(
                insert_power_to_h2_to_power,
                insert_power_to_h2_to_power_eGon100RE,
            ),
        )


class HydrogenMethaneLinkEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenMethaneLinkEtrago",
            version="0.0.5",
            dependencies=dependencies,
            tasks=(insert_h2_to_ch4_to_h2, insert_h2_to_ch4_eGon100RE),
        )


class HydrogenGridEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenGridEtrago",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(insert_h2_pipelines,),
        )
