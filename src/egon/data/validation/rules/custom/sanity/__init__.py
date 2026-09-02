"""Sanity check validation rules for eGon data quality."""

from .cts_demand import (  # noqa: F401
    CtsElectricityDemandShare,
    CtsHeatDemandShare,
)
from .dsm import DSMTimeseries  # noqa: F401
from .electrical_load_sectors import (  # noqa: F401
    ElectricalLoadSectorBreakdown,
)
from .electricity_capacity import (  # noqa: F401
    ElectricityCapacityComparison,
)
from .emobility_mit import (  # noqa: F401
    EVAllocationCount,
    EVGridDistrictAllocation,
    EVLowflexDrivingLoad,
    EVModelComponentsCreated,
    EVModelEnergyDemand,
    EVModelSoCConstraint,
    EVModelStorageCapacity,
    EVModelTimeseriesLength,
    EVTripChargingDemand,
    EVTripTimeranges,
)
from .gas_abroad import (  # noqa: F401
    CH4GeneratorsAbroad,
    CH4GridLinksAbroad,
    CH4LoadsAbroad,
    CH4StoresAbroad,
    GasBusesIsolatedAbroad,
    H2LoadsAbroad,
)
from .gas_grid import (  # noqa: F401
    CH4GridCapacity,
    GasBusesCount,
    GasBusesIsolated,
    GasLinksConnections,
    GasOnePortConnections,
)
from .gas_loads_generators import (  # noqa: F401
    GasGeneratorsCapacity,
    GasLoadsCapacity,
)
from .gas_stores import (  # noqa: F401
    CH4StoresCapacity,
    H2SaltcavernStoresCapacity,
)
from .heat_demand import HeatDemandValidation  # noqa: F401
from .heat_gas_load import HeatGasLoadPypsaEurComparison  # noqa: F401
from .home_batteries import HomeBatteriesAggregation  # noqa: F401
from .pv_rooftop import PvRooftopBuildingsValidation  # noqa: F401
from .residential_electricity import (  # noqa: F401
    ResidentialElectricityAnnualSum,
    ResidentialElectricityHhRefinement,
)

# Auto-generate __all__ from imported names (excludes private/module names)
__all__ = [
    name
    for name in dir()
    if not name.startswith("_") and isinstance(globals()[name], type)
]
