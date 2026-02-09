"""Sanity check validation rules for eGon data quality."""

from .residential_electricity import (
    ResidentialElectricityAnnualSum,
    ResidentialElectricityHhRefinement,
)
from .cts_demand import (
    CtsElectricityDemandShare,
    CtsHeatDemandShare,
)
from .home_batteries import (
    HomeBatteriesAggregation,
)
from .gas_stores import (
    CH4StoresCapacity,
    H2SaltcavernStoresCapacity,
)
from .gas_grid import (
    GasBusesIsolated,
    GasBusesCount,
    GasOnePortConnections,
    CH4GridCapacity,
    GasLinksConnections,
)
from .gas_loads_generators import (
    GasLoadsCapacity,
    GasGeneratorsCapacity,
)
from .electricity_capacity import (
    ElectricityCapacityComparison,
)
from .heat_demand import (
    HeatDemandValidation,
)
from .electrical_load_sectors import (
    ElectricalLoadSectorBreakdown,
)
from .dsm import (
    DSMTimeseries,
)
from .pv_rooftop import (
    PvRooftopBuildingsValidation,
)
from .emobility_mit import (
    EVAllocationCount,
    EVGridDistrictAllocation,
    EVTripTimeranges,
    EVTripChargingDemand,
    EVModelComponentsCreated,
    EVModelTimeseriesLength,
    EVModelEnergyDemand,
    EVModelStorageCapacity,
    EVModelSoCConstraint,
    EVLowflexDrivingLoad,
)
from .gas_abroad import (
    GasBusesIsolatedAbroad,
    CH4LoadsAbroad,
    H2LoadsAbroad,
    CH4GeneratorsAbroad,
    CH4StoresAbroad,
    CH4GridLinksAbroad,
)
from .heat_gas_load import (
    HeatGasLoadPypsaEurComparison,
)

# Auto-generate __all__ from imported names (excludes private/module names)
__all__ = [
    name for name in dir()
    if not name.startswith('_') and isinstance(globals()[name], type)
]