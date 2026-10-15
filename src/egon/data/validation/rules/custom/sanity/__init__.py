"""Sanity check validation rules for eGon data quality."""

from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .heat_supply import (  # noqa: F401
    DistrictHeatingSupplyAreaReference,
    DistrictHeatingSupplyChpCapacity,
    DistrictHeatingSupplyScenarioTarget,
    DistrictHeatingSupplyUniqueCarrier,
    IndividualHeatingSupplyHeatPumpTarget,
)
from .individual_heat_pumps import (  # noqa: F401
    HeatPeakLoadRange,
    HeatPumpCapacityRange,
    HeatPumpCapacityTarget,
    HeatPumpFloorContainment,
    HeatPumpFloorNotReduced,
    HeatPumpMinimumRows,
    HeatPumpNotInDistrictHeating,
    HeatPumpPeakLoadExists,
    HeatPumpScenarioCoverage,
    HeatPumpSizing,
    HeatTimeseriesCoverage,
    HeatTimeseriesEnergyBalance,
    HeatTimeseriesHeatPumpCapacity,
    HeatTimeseriesValues,
)
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
