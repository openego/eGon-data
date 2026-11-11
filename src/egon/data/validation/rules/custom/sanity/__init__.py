"""Sanity check validation rules for eGon data quality."""

from .data_centers import (  # noqa: F401
    DataCenterAnnualDemand,
    DataCenterLoads,
    DataCenterLoadTimeseries,
    DataCenterPeakLoads,
    DataCenterScenarioCoverage,
    DataCenterWasteHeatGenerators,
    DataCenterWasteHeatProfile,
    DataCenterWasteHeatTimeseries,
)
from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .heat_supply import (  # noqa: F401
    DistrictHeatingSupplyAreaReference,
    DistrictHeatingSupplyChpCapacity,
    DistrictHeatingSupplyScenarioTarget,
    DistrictHeatingSupplyUniqueCarrier,
    IndividualHeatingSupplyHeatPumpTarget,
)
from .power_plants_capacity import (  # noqa: F401
    BusExists,
    PowerPlantsCapacityComparison,
)
from .pv_rooftop_buildings import (
    PvRooftopCapacityComparison,
    PvRooftopDuplicateRows,
)
from .renewable_feedin import (  # noqa: F401
    RenewableFeedinTimeseries,
    ZensusWeatherCellMappingRowCount,
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
from .storages import (  # noqa: F401
    HomeBatteryAggregationComparison,
    HomeBatteryCapacityComparison,
    HomeBatteryDuplicateRows,
    PumpedHydroCapacityComparison,
)

# Auto-generate __all__ from imported names (excludes private/module names)
__all__ = [
    name
    for name in dir()
    if not name.startswith("_") and isinstance(globals()[name], type)
]
