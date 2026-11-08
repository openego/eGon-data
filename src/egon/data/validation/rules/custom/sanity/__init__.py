"""Sanity check validation rules for eGon data quality."""

from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .etrago_bus_table import (  # noqa: F401
    BusTableAttributes,
    BusTableConnected,
    BusTableMvGridBuses,
    BusTableNeighbourCoverage,
    BusTableNetwork,
    BusTableScenarioCoverage,
)
from .etrago_generator_table import (  # noqa: F401
    GeneratorTableBus,
    GeneratorTableBusCarrier,
    GeneratorTableForeignAdequacy,
    GeneratorTableForeignCoverage,
    GeneratorTableMaxUnitSize,
    GeneratorTableNationalCapacity,
    GeneratorTablePNom,
    GeneratorTableScenarioCoverage,
    GeneratorTableStaticLimits,
    GeneratorTableTimeseriesCoverage,
    GeneratorTableTimeseriesValues,
)
from .etrago_generators import (  # noqa: F401
    EtragoGeneratorCapacity,
    EtragoGeneratorMarginalCost,
    EtragoGeneratorPositiveCapacity,
    EtragoGeneratorScenarioCoverage,
    EtragoGeneratorTimeseriesCoverage,
    EtragoGeneratorTimeseriesRange,
    EtragoGeneratorUniquePerBusCarrier,
)
from .etrago_load_table import (  # noqa: F401
    LoadTableAnnualDemand,
    LoadTableBus,
    LoadTableBusCarrier,
    LoadTableDemandDefinition,
    LoadTableForeignCoverage,
    LoadTableForeignProfile,
    LoadTableHeatCoverage,
    LoadTableMvGridCoverage,
    LoadTableScenarioCoverage,
    LoadTableTimeseriesCoverage,
    LoadTableTimeseriesValues,
)
from .gas import (  # noqa: F401
    ElectrolyserCapPerState,
    GasBuildYearWithinScenario,
    GasBusReferences,
    GasComponentTotal,
    GasNetworkSupply,
    GasVoronoiCoverage,
    OcgtMatchesPowerPlants,
)
from .heat_etrago import (  # noqa: F401
    HeatBuses,
    HeatScenarioCoverage,
    HeatStores,
    HeatSupplyCapacity,
    HeatTimeseries,
)
from .heat_supply import (  # noqa: F401
    DistrictHeatingSupplyAreaReference,
    DistrictHeatingSupplyChpCapacity,
    DistrictHeatingSupplyScenarioTarget,
    DistrictHeatingSupplyUniqueCarrier,
    IndividualHeatingSupplyHeatPumpTarget,
)
from .hgv_charging import (  # noqa: F401
    HgvAnnualEnergy,
    HgvChargingPointProfiles,
    HgvChargingPointRange,
    HgvChargingPointSites,
    HgvDrivingLoadEnergy,
    HgvEventContract,
    HgvEventEnergy,
    HgvEventReferences,
    HgvHighwayDayLoad,
    HgvHighwayDayPeak,
    HgvLinkPower,
    HgvMinimumSites,
    HgvModelComponents,
    HgvModelCount,
    HgvOrphanTimeseries,
    HgvProfiles,
    HgvScenarioCoverage,
    HgvSiteBusAssignment,
    HgvSitePower,
    HgvSiteValues,
    HgvSlotChains,
    HgvStoreCapacity,
    HgvStoreInitial,
    HgvTimeseriesExists,
    HgvTimeseriesValues,
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
from .mit_charging import (  # noqa: F401
    MitBandReachable,
    MitBusByLevel,
    MitDeficitWithinBattery,
    MitDiagnostics,
    MitEventArrayReferences,
    MitEventArrayTotal,
    MitEventArrayUseCase,
    MitFallbackDistance,
    MitImportCounts,
    MitLinkParams,
    MitLocationAssigned,
    MitLocationOccupancy,
    MitLowflexPair,
    MitModelComponents,
    MitModelEnergy,
    MitOrphans,
    MitScenarioCoverage,
    MitStoreBand,
    MitTechData,
    MitTotalEnergy,
    MitTransmissionModels,
    MitVoltageLevel,
)
from .ocgt_etrago import (  # noqa: F401
    OcgtCapacity,
    OcgtNepCapacity,
    OcgtParameters,
    OcgtPositiveCapacity,
    OcgtScenarioCoverage,
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
