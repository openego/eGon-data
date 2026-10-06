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
from .heat_etrago import (  # noqa: F401
    HeatBuses,
    HeatScenarioCoverage,
    HeatStores,
    HeatSupplyCapacity,
    HeatTimeseries,
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

# Auto-generate __all__ from imported names (excludes private/module names)
__all__ = [
    name
    for name in dir()
    if not name.startswith("_") and isinstance(globals()[name], type)
]
