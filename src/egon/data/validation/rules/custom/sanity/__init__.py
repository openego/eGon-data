"""Sanity check validation rules for eGon data quality."""

from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .gas import (  # noqa: F401
    ElectrolyserCapPerState,
    GasBuildYearWithinScenario,
    GasBusesWithoutLink,
    GasBusReferences,
    GasComponentTotal,
    GasDuplicateBusLocations,
    GasDuplicateLinks,
    GasOnePortsPerBus,
    GasParameterMatch,
    GasTimeseriesComplete,
    GasVoronoiCoverage,
    H2DemandAboveElectrolyserCap,
    H2GridComponents,
    WasteHeatBoundedByElectrolysis,
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
