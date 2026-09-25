"""Sanity check validation rules for eGon data quality."""

from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .power_plants_capacity import PowerPlantsCapacityComparison  
from .pv_rooftop_buildings import (  
    PvRooftopCapacityComparison,
    PvRooftopDuplicateRows,
)
from .renewable_feedin import RenewableFeedinTimeseries  # noqa: F401
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
