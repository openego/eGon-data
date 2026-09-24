"""Sanity check validation rules for eGon data quality."""

from .demandregio import DemandRegioScenarioDemand  # noqa: F401
from .etrago_generators import (  # noqa: F401
    EtragoGeneratorCapacity,
    EtragoGeneratorMarginalCost,
    EtragoGeneratorPositiveCapacity,
    EtragoGeneratorScenarioCoverage,
    EtragoGeneratorTimeseriesCoverage,
    EtragoGeneratorTimeseriesRange,
    EtragoGeneratorUniquePerBusCarrier,
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
