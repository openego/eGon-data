"""Custom validation rules for eGon data."""

from .sanity import (
    ResidentialElectricityAnnualSum,
    ResidentialElectricityHhRefinement,
    CtsElectricityDemandShare,
    CtsHeatDemandShare,
)

__all__ = [
    "ResidentialElectricityAnnualSum",
    "ResidentialElectricityHhRefinement",
    "CtsElectricityDemandShare",
    "CtsHeatDemandShare",
]
