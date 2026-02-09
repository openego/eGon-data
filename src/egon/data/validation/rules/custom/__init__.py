"""Custom validation rules for eGon data."""

from .sanity import *  # noqa: F401, F403
from . import sanity

# Re-export everything from sanity
__all__ = sanity.__all__