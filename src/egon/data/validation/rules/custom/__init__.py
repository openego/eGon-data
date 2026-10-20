"""Custom validation rules for eGon data."""

from . import sanity
from .sanity import *  # noqa: F401, F403

# Re-export everything from sanity
__all__ = sanity.__all__
