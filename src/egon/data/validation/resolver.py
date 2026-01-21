"""Boundary resolution helpers for validation parameters."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BoundaryDependent:
    """
    Wrapper for values that vary by boundary (e.g. Schleswig-Holstein vs Everything).

    At validation runtime, the appropriate value is selected based on the
    current boundary setting.
    """
    values: Dict[str, Any]

    def resolve(self, boundary: str) -> Any:
        """Return the value for the given boundary, or the whole dict if not found."""
        if boundary in self.values:
            logger.debug("Resolved boundary-dependent value: %s -> %s", boundary, self.values[boundary])
            return self.values[boundary]
        return self.values


def resolve_boundary_dependence(boundary_dict: Dict[str, Any]) -> BoundaryDependent:
    """
    Wrap a boundary-dependent dict for deferred resolution.

    At validation runtime, the appropriate value is selected based on the
    current boundary setting.

    Example:
        expected_count=resolve_boundary_dependence({"Schleswig-Holstein": 27, "Everything": 431})
    """
    return BoundaryDependent(boundary_dict)


def resolve_value(value: Any, boundary: str) -> Any:
    """
    Resolve boundary-dependent values.

    If value is a BoundaryDependent, resolve it using the current boundary.
    Otherwise return value unchanged.
    """
    if isinstance(value, BoundaryDependent):
        return value.resolve(boundary)

    return value