"""Boundary resolution helpers for validation parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BoundaryDependent:
    """
    Wrapper for values that vary by boundary.

    E.g. Schleswig-Holstein vs Everything. At validation runtime, the
    appropriate value is selected based on the current boundary setting.
    """

    values: Dict[str, Any]

    def resolve(self, boundary: str) -> Any:
        """Return value for given boundary.

        Raises KeyError if boundary is not in values.
        """
        if boundary in self.values:
            logger.debug(
                "Resolved boundary-dependent value: %s -> %s",
                boundary,
                self.values[boundary],
            )
            return self.values[boundary]
        raise KeyError(
            f"Boundary '{boundary}' not found in boundary-dependent values. "
            f"Available: {list(self.values.keys())}"
        )


def resolve_boundary_dependence(
    boundary_dict: Dict[str, Any]
) -> BoundaryDependent:
    """
    Wrap a boundary-dependent dict for deferred resolution.

    At validation runtime, the appropriate value is selected based on the
    current boundary setting.

    Example:
        expected_count=resolve_boundary_dependence(
            {"Schleswig-Holstein": 27, "Everything": 431}
        )
    """
    return BoundaryDependent(boundary_dict)


def resolve_value(value: Any, boundary: str) -> Any:
    """
    Resolve boundary-dependent values.

    If value is a BoundaryDependent, resolve it using the current boundary.
    If value is a plain dict with the boundary as a key, resolve it.
    Otherwise return value unchanged.
    """
    if isinstance(value, BoundaryDependent):
        return value.resolve(boundary)

    # Handle plain dicts with boundary keys (fallback for serialization issues)
    if isinstance(value, dict) and boundary in value:
        logger.debug(
            "Resolved plain dict value: %s -> %s", boundary, value[boundary]
        )
        return value[boundary]

    return value
