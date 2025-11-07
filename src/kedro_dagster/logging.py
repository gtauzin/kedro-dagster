"""Drop-in logging shim that routes to Dagster logger when available.

This module mirrors Python's standard ``logging`` API while overriding
``getLogger(name)`` so that, when a Dagster run is active, it returns a
Dagster-aware logger from ``dagster.get_dagster_logger``. Otherwise, it
delegates to the standard library's ``logging.getLogger``.

Usage:
    # Replace standard import
    # from logging import getLogger  # old
    from kedro_dagster.logging import getLogger  # new

    LOGGER = getLogger(__name__)

The rest of the standard ``logging`` API is re-exported for convenience,
so ``import kedro_dagster.logging as logging`` should work for most
typical use cases.
"""

from __future__ import annotations

import logging as _logging
from typing import Any

import dagster as dg


def getLogger(name: str | None = None) -> _logging.Logger:
    """Return a logger, preferring Dagster's logger when a run is active.

    Args:
        name: Optional logger name, consistent with ``logging.getLogger``.

    Returns:
        logging.Logger: A standard logger instance. When a Dagster run is
        active, this is backed by Dagster's logging machinery.
    """
    logger: _logging.Logger
    try:
        # If there's an active Dagster context, this will succeed
        context = dg.OpExecutionContext.get()
        if context:
            logger = dg.get_dagster_logger(name)
    except Exception:
        # Otherwise, fall back to Python logging
        logger = _logging.getLogger(name)

    return logger


# Re-export the standard logging API for convenience so users can
# ``import kedro_dagster.logging as logging`` with minimal friction.
def _reexport_std_logging(namespace: dict[str, Any]) -> None:
    for attr in dir(_logging):
        if attr.startswith("_"):
            continue
        # Don't overwrite our custom getLogger
        if attr == "getLogger":
            continue
        namespace[attr] = getattr(_logging, attr)


# Populate module globals with stdlib logging names
_reexport_std_logging(globals())


# Be explicit about public exports
__all__ = [
    "getLogger",
]
# Extend with every public name from stdlib logging (excluding private and our getLogger override)
__all__.extend(n for n in dir(_logging) if not n.startswith("_") and n != "getLogger")
