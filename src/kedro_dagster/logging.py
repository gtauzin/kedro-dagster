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

import coloredlogs
import dagster as dg
import structlog


def getLogger(name: str | None = None) -> _logging.Logger:
    """Return a logger, preferring Dagster's logger when a run is active.

    Args:
        name: Optional logger name, consistent with ``logging.getLogger``.

    Returns:
        logging.Logger: A standard logger instance. When a Dagster run is
        active, this is backed by Dagster's logging machinery.
    """
    try:
        # If there's an active Dagster context, this will succeed
        context = dg.OpExecutionContext.get()
        if context:
            logger: _logging.Logger = dg.get_dagster_logger(name)
            return logger
    except Exception:  # Fallback if no active Dagster context
        pass

    # Otherwise, fall back to Python logging
    return _logging.getLogger(name)


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


def dagster_rich_formatter() -> structlog.stdlib.ProcessorFormatter:
    foreign_pre_chain = [
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.stdlib.ExtraAdder(),
    ]

    processors = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.dev.ConsoleRenderer(),
    ]

    try:
        # Try the newer API with processors list
        return structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=foreign_pre_chain,
            processors=processors,
        )
    except TypeError:
        # Fallback to older API with single processor
        # Chain the processors manually for older versions
        processor = structlog.dev.ConsoleRenderer()
        return structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=foreign_pre_chain,
            processor=processor,
        )


def dagster_json_formatter() -> structlog.stdlib.ProcessorFormatter:
    foreign_pre_chain = [
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.stdlib.ExtraAdder(),
    ]

    json_renderer = structlog.processors.JSONRenderer(sort_keys=True, ensure_ascii=False)
    processors = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        json_renderer,
    ]

    try:
        # Try the newer API with processors list
        return structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=foreign_pre_chain,
            processors=processors,
        )
    except TypeError:
        # Fallback to older API with single processor
        return structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=foreign_pre_chain,
            processor=json_renderer,
        )


def dagster_colored_formatter() -> coloredlogs.ColoredFormatter:
    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S %z"
    field_styles = {
        "levelname": {"color": "blue"},
        "asctime": {"color": "green"},
    }
    level_styles = {
        "debug": {},
        "error": {"color": "red"},
    }

    return coloredlogs.ColoredFormatter(
        fmt=fmt,
        datefmt=datefmt,
        field_styles=field_styles,
        level_styles=level_styles,
    )


# Be explicit about public exports
__all__ = [
    "getLogger",
    "dagster_rich_formatter",
    "dagster_json_formatter",
    "dagster_colored_formatter",
]
# Extend with every public name from stdlib logging (excluding private and our getLogger override)
__all__.extend(n for n in dir(_logging) if not n.startswith("_") and n != "getLogger")
