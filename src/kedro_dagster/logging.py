"""Drop-in logging shim that routes to Dagster logger when available."""

from __future__ import annotations

import logging as _logging

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
    except Exception as e:  # Fallback if no active Dagster context
        _logging.debug(f"No active Dagster context: {e}")

    # Otherwise, fall back to Python logging
    return _logging.getLogger(name)


def dagster_rich_formatter() -> structlog.stdlib.ProcessorFormatter:
    """Create a rich console formatter for Dagster logging.

    This formatter provides human-readable, colorized console output suitable
    for development and interactive use. It includes timestamps, logger names,
    log levels, and stack info when available.

    Returns:
        structlog.stdlib.ProcessorFormatter: A formatter configured for rich
        console output with automatic fallback for older structlog versions.
    """
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
    """Create a JSON formatter for Dagster logging.

    This formatter produces structured JSON output suitable for log aggregation
    systems, monitoring tools, and production environments. Each log entry is
    a single JSON object with consistent field names and ISO timestamps.

    Returns:
        structlog.stdlib.ProcessorFormatter: A formatter configured for JSON
        output with automatic fallback for older structlog versions.
    """
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
    """Create a colored formatter for Dagster logging using coloredlogs.

    This formatter provides colorized console output with customizable field
    and level styling. It uses a traditional logging format with timestamps,
    logger names, log levels, and messages, but with color highlighting for
    better readability in terminal environments.

    Returns:
        coloredlogs.ColoredFormatter: A formatter with blue level names,
        green timestamps, and red error messages.
    """
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
