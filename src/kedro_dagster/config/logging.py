"""Configuration definitions for Kedro-Dagster loggers.

Defines the schema for logger entries referenced by jobs in `dagster.yml`.
"""

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# Valid Python logging levels (normalized to uppercase)
LogLevel = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]


class LoggerOptions(BaseModel):
    """Options for defining Dagster loggers.

    Attributes:
        logger_name (str): Name of the logger to use. Must follow Python module naming conventions.
        log_level (LogLevel): Logging level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET).
        handlers (list[dict[str, Any]] | None): List of handler config dicts.
        formatters (dict[str, dict[str, Any]] | None): Formatter configs, name→config.
        filters (dict[str, dict[str, Any]] | None): Filter configs, name→config.

    """

    logger_name: str = Field(
        ...,
        description="Logger name following Python module naming conventions",
        min_length=1,
    )
    log_level: LogLevel = Field(
        default="INFO",
        description="Python logging level (case-insensitive)",
    )
    handlers: list[dict[str, Any]] | None = Field(
        default=None,
        description="List of handler configurations",
    )
    formatters: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Formatter configurations mapped by name",
    )
    filters: dict[str, dict[str, Any]] | None = Field(
        default=None,
        description="Filter configurations mapped by name",
    )

    class Config:
        """Pydantic configuration enforcing strict fields and assignment validation."""

        validate_assignment = True
        extra = "forbid"

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, v: str) -> str:
        """Normalize log level to uppercase for case-insensitive matching.

        Args:
            v: Log level string to normalize.

        Returns:
            Normalized log level in uppercase.

        Raises:
            ValueError: If log level is not a valid string.
        """
        if not isinstance(v, str):
            raise ValueError("Log level must be a string")

        normalized = v.upper().strip()

        # Check if the normalized level is valid
        valid_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if normalized not in valid_levels:
            raise ValueError(
                f"Invalid log level '{v}'. Must be one of: {', '.join(sorted(valid_levels))} (case-insensitive)"
            )

        return normalized

    @field_validator("logger_name")
    @classmethod
    def validate_logger_name(cls, v: str) -> str:
        """Validate logger name follows Python module naming conventions.

        Args:
            v: Logger name to validate.

        Returns:
            Validated logger name.

        Raises:
            ValueError: If logger name is invalid.
        """
        # Check for valid Python identifier pattern (allows dots for module-style names)
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$", v):
            raise ValueError(
                f"Logger name '{v}' must follow Python module naming conventions "
                "(alphanumeric + underscore, dot-separated)"
            )

        return v

    @field_validator("handlers")
    @classmethod
    def validate_handlers(cls, v: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
        """Validate handler configurations.

        Args:
            v: Handler configurations to validate.

        Returns:
            Validated handler configurations.

        Raises:
            ValueError: If handler configuration is invalid.
        """
        if v is None:
            return v

        for i, handler in enumerate(v):
            if "class" not in handler:
                raise ValueError(f"Handler at index {i} must specify a 'class' field")

            if not isinstance(handler["class"], str):
                raise ValueError(f"Handler class at index {i} must be a string")

        return v

    @field_validator("formatters")
    @classmethod
    def validate_formatters(cls, v: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]] | None:
        """Validate formatter configurations.

        Args:
            v: Formatter configurations to validate.

        Returns:
            Validated formatter configurations.

        Raises:
            ValueError: If formatter configuration is invalid.
        """
        if v is None:
            return v

        for name, formatter in v.items():
            if not isinstance(formatter, dict):
                raise ValueError(f"Formatter '{name}' configuration must be a dictionary")

            # Require either 'format' for standard formatter or '()' for custom class
            has_format = "format" in formatter
            has_callable = "()" in formatter

            if not (has_format or has_callable):
                raise ValueError(f"Formatter '{name}' must specify either 'format' field or '()' for custom class")

            # Type validation for keys when present
            if has_format and not isinstance(formatter["format"], str):
                raise ValueError(f"Formatter '{name}' 'format' must be a string")

            if has_callable and not isinstance(formatter["()"], str):
                raise ValueError(f"Formatter '{name}' '()' must be a string import path")

        return v

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, v: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]] | None:
        """Validate filter configurations.

        Args:
            v: Filter configurations to validate.

        Returns:
            Validated filter configurations.

        Raises:
            ValueError: If filter configuration is invalid.
        """
        if v is None:
            return v

        for name, filter_config in v.items():
            if not isinstance(filter_config, dict):
                raise ValueError(f"Filter '{name}' configuration must be a dictionary")

            # Require either custom callable/class path via '()' or a class path via 'class'
            has_callable = "()" in filter_config
            has_class = "class" in filter_config

            if not (has_callable or has_class):
                raise ValueError(
                    f"Filter '{name}' must specify either '()' for custom callable/class or 'class' for import path"
                )

            # Basic type checks for keys when present
            if has_callable and not isinstance(filter_config["()"], str):
                raise ValueError(f"Filter '{name}' '()' must be a string import path")

            if has_class and not isinstance(filter_config["class"], str):
                raise ValueError(f"Filter '{name}' class must be a string")

        return v

    @model_validator(mode="after")
    def validate_references(self) -> "LoggerOptions":
        """Validate that handler/formatter/filter references are consistent.

        Returns:
            Self after validation.

        Raises:
            ValueError: If there are inconsistent references.
        """
        # Collect available formatter and filter names
        available_formatters = set(self.formatters.keys()) if self.formatters else set()
        available_filters = set(self.filters.keys()) if self.filters else set()

        # Check handlers reference valid formatters/filters
        if self.handlers:
            for i, handler in enumerate(self.handlers):
                # Check formatter reference
                if "formatter" in handler:
                    formatter_name = handler["formatter"]
                    if formatter_name not in available_formatters:
                        raise ValueError(
                            f"Handler at index {i} references unknown formatter '{formatter_name}'. "
                            f"Available formatters: {sorted(available_formatters)}"
                        )

                # Check filter references
                if "filters" in handler:
                    handler_filters = handler["filters"]
                    if isinstance(handler_filters, list):
                        for filter_name in handler_filters:
                            if filter_name not in available_filters:
                                raise ValueError(
                                    f"Handler at index {i} references unknown filter '{filter_name}'. "
                                    f"Available filters: {sorted(available_filters)}"
                                )

        return self
