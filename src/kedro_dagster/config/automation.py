"""Configuration definitions for Kedro-Dagster."""

from typing import Any

from pydantic import BaseModel


class ScheduleOptions(BaseModel):
    """Schedule configuration options."""

    cron_schedule: str
    execution_timezone: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None
