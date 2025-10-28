"""Configuration definitions for Kedro-Dagster schedules.

Defines the schema for schedule entries referenced by jobs in `dagster.yml`.
"""

from typing import Any

from pydantic import BaseModel


class ScheduleOptions(BaseModel):
    """Options for defining Dagster schedules.

    Attributes:
        cron_schedule (str): Cron expression for the schedule.
        execution_timezone (str | None): Timezone in which the schedule should execute.
        description (str | None): Optional description of the schedule.
        metadata (dict[str, Any] | None): Additional metadata for the schedule.
    """

    cron_schedule: str
    execution_timezone: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None
