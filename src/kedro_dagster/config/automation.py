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

    Example YAML snippet:

        ```yaml
        schedules:
            daily_schedule:
                cron_schedule: "0 6 * * *"     # every day at 06:00
                execution_timezone: "UTC"
                description: "Run every morning"
                metadata:
                    owner: data-platform
        ```
    """

    cron_schedule: str
    execution_timezone: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None
