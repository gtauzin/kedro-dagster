"""Configuration definitions for Kedro-Dagster."""

from pydantic import BaseModel


# TODO: Map name
class ScheduleOptions(BaseModel):
    cron_schedule: str
    execution_timezone: str | None = None
    description: str | None = None
    metadata: dict | None = None
