"""Configuration definitions for Kedro-Dagster."""

from pydantic import BaseModel


class ScheduleOptions(BaseModel):
    cron_schedule: str
