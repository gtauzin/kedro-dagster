"""Configuration definitions for Kedro-Dagster."""

from pydantic import BaseModel


class ScheduleOptions(BaseModel):
    pass


class SensorOptions(BaseModel):
    pass
