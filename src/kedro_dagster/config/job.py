"""Configuration definitions for Kedro-Dagster."""

from typing import Any

from pydantic import BaseModel

from .automation import ScheduleOptions
from .execution import ExecutorOptions


class PipelineOptions(BaseModel):
    pipeline_name: str | None = None
    from_nodes: list[str] | None = None
    to_nodes: list[str] | None = None
    node_names: list[str] | None = None
    from_inputs: list[str] | None = None
    to_outputs: list[str] | None = None
    node_namespace: str | None = None
    tags: list[str] | None = None

    class Config:
        extra = "forbid"


class NodeOptions(BaseModel):
    node_name: str | None = None
    config: dict[str, Any] | None = None

    class Config:
        extra = "forbid"


class JobOptions(BaseModel):
    pipeline: PipelineOptions
    executor: ExecutorOptions | str | None = None
    schedule: ScheduleOptions | str | None = None

    class Config:
        extra = "forbid"
