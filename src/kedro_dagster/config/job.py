"""Configuration definitions for Kedro-Dagster."""

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from .automation import ScheduleOptions
from .execution import ExecutorOptions


class PipelineOptions(BaseModel):
    pipeline_name: str | None = None
    from_nodes: Iterable[str] | None = None
    to_nodes: Iterable[str] | None = None
    node_names: Iterable[str] | None = None
    from_inputs: Iterable[str] | None = None
    to_outputs: Iterable[str] | None = None
    namespace: str | None = None
    tags: Iterable[str] | None = None

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
