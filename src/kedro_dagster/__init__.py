"""Kedro plugin for running a project with Dagster.

This library automatically suppresses pydantic deprecation warnings from external
dependencies (like Dagster). To disable this behavior, set the environment variable:

    SUPPRESS_DAGSTER_PYDANTIC_WARNINGS=false
"""

import logging as _logging

from .catalog import CatalogTranslator
from .dagster import ExecutorCreator, LoggerCreator, ScheduleCreator
from .datasets import NOTHING_OUTPUT, DagsterNothingDataset, DagsterPartitionedDataset
from .kedro import KedroRunTranslator
from .logging import dagster_colored_formatter, dagster_json_formatter, dagster_rich_formatter, getLogger
from .nodes import NodeTranslator
from .pipelines import PipelineTranslator
from .translator import DagsterCodeLocation, KedroProjectTranslator

_logging.getLogger(__name__).setLevel(_logging.INFO)


__all__ = [
    "CatalogTranslator",
    "NOTHING_OUTPUT",
    "DagsterNothingDataset",
    "DagsterPartitionedDataset",
    "ExecutorCreator",
    "LoggerCreator",
    "ScheduleCreator",
    "KedroRunTranslator",
    "NodeTranslator",
    "PipelineTranslator",
    "DagsterCodeLocation",
    "KedroProjectTranslator",
    "dagster_rich_formatter",
    "dagster_json_formatter",
    "dagster_colored_formatter",
    "getLogger",
]
