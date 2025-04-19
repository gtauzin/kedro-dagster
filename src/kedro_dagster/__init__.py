"""Kedro plugin for running a project with Dagster."""

# TODO
__version__ = "0.0.1"

import logging

from .catalog import CatalogTranslator
from .dagster import ExecutorCreator, LoggerTranslator, ScheduleCreator
from .kedro import KedroRunTranslator
from .nodes import NodeTranslator
from .pipelines import PipelineTranslator
from .translator import DagsterCodeLocation, KedroProjectTranslator

logging.getLogger(__name__).setLevel(logging.INFO)


__all__ = [
    "CatalogTranslator",
    "ExecutorCreator",
    "LoggerTranslator",
    "ScheduleCreator",
    "KedroRunTranslator",
    "NodeTranslator",
    "PipelineTranslator",
    "DagsterCodeLocation",
    "KedroProjectTranslator",
]
