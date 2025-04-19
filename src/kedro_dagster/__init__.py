"""Kedro plugin for running a project with Dagster."""

__version__ = "0.0.1"

import logging
import warnings

import dagster as dg

from .catalog import CatalogTranslator
from .dagster import ExecutorCreator, LoggerTranslator, ScheduleCreator
from .kedro import KedroRunTranslator
from .nodes import NodeTranslator
from .pipelines import PipelineTranslator
from .translator import DagsterCodeLocation, KedroProjectTranslator

logging.getLogger(__name__).setLevel(logging.INFO)

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)

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
