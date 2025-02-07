"""Translation function from Kedro to Dagtser."""

import warnings
from logging import getLogger
from pathlib import Path

import dagster as dg
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import (
    ExecutorCreator,
    LoggerTranslator,
    ScheduleCreator,
)
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipeline_hooks import PipelineHookTranslator
from kedro_dagster.pipelines import PipelineTranslator

LOGGER = getLogger(__name__)

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)


class KedroDagsterTranslator(
    NodeTranslator,
    CatalogTranslator,
    PipelineHookTranslator,
    PipelineTranslator,
    ExecutorCreator,
    ScheduleCreator,
    LoggerTranslator,
):
    """Translate Kedro project into Dagster.

    Args:
        project_path: The path to the Kedro project.
        env: A string representing the Kedro environment to use.
            If None, the default environment is used.
        conf_source: The path to the Kedro configuration source
            directory. If None, the default configuration source
            directory is used.

    Attributes:
        _named_ops: A dictionary of named Dagster operations.
        named_assets_: A dictionary of named Dagster assets
        named_resources_: A dictionary of named Dagster resources.
        named_jobs_: A dictionary of named Dagster jobs.
        named_schedules_: A dictionary of named Dagster schedules.
        named_sensors_: A dictionary of named Dagster sensors.
        named_loggers_: A dictionary of named Dagster loggers.
        named_executors_: A dictionary of named Dagster executors.

    """

    def __init__(
        self,
        project_path: Path | None = None,
        env: str | None = None,
        conf_source: Path | None = None,
    ):
        self.project_path = project_path
        self.env = env
        self.conf_source = conf_source

        self.initialialize_outputs()

        self.initialialize_kedro()
        self.load_context()
        self.load_config()

        default_pipeline = pipelines.get("__default__")
        self._named_nodes = {node.name: node for node in default_pipeline.nodes}

    def initialialize_outputs(self):
        self._named_graph_ops = {}
        self._named_ops = {}
        self.named_assets_ = {}
        self.named_resources_ = {}
        self.named_jobs_ = {}
        self.named_schedules_ = {}
        self.named_sensors_ = {}
        self.named_loggers_ = {}
        self.named_executors_ = {}

    def initialialize_kedro(self):
        LOGGER.info("Initializing Kedro...")

        self._project_path = self.project_path
        if self._project_path is None:
            self._project_path = _find_kedro_project(Path.cwd()) or Path.cwd()

        LOGGER.info("Bootstrapping project...")
        self._project_metadata = bootstrap_project(self._project_path)
        LOGGER.info(f"Project name: {self._project_metadata.project_name}")

    def load_context(self):
        LOGGER.info("Loading context...")
        self._session = KedroSession.create(
            project_path=self._project_path,
            env=self.env,
            conf_source=self.conf_source,
        )

        self._session_id = self._session.session_id
        LOGGER.info(f"Session created with ID {self._session_id}")

        self._context = self._session.load_context()
        self._catalog = self._context.catalog
        self._hook_manager = self._context._hook_manager

    def load_config(self):
        LOGGER.info("Loading configuration...")
        self._dagster_config = get_dagster_config(self._context)

    def translate(self):
        LOGGER.info("Translating Kedro project into Dagster...")

        self.translate_loggers()
        self.translate_nodes()
        self.translate_catalog()
        self.translate_pipeline_hook(run_params=self._get_run_params())
        self.create_executors()
        self.translate_pipelines()
        self.create_schedules()
