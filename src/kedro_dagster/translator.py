"""Translation from Kedro to Dagster."""

import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path

import dagster as dg
from kedro.framework.project import find_pipelines, settings
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
from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator
from kedro_dagster.utils import get_mlflow_resource_from_config, is_mlflow_enabled

LOGGER = getLogger(__name__)


@dataclass
class DagsterCodeLocation:
    """Represents a Kedro-based Dagster code location.

    Attributes:
        named_ops: A dictionary of named Dagster operations.
        named_assets: A dictionary of named Dagster assets.
        named_resources: A dictionary of named Dagster resources.
        named_jobs: A dictionary of named Dagster jobs.
        named_executors: A dictionary of named Dagster executors.
        named_schedules: A dictionary of named Dagster schedules.
        named_sensors: A dictionary of named Dagster sensors.
        named_loggers: A dictionary of named Dagster loggers.
    """

    named_ops: dict[str, dg.OpDefinition]
    named_assets: dict[str, dg.AssetSpec | dg.AssetsDefinition]
    named_resources: dict[str, dg.ResourceDefinition]
    named_jobs: dict[str, dg.JobDefinition]
    named_executors: dict[str, dg.ExecutorDefinition]
    named_schedules: dict[str, dg.ScheduleDefinition]
    named_sensors: dict[str, dg.SensorDefinition]
    named_loggers: dict[str, dg.LoggerDefinition]


class KedroProjectTranslator:
    """Translate Kedro project into Dagster code location.

    Args:
        project_path (Path | None): The path to the Kedro project.
        env (str | None): Kedro environment to use.
        conf_source (str | None): Path to the Kedro configuration source directory.
    """

    def __init__(
        self,
        project_path: Path | None = None,
        env: str | None = None,
        conf_source: str | None = None,
    ) -> None:
        self._project_path: Path
        if project_path is None:
            self._project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
        else:
            self._project_path = project_path

        if env is None:
            default_run_env = settings._CONFIG_LOADER_ARGS["default_run_env"]
            env = os.getenv("KEDRO_ENV", default_run_env) or ""

        self._env: str = env

        self.initialize_kedro(conf_source=conf_source)

    def initialize_kedro(self, conf_source: str | None = None) -> None:
        """Initialize Kedro context and pipelines for translation.

        Args:
            conf_source (str | None): Optional configuration source directory.
        """
        LOGGER.info("Initializing Kedro project...")

        LOGGER.info("Bootstrapping Kedro project at path: %s", self._project_path)
        self._project_metadata = bootstrap_project(self._project_path)
        LOGGER.info("Project name: %s", self._project_metadata.project_name)

        LOGGER.info("Creating Kedro session...")
        self._session = KedroSession.create(
            project_path=self._project_path,
            env=self._env,
            conf_source=conf_source,
        )

        self._session_id = self._session.session_id
        LOGGER.info("Session created with ID: %s", self._session_id)

        LOGGER.info("Loading Kedro context...")
        self._context = self._session.load_context()

        self._pipelines = find_pipelines()

        LOGGER.info("Kedro initialization complete.")

    # TODO: Allow translating a subset of the project?
    # TODO: Allow to pass params that overwrite the dagster config
    def to_dagster(self) -> DagsterCodeLocation:
        """Translate the Kedro project into a Dagster code location.

        Returns:
            DagsterCodeLocation: The translated Dagster code location.
        """
        LOGGER.info("Translating Kedro project into Dagster...")

        LOGGER.info("Loading Dagster configuration...")
        dagster_config = get_dagster_config(self._context)

        LOGGER.info("Creating Dagster run resources...")
        kedro_run_translator = KedroRunTranslator(
            context=self._context,
            project_path=str(self._project_path),
            env=self._env,
            session_id=self._session_id,
        )
        kedro_run_resource = kedro_run_translator.to_dagster(
            pipeline_name="__default__",
            filter_params={},
            load_versions=None,
            extra_params=None,
        )
        named_resources: dict[str, dg.ResourceDefinition] = {"kedro_run": kedro_run_resource}

        if is_mlflow_enabled():
            # Add MLflow resource if enabled in the Kedro context
            named_resources["mlflow"] = get_mlflow_resource_from_config(self._context.mlflow)

        LOGGER.info("Mapping Dagster loggers...")
        self.logger_creator = LoggerTranslator(
            dagster_config=dagster_config, package_name=self._project_metadata.package_name, pipelines=self._pipelines
        )
        named_loggers = self.logger_creator.to_dagster()

        LOGGER.info("Translating Kedro catalog to Dagster IO managers...")
        self.catalog_translator = CatalogTranslator(
            catalog=self._context.catalog,
            hook_manager=self._context._hook_manager,
            env=self._env,
        )
        named_io_managers = self.catalog_translator.to_dagster()
        named_resources |= named_io_managers

        LOGGER.info("Translating Kedro nodes to Dagster ops and assets...")
        self.node_translator = NodeTranslator(
            pipelines=self._pipelines,
            catalog=self._context.catalog,
            hook_manager=self._context._hook_manager,
            session_id=self._session_id,
            named_resources=named_resources,
            env=self._env,
        )
        named_ops, named_assets = self.node_translator.to_dagster()

        LOGGER.info("Creating Dagster executors...")
        self.executor_creator = ExecutorCreator(dagster_config=dagster_config)
        named_executors = self.executor_creator.create_executors()

        LOGGER.info("Translating Kedro pipelines to Dagster jobs...")
        self.pipeline_translator = PipelineTranslator(
            dagster_config=dagster_config,
            context=self._context,
            project_path=str(self._project_path),
            env=self._env,
            session_id=self._session_id,
            named_assets=named_assets,
            named_ops=named_ops,
            named_resources=named_resources,
            named_executors=named_executors,
        )
        named_jobs = self.pipeline_translator.to_dagster()

        LOGGER.info("Creating Dagster schedules...")
        self.schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
        named_schedules = self.schedule_creator.create_schedules()

        LOGGER.info("Creating Dagster run sensors...")
        named_sensors = kedro_run_translator._translate_on_pipeline_error_hook(named_jobs=named_jobs)

        LOGGER.info("Kedro project successfully translated into Dagster.")

        return DagsterCodeLocation(
            named_resources=named_resources,
            named_assets=named_assets,
            named_ops=named_ops,
            named_jobs=named_jobs,
            named_executors=named_executors,
            named_schedules=named_schedules,
            named_sensors=named_sensors,
            named_loggers=named_loggers,
        )
