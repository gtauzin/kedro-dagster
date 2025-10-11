"""Translation from Kedro to Dagster."""

import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg
from kedro.framework.project import find_pipelines, pipelines, settings
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
from kedro_dagster.utils import get_filter_params_dict, get_mlflow_resource_from_config, is_mlflow_enabled

if TYPE_CHECKING:
    from kedro.pipeline import Pipeline
    from pydantic import BaseModel

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
            # TODO: Double check if this is the right way to get the default environment
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

    def get_defined_pipelines(self, dagster_config: "BaseModel", translate_all: bool) -> list["Pipeline"]:
        """Get pipelines to translate.

        Args:
            dagster_config (dict[str, Any]): The configuration of the Dagster job.
            translate_all (bool): Whether to translate the whole Kedro project.
            If ``False``, translates only the pipelines defined in `dagster.yml`.

        Returns:
            list[Pipeline]: List of Kedro pipelines to translate.
        """
        if translate_all:
            return list(find_pipelines().values())

        defined_pipelines = []
        for job_config in dagster_config.jobs.values():
            pipeline_config = job_config.pipeline.model_dump()

            pipeline_name = pipeline_config.get("pipeline_name", "__default__")
            filter_params = get_filter_params_dict(pipeline_config)
            pipeline = pipelines.get(pipeline_name).filter(**filter_params)
            defined_pipelines.append(pipeline)

        return defined_pipelines

    def to_dagster(self, translate_all: bool = False) -> DagsterCodeLocation:
        """Translate Kedro project into Dagster.

        Args:
            translate_all (bool): Whether to translate the whole Kedro project.
            If ``False``, translates only the pipelines defined in `dagster.yml`.

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
        )
        named_resources: dict[str, dg.ResourceDefinition] = {"kedro_run": kedro_run_resource}

        if is_mlflow_enabled():
            # Add MLflow resource if enabled in the Kedro context
            named_resources["mlflow"] = get_mlflow_resource_from_config(self._context.mlflow)

        LOGGER.info("Mapping Dagster loggers...")
        self.logger_creator = LoggerTranslator(
            dagster_config=dagster_config, package_name=self._project_metadata.package_name
        )
        named_loggers = self.logger_creator.to_dagster()

        LOGGER.info("Translating Kedro catalog to Dagster IO managers...")
        defined_pipelines = self.get_defined_pipelines(dagster_config=dagster_config, translate_all=translate_all)
        self.catalog_translator = CatalogTranslator(
            catalog=self._context.catalog,
            pipelines=defined_pipelines,
            hook_manager=self._context._hook_manager,
            env=self._env,
        )
        named_io_managers, asset_partitions = self.catalog_translator.to_dagster()
        named_resources |= named_io_managers

        LOGGER.info("Translating Kedro nodes to Dagster ops and assets...")
        self.node_translator = NodeTranslator(
            pipelines=defined_pipelines,
            catalog=self._context.catalog,
            hook_manager=self._context._hook_manager,
            session_id=self._session_id,
            asset_partitions=asset_partitions,
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
            asset_partitions=asset_partitions,
            named_ops=named_ops,
            named_resources=named_resources,
            named_executors=named_executors,
            enable_mlflow=is_mlflow_enabled(),
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
