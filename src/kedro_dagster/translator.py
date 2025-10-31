"""High-level translation from a Kedro project into a Dagster code location.

This module orchestrates the end-to-end conversion from a Kedro project
into a Dagster code location composed of assets, jobs, resources, schedules,
sensors, and loggers. It bootstraps the Kedro session, loads configuration,
translates the catalog and nodes, and wires the resulting definitions together.
"""

import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg
from kedro.framework.project import find_pipelines, pipelines, settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

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
from kedro_dagster.utils import (
    find_kedro_project,
    get_filter_params_dict,
    get_mlflow_resource_from_config,
    is_mlflow_enabled,
)

if TYPE_CHECKING:
    from kedro.pipeline import Pipeline
    from pydantic import BaseModel

LOGGER = getLogger(__name__)


@dataclass
class DagsterCodeLocation:
    """A typed container for all artifacts that make up a Dagster code location.

    Attributes:
        named_assets: Dictionary of named Dagster assets.
        named_resources: Dictionary of named Dagster resources.
        named_jobs: Dictionary of named Dagster jobs.
        named_executors: Dictionary of named Dagster executors.
        named_schedules: Dictionary of named Dagster schedules.
        named_sensors: Dictionary of named Dagster sensors.
        named_loggers: Dictionary of named Dagster loggers.
    """

    named_assets: dict[str, dg.AssetSpec | dg.AssetsDefinition]
    named_resources: dict[str, dg.ResourceDefinition]
    named_jobs: dict[str, dg.JobDefinition]
    named_executors: dict[str, dg.ExecutorDefinition]
    named_schedules: dict[str, dg.ScheduleDefinition]
    named_sensors: dict[str, dg.SensorDefinition]
    named_loggers: dict[str, dg.LoggerDefinition]


class KedroProjectTranslator:
    """Translate a Kedro project into a Dagster code location.

    Args:
        project_path (Path | None): Path to the Kedro project. If omitted, auto-discovered.
        env (str | None): Kedro environment to use. Defaults to Kedro's configured default.
        conf_source (str | None): Optional path to the Kedro configuration source directory.
    """

    def __init__(
        self,
        project_path: Path | None = None,
        env: str | None = None,
        conf_source: str | None = None,
    ) -> None:
        self._project_path: Path
        if project_path is None:
            self._project_path = find_kedro_project(Path.cwd()) or Path.cwd()
        else:
            self._project_path = project_path

        if env is None:
            default_run_env = settings._CONFIG_LOADER_ARGS["default_run_env"]
            env = os.getenv("KEDRO_ENV", default_run_env) or ""

        self._env: str = env

        self.initialize_kedro(conf_source=conf_source)

    def initialize_kedro(self, conf_source: str | None = None) -> None:
        """Initialize Kedro context and pipelines for translation.

        This bootstraps the Kedro project, starts a session, loads the context,
        and discovers pipelines to prepare for translation.

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
        """Resolve the list of pipelines to translate.

        Args:
            dagster_config (BaseModel): Validated configuration for Kedro-Dagster.
            translate_all (bool): If True, translate all discovered pipelines; otherwise,
                only those referenced by jobs in the configuration.

        Returns:
            list[Pipeline]: Kedro pipelines to translate.
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
        """Translate the Kedro project into a Dagster code location.

        Args:
            translate_all (bool): Whether to translate all pipelines or only those referenced
                in the ``dagster.yml`` configuration.

        Returns:
            DagsterCodeLocation: Translated Dagster code location composed of assets, jobs, resources, and more.
        """
        LOGGER.info("Translating Kedro project into Dagster...")

        LOGGER.info("Loading Dagster configuration...")
        dagster_config = get_dagster_config(self._context)

        LOGGER.info("Creating Dagster run resources...")
        kedro_run_translator = KedroRunTranslator(
            context=self._context,
            project_path=str(self._project_path),
            env=self._env,
            run_id=self._session_id,
        )
        kedro_run_resource = kedro_run_translator.to_dagster(
            pipeline_name="__default__",
            filter_params={},
        )
        named_resources: dict[str, dg.ResourceDefinition] = {"kedro_run": kedro_run_resource}

        if is_mlflow_enabled():
            # Add MLflow resource only if MLflow is installed and configured on the context
            ctx_mlflow = getattr(self._context, "mlflow", None)
            if ctx_mlflow is not None:
                named_resources["mlflow"] = get_mlflow_resource_from_config(ctx_mlflow)
            else:
                LOGGER.info("MLflow is installed but not configured on the Kedro context; skipping MLflow resource.")

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
            run_id=self._session_id,
            asset_partitions=asset_partitions,
            named_resources=named_resources,
            env=self._env,
        )
        named_op_factories, named_assets = self.node_translator.to_dagster()
        LOGGER.info("Creating Dagster executors...")
        self.executor_creator = ExecutorCreator(dagster_config=dagster_config)
        named_executors = self.executor_creator.create_executors()

        LOGGER.info("Translating Kedro pipelines to Dagster jobs...")
        self.pipeline_translator = PipelineTranslator(
            dagster_config=dagster_config,
            context=self._context,
            project_path=str(self._project_path),
            env=self._env,
            run_id=self._session_id,
            named_assets=named_assets,
            asset_partitions=asset_partitions,
            named_op_factories=named_op_factories,
            named_resources=named_resources,
            named_executors=named_executors,
            enable_mlflow=is_mlflow_enabled() and hasattr(self._context, "mlflow"),
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
            named_jobs=named_jobs,
            named_executors=named_executors,
            named_schedules=named_schedules,
            named_sensors=named_sensors,
            named_loggers=named_loggers,
        )
