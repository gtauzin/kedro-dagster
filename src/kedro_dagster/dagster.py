"""Dagster integration utilities: executors, schedules, and loggers.

This module provides small translators/creators that build Dagster artifacts
from the validated Kedro-Dagster configuration: executors, schedules and
pipeline-specific loggers.
"""

import logging
from typing import TYPE_CHECKING

import dagster as dg
from kedro.framework.project import pipelines

from kedro_dagster.config.execution import (
    CeleryDockerExecutorOptions,
    CeleryExecutorOptions,
    CeleryK8sJobExecutorOptions,
    DaskExecutorOptions,
    DockerExecutorOptions,
    InProcessExecutorOptions,
    K8sJobExecutorOptions,
    MultiprocessExecutorOptions,
)

if TYPE_CHECKING:
    from pydantic import BaseModel


class ExecutorCreator:
    """Create Dagster executor definitions from Kedro-Dagster configuration.

    Args:
        dagster_config (BaseModel): Parsed Kedro-Dagster config containing executor entries.
    """

    _OPTION_EXECUTOR_MAP = {
        InProcessExecutorOptions: dg.in_process_executor,
        MultiprocessExecutorOptions: dg.multiprocess_executor,
    }

    _EXECUTOR_CONFIGS = [
        (CeleryExecutorOptions, "dagster_celery", "celery_executor"),
        (CeleryDockerExecutorOptions, "dagster_celery_docker", "celery_docker_executor"),
        (CeleryK8sJobExecutorOptions, "dagster_celery_k8s", "celery_k8s_job_executor"),
        (DaskExecutorOptions, "dagster_dask", "dask_executor"),
        (DockerExecutorOptions, "dagster_docker", "docker_executor"),
        (K8sJobExecutorOptions, "dagster_k8s", "k8s_job_executor"),
    ]

    def __init__(self, dagster_config: "BaseModel"):
        self._dagster_config = dagster_config

    def register_executor(self, executor_option: "BaseModel", executor: dg.ExecutorDefinition) -> None:
        """Register a mapping between an options model and a Dagster executor factory.

        Args:
            executor_option (BaseModel): Pydantic model type acting as the key.
            executor (ExecutorDefinition): Dagster executor factory to use for that key.
        """
        self._OPTION_EXECUTOR_MAP[executor_option] = executor

    def create_executors(self) -> dict[str, dg.ExecutorDefinition]:
        """Instantiate executor definitions declared in the configuration.

        Returns:
            dict[str, dg.ExecutorDefinition]: Mapping of executor name to configured executor.
        """
        # Register all available executors dynamically
        for executor_option, module_name, executor_name in self._EXECUTOR_CONFIGS:
            try:
                module = __import__(module_name, fromlist=[executor_name])
                executor = getattr(module, executor_name)
                self.register_executor(executor_option, executor)
            except ImportError:
                pass

        named_executors = {}
        for executor_name, executor_config in self._dagster_config.executors.items():
            # Make use of the executor map to create the executor
            executor = self._OPTION_EXECUTOR_MAP.get(type(executor_config), None)
            if executor is None:
                raise ValueError(
                    f"Executor {executor_name} not supported. "
                    "Please use one of the following executors: "
                    f"{', '.join([str(k) for k in self._OPTION_EXECUTOR_MAP.keys()])}"
                )
            executor = executor.configured(executor_config.model_dump())
            named_executors[executor_name] = executor

        return named_executors


class ScheduleCreator:
    """Create Dagster schedule definitions from Kedro configuration."""

    def __init__(self, dagster_config: "BaseModel", named_jobs: dict[str, dg.JobDefinition]):
        self._dagster_config = dagster_config
        self._named_jobs = named_jobs

    def create_schedules(self) -> dict[str, dg.ScheduleDefinition]:
        """Create schedule definitions from the configuration.

        Returns:
            dict[str, dg.ScheduleDefinition]: Dict of schedule definitions keyed by job name.

        """
        named_schedule_config = {}
        if self._dagster_config.schedules is not None:
            for schedule_name, schedule_config in self._dagster_config.schedules.items():
                named_schedule_config[schedule_name] = schedule_config.model_dump()

        named_schedules = {}
        for job_name, job_config in self._dagster_config.jobs.items():
            schedule_config = job_config.schedule
            if isinstance(schedule_config, str):
                schedule_name = schedule_config
                if schedule_name in named_schedule_config:
                    schedule = dg.ScheduleDefinition(
                        name=f"{job_name}_{schedule_name}_schedule",
                        job=self._named_jobs[job_name],
                        **named_schedule_config[schedule_name],
                    )
                else:
                    raise ValueError(
                        f"Schedule defined by {schedule_config} not found. "
                        "Please make sure the schedule is defined in the configuration."
                    )

                named_schedules[job_name] = schedule

        return named_schedules


class LoggerTranslator:
    """Translate Kedro pipeline loggers to Dagster loggers."""

    def __init__(self, dagster_config: "BaseModel", package_name: str):
        self._dagster_config = dagster_config
        self._package_name = package_name

    @staticmethod
    def _get_logger_definition(package_name: str, pipeline_name: str) -> dg.LoggerDefinition:
        def pipeline_logger(context: dg.InitLoggerContext) -> logging.Logger:
            logger = logging.getLogger(f"{package_name}.pipelines.{pipeline_name}.nodes")
            return logger

        return dg.LoggerDefinition(
            pipeline_logger,
            description=f"Logger for pipeline`{pipeline_name}`.",
        )

    def to_dagster(self) -> dict[str, dg.LoggerDefinition]:
        """Translate Kedro loggers to Dagster loggers.

        Returns:
            dict[str, LoggerDefinition]: Mapping of fully-qualified logger name to definition.
        """
        named_loggers = {}
        for pipeline_name in pipelines:
            if pipeline_name != "__default__":
                named_loggers[f"{self._package_name}.pipelines.{pipeline_name}.nodes"] = self._get_logger_definition(
                    self._package_name, pipeline_name
                )

        return named_loggers
