"""Dagster executor creation from config."""

import logging

import dagster as dg
from kedro.framework.project import pipelines
from pydantic import BaseModel

from kedro_dagster.config.execution import (
    InProcessExecutorOptions,
    K8sJobExecutorOptions,
    MultiprocessExecutorOptions,
)


class ExecutorCreator:
    """Creates Dagster executor definitions from Kedro configuration."""

    # TODO: Map all dagster executors
    _OPTION_EXECUTOR_MAP = {
        InProcessExecutorOptions: dg.in_process_executor,
        MultiprocessExecutorOptions: dg.multiprocess_executor,
    }

    def __init__(self, dagster_config: BaseModel):
        self._dagster_config = dagster_config

    def register_executor(self, executor_option: BaseModel, executor: dg.ExecutorDefinition) -> None:
        """Register an executor option with a Dagster executor.

        Args:
            executor_option (BaseModel): The executor option to register.
            executor (ExecutorDefinition): The executor to register the option with.

        """

        self._OPTION_EXECUTOR_MAP[executor_option] = executor

    def create_executors(self) -> dict[str, dg.ExecutorDefinition]:
        """Create executor definitions from the configuration.

        Returns:
            Dict[str, ExecutorDefinition]: A dict of executor definitions.

        """
        try:
            from dagster_k8s import k8s_job_executor

            self.register_executor(K8sJobExecutorOptions, k8s_job_executor)
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
    """Creates Dagster schedule definitions from Kedro configuration."""

    def __init__(self, dagster_config: BaseModel, named_jobs: dict[str, dg.JobDefinition]):
        self._dagster_config = dagster_config
        self._named_jobs = named_jobs

    def create_schedules(self) -> dict[str, dg.ScheduleDefinition]:
        """Create schedule definitions from the configuration.

        Returns:
            Dict[str, ScheduleDefinition]: A dict of schedule definitions.

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


# TODO: Allow logger customization
class LoggerTranslator:
    """Translates Kedro loggers to Dagster loggers."""

    def __init__(self, dagster_config: BaseModel, project_metadata: BaseModel):
        self._dagster_config = dagster_config
        self._package_name = project_metadata.package_name

    def translate_loggers(self):
        """Translate Kedro loggers to Dagster loggers."""
        named_loggers = {}
        for pipeline_name in pipelines:
            if pipeline_name != "__default__":

                def get_logger_definition(package_name, pipeline_name):
                    def pipeline_logger(context: dg.InitLoggerContext):
                        return logging.getLogger(f"{package_name}.pipelines.{pipeline_name}.nodes")

                    return dg.LoggerDefinition(
                        pipeline_logger,
                        description=f"Logger for pipeline`{pipeline_name}` of package `{package_name}`.",
                    )

                named_loggers[f"{self._package_name}.pipelines.{pipeline_name}.nodes"] = get_logger_definition(
                    self._package_name, pipeline_name
                )

        return named_loggers
