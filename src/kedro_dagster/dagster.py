"""Dagster integration utilities: executors, schedules, and loggers.

This module provides small translators/creators that build Dagster artifacts
from the validated Kedro-Dagster configuration: executors, schedules and
loggers.
"""

import importlib
import logging
import sys
from typing import TYPE_CHECKING, Any

import dagster as dg

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

        # First, create executors from the global executors configuration
        if self._dagster_config.executors is not None:
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

        # Next, iterate over jobs to handle inline executor configurations
        if self._dagster_config.jobs is not None:
            available_executor_names = set(named_executors.keys())

            for job_name, job_config in self._dagster_config.jobs.items():
                executor_config = job_config.executor
                if executor_config is not None:
                    if isinstance(executor_config, str):
                        # String reference - validate it exists in available executors
                        if executor_config not in available_executor_names:
                            raise ValueError(
                                f"Executor reference '{executor_config}' for job '{job_name}' not found in available executors. "
                                f"Available executors: {sorted(available_executor_names)}"
                            )
                    else:
                        # Inline executor configuration - create executor definition
                        executor = self._OPTION_EXECUTOR_MAP.get(type(executor_config), None)
                        if executor is None:
                            raise ValueError(
                                f"Executor type {type(executor_config)} for job '{job_name}' not supported. "
                                "Please use one of the following executor types: "
                                f"{', '.join([str(k) for k in self._OPTION_EXECUTOR_MAP.keys()])}"
                            )

                        # Create the executor with job-specific naming
                        executor_name = f"{job_name}__executor"
                        executor_def = executor.configured(executor_config.model_dump())
                        named_executors[executor_name] = executor_def

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
            if schedule_config is not None:
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
                else:
                    # If schedule_config is not a string, create schedule definition using inline config
                    schedule = dg.ScheduleDefinition(
                        name=f"{job_name}__schedule",
                        job=self._named_jobs[job_name],
                        **schedule_config.model_dump(),
                    )

                named_schedules[job_name] = schedule

        return named_schedules


class LoggerCreator:
    """Create Dagster logger definitions from Kedro-Dagster configuration.

    Args:
        dagster_config (BaseModel): Parsed Kedro-Dagster config containing logger entries.
    """

    def __init__(self, dagster_config: "BaseModel"):
        self._dagster_config = dagster_config

    def _get_logger_definition(self, logger_name: str, logger_config: dict[str, Any]) -> dg.LoggerDefinition:
        """Create a Dagster logger definition from the configuration.

        Args:
            logger_name (str): Name of the logger.
            logger_config (dict[str, Any]): Logger configuration dictionary.

        Returns:
            dg.LoggerDefinition: Dagster logger definition.
        """

        def _resolve_reference(ref: Any) -> Any:
            """Resolve a string with "module.ClassName" or "module:function_name"."""
            if isinstance(ref, str):
                module_path, _, attr = ref.rpartition(".")
                if module_path:
                    module = importlib.import_module(module_path)
                    return getattr(module, attr)
                else:
                    # no module specified, assume built‑in or current namespace
                    return globals()[attr]
            raise TypeError(f"Unable to resolve reference {ref!r}")

        def dagster_logger(context: dg.InitLoggerContext) -> logging.Logger:
            # Use the provided config directly instead of dynamic schema
            config_data = dict(context.logger_config)
            level = config_data.get("log_level", "INFO").upper()

            klass = logging.getLoggerClass()
            logger_ = klass(logger_name, level=level)

            # Optionally clear existing handlers to prevent duplicates
            for h in list(logger_.handlers):
                logger_.removeHandler(h)

            # Build formatter registry
            formatter_registry: dict[str, logging.Formatter] = {}
            if config_data.get("formatters"):
                for fname, fcfg in config_data["formatters"].items():
                    if "()" in fcfg:
                        # Callable given to create formatter
                        formatter_callable = _resolve_reference(fcfg["()"])
                        # remove the special key for constructor params
                        init_kwargs = {k: v for k, v in fcfg.items() if k != "()"}
                        fmt_inst = formatter_callable(**init_kwargs)
                    else:
                        # Use standard logging.Formatter
                        fmt_str = fcfg.get("format", None)
                        datefmt = fcfg.get("datefmt", None)
                        style = fcfg.get("style", "%")
                        fmt_inst = logging.Formatter(fmt_str, datefmt=datefmt, style=style)
                    formatter_registry[fname] = fmt_inst

            # Build filter registry
            filter_registry: dict[str, logging.Filter] = {}
            if config_data.get("filters"):
                for fname, fcfg in config_data["filters"].items():
                    if "()" in fcfg:
                        filter_callable = _resolve_reference(fcfg["()"])
                        init_kwargs = {k: v for (k, v) in fcfg.items() if k != "()"}
                        filter_inst = filter_callable(**init_kwargs)
                    else:
                        # assume class path in "class" key
                        cls_path = fcfg.get("class")
                        params = fcfg.get("params", {})
                        filter_cls = _resolve_reference(cls_path)
                        filter_inst = filter_cls(**params)
                    filter_registry[fname] = filter_inst

            # Build handlers
            if config_data.get("handlers"):
                for hcfg in config_data["handlers"]:
                    # Resolve handler class
                    if "()" in hcfg:
                        handler_callable = _resolve_reference(hcfg["()"])
                        init_kwargs = {k: v for (k, v) in hcfg.items() if k != "()"}
                        handler_inst = handler_callable(**init_kwargs)
                    else:
                        cls_path = hcfg.get("class", "logging.StreamHandler")
                        handler_cls = _resolve_reference(cls_path)
                        args = hcfg.get("args", []) or []
                        kwargs = hcfg.get("kwargs", {}) or {}
                        handler_inst = handler_cls(*args, **kwargs)

                    # Set handler level
                    h_level = hcfg.get("level")
                    if h_level:
                        handler_inst.setLevel(h_level.upper())

                    # Attach formatter
                    fmt_ref = hcfg.get("formatter")
                    if fmt_ref and formatter_registry.get(fmt_ref):
                        handler_inst.setFormatter(formatter_registry[fmt_ref])
                    elif config_data.get("formatters") is None:
                        # No formatter registry, attach default
                        handler_inst.setFormatter(
                            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
                        )

                    # Attach filters
                    filter_refs = hcfg.get("filters") or []
                    for fref in filter_refs:
                        if fref in filter_registry:
                            handler_inst.addFilter(filter_registry[fref])

                    logger_.addHandler(handler_inst)
            else:
                # No handlers specified, default to StreamHandler
                sh = logging.StreamHandler(stream=sys.stdout)
                sh.setLevel(level)
                sh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
                logger_.addHandler(sh)

            return logger_

        config_schema = {
            "log_level": dg.Field(str, default_value="INFO"),
            "handlers": dg.Field(dict, is_required=False),
            "formatters": dg.Field(dict, is_required=False),
            "filters": dg.Field(dict, is_required=False),
        }

        return dg.LoggerDefinition(
            dagster_logger,
            description=f"Logger definition `{logger_name}`.",
            config_schema=config_schema,
        )

    def create_loggers(self) -> dict[str, dg.LoggerDefinition]:
        """Create logger definitions from the configuration.

        Returns:
            dict[str, LoggerDefinition]: Mapping of fully-qualified logger name to definition.
        """
        named_loggers = {}
        if self._dagster_config.loggers:
            for logger_name, logger_config in self._dagster_config.loggers.items():
                logger = self._get_logger_definition(logger_name, logger_config.model_dump())
                named_loggers[logger_name] = logger

        # Iterate over jobs to handle job-specific logger configurations
        if hasattr(self._dagster_config, "jobs") and self._dagster_config.jobs:
            available_logger_names = list(named_loggers.keys())
            for job_name, job_config in self._dagster_config.jobs.items():
                if hasattr(job_config, "loggers") and job_config.loggers:
                    for idx, logger_config in enumerate(job_config.loggers):
                        if isinstance(logger_config, str):
                            # If logger_config is a string, check it exists in available_logger_names
                            if logger_config not in available_logger_names:
                                raise ValueError(
                                    f"Logger '{logger_config}' referenced in job '{job_name}' "
                                    f"not found in available loggers: {available_logger_names}"
                                )
                        else:
                            # If logger_config is not a string, create logger definition named after the job
                            job_logger_name = f"{job_name}__logger_{idx}"
                            logger = self._get_logger_definition(job_logger_name, logger_config.model_dump())
                            named_loggers[job_logger_name] = logger

        return named_loggers
