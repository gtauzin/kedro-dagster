"""Dagster integration utilities: executors, schedules, and loggers.

This module provides small translators/creators that build Dagster artifacts
from the validated Kedro-Dagster configuration: executors, schedules and
loggers.
"""

import importlib
import logging
import sys
from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg

from kedro_dagster.config.execution import (
    CeleryDockerExecutorOptions,
    CeleryExecutorOptions,
    CeleryK8sJobExecutorOptions,
    DaskExecutorOptions,
    DockerExecutorOptions,
    ExecutorOptions,
    InProcessExecutorOptions,
    K8sJobExecutorOptions,
    MultiprocessExecutorOptions,
)

if TYPE_CHECKING:
    from kedro_dagster.config.kedro_dagster import KedroDagsterConfig

LOGGER = getLogger(__name__)


class ExecutorCreator:
    """Create Dagster executor definitions from Kedro-Dagster configuration.

    Args:
        dagster_config (KedroDagsterConfig): Parsed Kedro-Dagster config containing executor entries.
    """

    _OPTION_EXECUTOR_MAP = {
        InProcessExecutorOptions: dg.in_process_executor,
        MultiprocessExecutorOptions: dg.multiprocess_executor,
    }

    _EXECUTOR_CONFIGS: list[tuple[type[ExecutorOptions], str, str]] = [
        (CeleryExecutorOptions, "dagster_celery", "celery_executor"),
        (CeleryDockerExecutorOptions, "dagster_celery_docker", "celery_docker_executor"),
        (CeleryK8sJobExecutorOptions, "dagster_celery_k8s", "celery_k8s_job_executor"),
        (DaskExecutorOptions, "dagster_dask", "dask_executor"),
        (DockerExecutorOptions, "dagster_docker", "docker_executor"),
        (K8sJobExecutorOptions, "dagster_k8s", "k8s_job_executor"),
    ]

    def __init__(self, dagster_config: "KedroDagsterConfig"):
        self._dagster_config = dagster_config

    def register_executor(self, executor_option: type[ExecutorOptions], executor: dg.ExecutorDefinition) -> None:
        """Register a mapping between an options model and a Dagster executor factory.

        Args:
            executor_option (type[ExecutorOptions]): Pydantic model type acting as the key.
            executor (ExecutorDefinition): Dagster executor factory to use for that key.
        """
        self._OPTION_EXECUTOR_MAP[executor_option] = executor

    def create_executors(self) -> dict[str, dg.ExecutorDefinition]:
        """Instantiate executor definitions declared in the configuration.

        Returns:
            dict[str, dg.ExecutorDefinition]: Mapping of executor name to configured executor.
        """
        LOGGER.info("Creating Dagster executors...")
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
                LOGGER.debug(f"Creating executor '{executor_name}'...")
                # Make use of the executor map to create the executor
                executor = self._OPTION_EXECUTOR_MAP.get(type(executor_config), None)
                if executor is None:
                    msg = (
                        f"Executor '{executor_name}' not supported. "
                        f"Please use one of the following executors: "
                        f"{', '.join([str(k) for k in self._OPTION_EXECUTOR_MAP.keys()])}"
                    )
                    LOGGER.error(msg)
                    raise ValueError(msg)
                executor = executor.configured(executor_config.model_dump())
                named_executors[executor_name] = executor

        # Next, iterate over jobs to handle inline executor configurations
        if self._dagster_config.jobs is not None:
            available_executor_names = set(named_executors.keys())

            for job_name, job_config in self._dagster_config.jobs.items():
                if job_config.executor is not None:
                    LOGGER.debug(f"Processing executor configuration for job '{job_name}'...")
                    if isinstance(job_config.executor, str):
                        # String reference - validate it exists in available executors
                        if job_config.executor not in available_executor_names:
                            msg = (
                                f"Executor named '{job_config.executor}' for job '{job_name}' not found in available executors. "
                                f"Available executors: {sorted(available_executor_names)}"
                            )
                            LOGGER.error(msg)
                            raise ValueError(msg)
                    else:
                        # Inline executor configuration - create executor definition
                        executor = self._OPTION_EXECUTOR_MAP.get(type(job_config.executor), None)
                        if executor is None:
                            msg = (
                                f"Executor type `{type(job_config.executor)}` for job '{job_name}' not supported. "
                                f"Please use one of the following executor types: "
                                f"{', '.join([str(k) for k in self._OPTION_EXECUTOR_MAP.keys()])}"
                            )
                            LOGGER.error(msg)
                            raise ValueError(msg)

                        # Create the executor with job-specific naming
                        executor_name = f"{job_name}__executor"
                        executor_def = executor.configured(job_config.executor.model_dump())
                        named_executors[executor_name] = executor_def

        LOGGER.debug(f"Created {len(named_executors)} executor(s)")
        return named_executors


class ScheduleCreator:
    """Create Dagster schedule definitions from Kedro configuration.

    Args:
        dagster_config (KedroDagsterConfig): Parsed Kedro-Dagster config containing schedule entries.
        named_jobs (dict[str, dg.JobDefinition]): Mapping of job names to Dagster job definitions.
    """

    def __init__(self, dagster_config: "KedroDagsterConfig", named_jobs: dict[str, dg.JobDefinition]):
        self._dagster_config = dagster_config
        self._named_jobs = named_jobs

    def create_schedules(self) -> dict[str, dg.ScheduleDefinition]:
        """Create schedule definitions from the configuration.

        Returns:
            dict[str, dg.ScheduleDefinition]: Dict of schedule definitions keyed by job name.

        """
        LOGGER.info("Creating Dagster schedules...")
        named_schedule_config = {}
        if self._dagster_config.schedules is not None:
            for schedule_name, schedule_config in self._dagster_config.schedules.items():
                LOGGER.debug(f"Registering schedule '{schedule_name}'...")
                named_schedule_config[schedule_name] = schedule_config.model_dump()

        available_schedule_names = set(named_schedule_config.keys())

        named_schedules = {}
        if self._dagster_config.jobs is not None:
            for job_name, job_config in self._dagster_config.jobs.items():
                if job_config.schedule is not None:
                    LOGGER.debug(f"Creating schedule for job '{job_name}'...")
                    if isinstance(job_config.schedule, str):
                        schedule_name = job_config.schedule
                        if schedule_name in named_schedule_config:
                            schedule = dg.ScheduleDefinition(
                                name=f"{job_name}_{schedule_name}_schedule",
                                job=self._named_jobs[job_name],
                                **named_schedule_config[schedule_name],
                            )
                        else:
                            msg = (
                                f"Schedule named '{schedule_name}' for job '{job_name}' not found. "
                                f"Available schedules: {sorted(available_schedule_names)}"
                            )
                            LOGGER.error(msg)
                            raise ValueError(msg)
                    else:
                        # If schedule_config is not a string, create schedule definition using inline config
                        schedule = dg.ScheduleDefinition(
                            name=f"{job_name}__schedule",
                            job=self._named_jobs[job_name],
                            **job_config.schedule.model_dump(),
                        )

                    named_schedules[job_name] = schedule

        LOGGER.debug(f"Created {len(named_schedules)} schedule(s)")
        return named_schedules


class LoggerCreator:
    """Create Dagster logger definitions from Kedro-Dagster configuration.

    Args:
        dagster_config (KedroDagsterConfig): Parsed Kedro-Dagster config containing logger entries.
    """

    def __init__(self, dagster_config: "KedroDagsterConfig"):
        self._dagster_config = dagster_config

    def _get_logger_definition(self, logger_name: str) -> dg.LoggerDefinition:
        """Create a Dagster logger definition from the configuration.

        Args:
            logger_name (str): Name of the logger.

        Returns:
            dg.LoggerDefinition: Dagster logger definition.
        """

        def _resolve_reference(ref: Any) -> Any:
            """Resolve a string with "module.ClassName", "module:function_name", or "ext://module.attr"."""
            if isinstance(ref, str):
                # Handle ext:// protocol used in logging configurations
                if ref.startswith("ext://"):
                    ref = ref[6:]  # Remove "ext://" prefix

                module_path, _, attr = ref.rpartition(".")
                if module_path:
                    module = importlib.import_module(module_path)
                    return getattr(module, attr)

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
            default_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            formatter_registry: dict[str, logging.Formatter] = {}
            if config_data.get("formatters"):
                for fname, fcfg in config_data["formatters"].items():
                    if "()" in fcfg:
                        # Callable given to create formatter
                        formatter_callable = _resolve_reference(fcfg["()"])
                        # remove the special key for constructor params
                        init_kwargs = {k: v for k, v in fcfg.items() if k != "()"}
                        fmt_inst = formatter_callable(**init_kwargs)
                    elif "class" in fcfg:
                        # Assume class path given in "class" key
                        cls_path = fcfg.get("class")
                        formatter_cls = _resolve_reference(cls_path)
                        init_kwargs = {k: v for k, v in fcfg.items() if k != "class"}
                        fmt_inst = formatter_cls(**init_kwargs)
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
                        filter_cls = _resolve_reference(cls_path)
                        init_kwargs = {k: v for (k, v) in fcfg.items() if k != "class"}
                        filter_inst = filter_cls(**init_kwargs)
                    filter_registry[fname] = filter_inst

            # Build handlers
            if config_data.get("handlers"):
                for hcfg in config_data["handlers"]:
                    fmt_ref = hcfg.get("formatter", None)
                    filter_refs = hcfg.get("filters", [])
                    h_level = hcfg.get("level", level).upper()
                    # Resolve handler class
                    if "()" in hcfg:
                        handler_callable = _resolve_reference(hcfg["()"])
                        init_kwargs = {k: v for (k, v) in hcfg.items() if k != "()"}
                        # Resolve stream references like "ext://sys.stdout"
                        if "stream" in init_kwargs and isinstance(init_kwargs["stream"], str):
                            init_kwargs["stream"] = _resolve_reference(init_kwargs["stream"])
                        handler_inst = handler_callable(**init_kwargs)
                    else:
                        cls_path = hcfg.get("class", "logging.StreamHandler")
                        handler_cls = _resolve_reference(cls_path)
                        init_kwargs = {
                            k: v for (k, v) in hcfg.items() if k not in ("class", "level", "formatter", "filters")
                        }
                        # Resolve stream references like "ext://sys.stdout"
                        if "stream" in init_kwargs and isinstance(init_kwargs["stream"], str):
                            init_kwargs["stream"] = _resolve_reference(init_kwargs["stream"])
                        handler_inst = handler_cls(**init_kwargs)

                    # Set handler level
                    handler_inst.setLevel(h_level)

                    # Attach formatter
                    if fmt_ref is not None and formatter_registry.get(fmt_ref):
                        handler_inst.setFormatter(formatter_registry[fmt_ref])
                    elif fmt_ref is None:
                        # No formatter specified for this handler, attach default
                        handler_inst.setFormatter(default_formatter)

                    # Attach filters
                    for fref in filter_refs:
                        if fref in filter_registry:
                            handler_inst.addFilter(filter_registry[fref])

                    logger_.addHandler(handler_inst)
            else:
                # No handlers specified, default to StreamHandler
                sh = logging.StreamHandler(stream=sys.stdout)
                sh.setLevel(level)
                sh.setFormatter(default_formatter)
                logger_.addHandler(sh)

            return logger_

        config_schema = {
            "log_level": dg.Field(str, default_value="INFO"),
            "handlers": dg.Field(list, is_required=False),
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
        LOGGER.info("Creating Dagster loggers...")
        named_loggers = {}
        if self._dagster_config.loggers:
            for logger_name in self._dagster_config.loggers.keys():
                LOGGER.debug(f"Creating logger '{logger_name}'...")
                logger = self._get_logger_definition(logger_name)
                named_loggers[logger_name] = logger

        # Iterate over jobs to handle job-specific logger configurations
        if hasattr(self._dagster_config, "jobs") and self._dagster_config.jobs:
            available_logger_names = list(named_loggers.keys())
            for job_name, job_config in self._dagster_config.jobs.items():
                if hasattr(job_config, "loggers") and job_config.loggers:
                    LOGGER.debug(f"Processing {len(job_config.loggers)} loggers for job '{job_name}'...")
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
                            logger = self._get_logger_definition(job_logger_name)
                            named_loggers[job_logger_name] = logger

        LOGGER.debug(f"Created {len(named_loggers)} logger(s)")
        return named_loggers
