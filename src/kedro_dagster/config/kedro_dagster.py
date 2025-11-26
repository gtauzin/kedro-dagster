"""Configuration definitions for Kedro-Dagster.

This module parses and validates the top-level `dagster.yml` entries, merging
sub-models for executors, schedules, and jobs configs into a single
KedroDagsterConfig object.
"""

from logging import getLogger
from typing import TYPE_CHECKING, Any

from kedro.config import MissingConfigException
from pydantic import BaseModel, model_validator

from kedro_dagster.utils import PYDANTIC_VERSION, create_pydantic_config

from .automation import ScheduleOptions
from .execution import EXECUTOR_MAP, ExecutorOptions
from .job import JobOptions
from .logging import LoggerOptions

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext

LOGGER = getLogger(__name__)


class KedroDagsterConfig(BaseModel):
    """Main configuration class representing the `dagster.yml` structure.

    Attributes:
        loggers (dict[str, LoggerOptions] | None): Mapping of logger names to logger options.
        executors (dict[str, ExecutorOptions] | None): Mapping of executor names to executor options.
        schedules (dict[str, ScheduleOptions] | None): Mapping of schedule names to schedule options.
        jobs (dict[str, JobOptions] | None): Mapping of job names to job options.
    """

    loggers: dict[str, LoggerOptions] | None = None
    executors: dict[str, ExecutorOptions] | None = None
    schedules: dict[str, ScheduleOptions] | None = None
    jobs: dict[str, JobOptions] | None = None

    # Version-aware Pydantic configuration
    if PYDANTIC_VERSION[0] >= 2:  # noqa: PLR2004
        model_config = create_pydantic_config(validate_assignment=True, extra="forbid")
    else:  # pragma: no cover
        Config = create_pydantic_config(validate_assignment=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def validate_executors(cls, values: dict[str, Any]) -> dict[str, Any]:
        executors = values.get("executors", {})

        parsed_executors = {}
        for name, executor_config in executors.items():
            if "in_process" in executor_config:
                executor_name = "in_process"
            elif "multiprocess" in executor_config:
                executor_name = "multiprocess"
            elif "k8s_job_executor" in executor_config:
                executor_name = "k8s_job_executor"
            elif "docker_executor" in executor_config:
                executor_name = "docker_executor"
            else:
                msg = f"Unknown executor type in {name}"
                LOGGER.error(msg)
                raise ValueError(msg)

            executor_options_class = EXECUTOR_MAP[executor_name]
            executor_options_params = executor_config[executor_name] or {}
            parsed_executors[name] = executor_options_class(**executor_options_params)

        values["executors"] = parsed_executors
        return values


def get_dagster_config(context: "KedroContext") -> KedroDagsterConfig:
    """Get the Dagster configuration from the `dagster.yml` file.

    Args:
    context: ``KedroContext`` that was created.

    Returns:
        KedroDagsterConfig: Dagster configuration.
    """
    LOGGER.info("Loading Dagster configuration...")

    try:
        if "dagster" not in context.config_loader.config_patterns.keys():
            context.config_loader.config_patterns.update({"dagster": ["dagster*", "dagster*/**", "**/dagster*"]})
        conf_dagster_yml = context.config_loader["dagster"]
    except MissingConfigException:
        LOGGER.warning(
            "No 'dagster.yml' config file found in environment. Default configuration will be used. "
            "Use ``kedro dagster init`` command in CLI to customize the configuration."
        )

        conf_dagster_yml = {}

    dagster_config = KedroDagsterConfig(**conf_dagster_yml)

    return dagster_config
