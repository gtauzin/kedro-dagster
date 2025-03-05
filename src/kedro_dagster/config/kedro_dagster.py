"""Configuration definitions for Kedro-Dagster."""

from logging import getLogger
from typing import Any

from kedro.config import MissingConfigException
from kedro.framework.context import KedroContext
from pydantic import BaseModel, model_validator

from .automation import ScheduleOptions
from .dev import DevOptions
from .execution import EXECUTOR_MAP, ExecutorOptions
from .job import JobOptions

LOGGER = getLogger(__name__)


class KedroDagsterConfig(BaseModel):
    """Configuration for Kedro-Dagster."""

    dev: DevOptions | None = None
    executors: dict[str, ExecutorOptions] | None = None
    schedules: dict[str, ScheduleOptions] | None = None
    jobs: dict[str, JobOptions] | None = None

    class Config:
        validate_assignment = True
        extra = "forbid"

    @model_validator(mode="before")
    @classmethod
    def validate_dev(cls, values: dict[str, Any]) -> dict[str, Any]:
        dev = values.get("dev", DevOptions())
        values["dev"] = dev
        return values

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
                raise ValueError(f"Unknown executor type in {name}")

            executor_options_class = EXECUTOR_MAP[executor_name]
            executor_options_params = executor_config[executor_name] or {}
            parsed_executors[name] = executor_options_class(**executor_options_params)

        values["executors"] = parsed_executors
        return values


def get_dagster_config(context: KedroContext) -> KedroDagsterConfig:
    """Get the Dagster configuration from the `dagster.yml` file.

    Args:
        context: The ``KedroContext`` that was created.

    Returns:
        KedroDagsterConfig: The Dagster configuration.
    """
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
