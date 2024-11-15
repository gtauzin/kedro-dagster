from collections.abc import Iterable
from pathlib import Path
from typing import Literal, Optional

from dagster import get_dagster_logger
from kedro.config import MissingConfigException
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project
from pydantic import BaseModel

LOGGER = get_dagster_logger()


class DevOptions(BaseModel):
    log_level: Literal["critical", "error", "warning", "info", "debug"] = "info"
    log_format: Literal["colored", "json", "rich"] = "colored"
    port: str = "3000"
    host: str = "127.0.0.1"
    live_data_poll_rate: str = "2000"

    @property
    def python_file(self):
        project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
        project_metadata = bootstrap_project(project_path)
        package_name = project_metadata.package_name
        definitions_py = "definitions.py"
        definitions_py_path = project_path / "src" / package_name / definitions_py

        return definitions_py_path

    class Config:
        extra = "forbid"


class JobsOptions(BaseModel):
    pipeline: Optional[str] = None
    from_nodes: Iterable[str] | None = None
    to_nodes: Iterable[str] | None = None
    node_names: Iterable[str] | None = None
    from_inputs: Iterable[str] | None = None
    to_outputs: Iterable[str] | None = None
    namespace: str | None = None
    tags: Iterable[str] | None = None

    class Config:
        extra = "forbid"


class KedroDagsterConfig(BaseModel):
    dev: Optional[DevOptions] = None
    jobs: Optional[dict[str, JobsOptions]] = None

    class Config:
        # force triggering type control when setting value instead of init
        validate_assignment = True
        # raise an error if an unknown key is passed to the constructor
        extra = "forbid"


def get_dagster_config(context):
    try:
        if "dagster" not in context.config_loader.config_patterns.keys():
            context.config_loader.config_patterns.update({"dagster": ["dagster*", "dagster*/**", "**/dagster*"]})
        conf_dagster_yml = context.config_loader["dagster"]
    except MissingConfigException:
        LOGGER.warning(
            "No 'dagster.yml' config file found in environment. Default configuration will be used. Use ``kedro dagster init`` command in CLI to customize the configuration."
        )
        # we create an empty dict to have the same behaviour when the dagster.yml
        # is commented out. In this situation there is no MissingConfigException
        # but we got an empty dict
        conf_dagster_yml = {}

    dagster_config = KedroDagsterConfig.model_validate({**conf_dagster_yml})

    # store in context for interactive use
    # we use __setattr__ instead of context.dagster because
    # the class will become frozen in kedro>=0.19
    context.__setattr__("dagster", dagster_config)

    return dagster_config
