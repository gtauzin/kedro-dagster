"""Configuration definitions for `kedro dagster dev`."""

from pathlib import Path
from typing import Literal

from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project
from pydantic import BaseModel


class DevOptions(BaseModel):
    """Development configuration options for the `kedro dagster dev` command.

    Attributes:
        log_level (Literal["critical", "error", "warning", "info", "debug"]): Logging level.
        log_format (Literal["colored", "json", "rich"]): Format for log output.
        port (str): Port for the dev server.
        host (str): Host for the dev server.
        live_data_poll_rate (str): Poll rate for live data updates in milliseconds.
    """

    log_level: Literal["critical", "error", "warning", "info", "debug"] = "info"
    log_format: Literal["colored", "json", "rich"] = "colored"
    port: str = "3000"
    host: str = "127.0.0.1"
    live_data_poll_rate: str = "2000"

    @property
    def python_file(self) -> Path:
        project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
        project_metadata = bootstrap_project(project_path)
        package_name = project_metadata.package_name
        definitions_py = "definitions.py"
        definitions_py_path = project_path / "src" / package_name / definitions_py

        return definitions_py_path  # type: ignore[no-any-return]

    class Config:
        extra = "forbid"
