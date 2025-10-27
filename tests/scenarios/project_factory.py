from __future__ import annotations

import importlib
import os
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from click.testing import CliRunner
from kedro.framework.cli.starters import create_cli as kedro_cli

ALL_PLUGINS = ["kedro-mlflow"]


@dataclass
class KedroProjectOptions:
    """Options to build a fake Kedro project scenario for testing.

    Attributes:
        env: Kedro environment to write configs to (e.g., "base", "local").
        catalog: Python dict for catalog.yml content to write under conf/<env>/.
        dagster: Optional Python dict for dagster.yml content to write under conf/<env>/.
        parameters: Optional dict of parameters to write (filename without extension supported via parameters_filename).
        parameters_filename: Optional name for parameters file (default: parameters.yml).
        plugins: Optional list of Kedro plugins/packages to preinstall in the isolated uv-run
                 environment used to create the test project (e.g., ["kedro-mlflow"]).
    """

    env: str = "base"
    catalog: dict[str, Any] = field(default_factory=dict)
    dagster: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = None
    parameters_filename: str = "parameters.yml"
    pipeline_registry_py: str | None = None
    plugins: list[str] = field(default_factory=list)


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            data,
            f,
            sort_keys=True,
            allow_unicode=True,
            default_flow_style=False,
            indent=2,
        )


def build_kedro_project_scenario(
    temp_directory: Path,
    options: KedroProjectOptions,
    project_name: str,
) -> tuple[Path, KedroProjectOptions]:
    """Create a fresh Kedro project in an isolated env and inject scenario-specific configs.

    The project is created via `uv run` to ensure a clean environment that only includes
    Kedro (and optional specified plugins), avoiding any locally installed Kedro plugins
    from the developer machine. After creation, env-specific conf files are written.

    Args:
        temp_directory: Temporary base directory under which the project variant will be created.
        options: Variant options including env, catalog, dagster config, and parameters.
        project_name: Name for the new project variant directory.

    Returns:
        tuple[Path, KedroProjectOptions]: The path to the new project variant and the
        options used to build it (including env and scenario-specific configs).
    """
    # Create the Kedro project in an isolated, fresh environment using `uv run`
    # so that local Kedro plugins installed on the developer machine are not picked up.
    # We pin Kedro to the version range declared in this project (see pyproject.toml).
    os.chdir(temp_directory)

    package_name = project_name.replace("-", "_")

    project_path: Path = Path(temp_directory.join(project_name))
    if project_path.exists():
        # Remove existing directory to ensure a fresh project creation
        shutil.rmtree(project_path)

    cli_runner = CliRunner()
    cli_runner.invoke(
        kedro_cli,
        [
            "new",
            "-v",
            "--name",
            project_name,
            "--tools",
            "none",
            "--example",
            "no",
        ],
    )

    if "kedro-mlflow" in options.plugins:
        # Ensure a local MLflow file store exists for projects created in tests
        # As kedro-mlflow is installed it will raise if it's missing.
        mlruns_dir = project_path / "mlruns"
        mlruns_dir.mkdir(parents=True, exist_ok=True)
        # MLflow's FileStore also expects a ".trash" subdirectory to exist when querying
        # deleted experiments; ensure it is present to avoid errors during setup/search.
        (mlruns_dir / ".trash").mkdir(parents=True, exist_ok=True)
    else:
        # Edit settings.py to add DISABLE_HOOKS_FOR_PLUGINS to avoid errors if plugin
        # is not installed but the project template includes it in settings.py hooks.
        settings_file = project_path / "src" / package_name / "settings.py"
        settings_text = settings_file.read_text(encoding="utf-8")
        settings_text += "\n\nDISABLE_HOOKS_FOR_PLUGINS = ('kedro_mlflow', )\n"
        settings_file.write_text(settings_text, encoding="utf-8")

    # Inject configuration files
    conf_env_dir = project_path / "conf" / options.env
    conf_env_dir.mkdir(parents=True, exist_ok=True)

    if options.catalog:
        _write_yaml(conf_env_dir / "catalog.yml", options.catalog)

    if options.dagster:
        _write_yaml(conf_env_dir / "dagster.yml", options.dagster)

    if options.parameters is not None:
        # Allow parameters_* files by passing name like "parameters_data_processing.yml"
        filename = options.parameters_filename or "parameters.yml"
        _write_yaml(conf_env_dir / filename, options.parameters)

    # Ensure settings.py contains dagster patterns for config loader
    src_dir = project_path / "src"
    package_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if not package_dirs:
        return project_path

    if package_dirs:
        package_name = package_dirs[0].name
        if options.pipeline_registry_py is not None:
            pipeline_registry_file = package_dirs[0] / "pipeline_registry.py"
            pipeline_registry_file.write_text(options.pipeline_registry_py, encoding="utf-8")

        # Clear cached modules so updates to the project's pipeline registry are picked up
        sys.modules.pop("kedro.framework.project", None)
        sys.modules.pop(f"{package_name}.pipeline_registry", None)
        # Also clear any imported submodules under `<package>.pipelines` to avoid stale definitions
        for modname in list(sys.modules.keys()):
            if modname == f"{package_name}.pipelines" or modname.startswith(f"{package_name}.pipelines."):
                sys.modules.pop(modname, None)

    # Re-import and configure the Kedro project using its Python package name
    configure_project = importlib.import_module("kedro.framework.project").configure_project
    configure_project(package_name)

    return project_path, options
