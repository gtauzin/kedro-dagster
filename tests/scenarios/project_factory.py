from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class KedroProjectOptions:
    """Options to build a fake Kedro project variant for testing.

    Attributes:
        env: Kedro environment to write configs to (e.g., "base", "local").
        catalog: Python dict for catalog.yml content to write under conf/<env>/.
        dagster: Optional Python dict for dagster.yml content to write under conf/<env>/.
        parameters: Optional dict of parameters to write (filename without extension supported via parameters_filename).
        parameters_filename: Optional name for parameters file (default: parameters.yml).
    """

    env: str = "base"
    catalog: dict[str, Any] = field(default_factory=dict)
    dagster: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = None
    parameters_filename: str = "parameters.yml"
    # Optional: override the project's pipeline registry source for this variant
    pipeline_registry_py: str | None = None


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # JSON is a valid subset of YAML; writing JSON keeps dependencies minimal for tests
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def build_kedro_project_variant(
    base_project: Path,
    destination_root: Path,
    options: KedroProjectOptions,
) -> Path:
    """Create a new Kedro project variant by copying a base template and injecting configs.

    We avoid re-running `kedro new` for speed by cloning the session-scoped base project
    created in tests and customizing conf files for each scenario.

    Args:
        base_project: Path to an existing Kedro project to use as template.
        destination_root: Temporary root directory where the variant will be created.
        options: Variant options including env, catalog, dagster config, and parameters.

    Returns:
        Path: The path to the new project variant.
    """
    assert base_project.exists(), f"Base project not found: {base_project}"
    destination_root.mkdir(parents=True, exist_ok=True)

    variant_dir = destination_root / f"{base_project.name}_variant_{options.env}"
    if variant_dir.exists():
        shutil.rmtree(variant_dir)
    shutil.copytree(base_project, variant_dir)

    # Inject configuration files
    conf_env_dir = variant_dir / "conf" / options.env
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
    src_dir = variant_dir / "src"
    package_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if not package_dirs:
        return variant_dir
    settings_py = package_dirs[0] / "settings.py"
    if settings_py.exists():
        # Basic enforcement: ensure dagster patterns present
        content = settings_py.read_text(encoding="utf-8")
        if '"dagster"' not in content:
            content += (
                "\nCONFIG_LOADER_ARGS = {\n"
                '    "base_env": "base",\n'
                '    "default_run_env": "local",\n'
                '    "config_patterns": {\n'
                '        "dagster": ["dagster*", "dagster/**"],\n'
                "    }\n"
                "}\n"
            )
            settings_py.write_text(content, encoding="utf-8")

    # Optionally override pipeline registry for this variant
    if options.pipeline_registry_py is not None and package_dirs:
        pipeline_registry_file = package_dirs[0] / "pipeline_registry.py"
        pipeline_registry_file.write_text(options.pipeline_registry_py, encoding="utf-8")

    return variant_dir
