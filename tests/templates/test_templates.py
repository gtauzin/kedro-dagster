from __future__ import annotations

import ast
from importlib.resources import files

import yaml

from kedro_dagster.config.kedro_dagster import KedroDagsterConfig
from kedro_dagster.utils import render_jinja_template


def _read_text_from_package(relative_path: str) -> str:
    # Resolve path relative to the installed package root to avoid relying on
    # the `templates` directory being a Python package.
    pkg_root = files("kedro_dagster")
    path = pkg_root.joinpath(*relative_path.split("/"))
    assert path.is_file(), f"Missing template file: {relative_path}"
    return path.read_text(encoding="utf-8")


def test_definitions_template_is_valid_python_and_contains_expected_constructs():
    """definitions.py template parses and includes expected translator/definitions snippets."""
    code = _read_text_from_package("templates/definitions.py")

    # Check it parses as valid Python (without executing it)
    ast.parse(code)

    # Light content checks to ensure expected objects are present in the template
    expected_snippets = [
        "from kedro_dagster import KedroProjectTranslator",
        "translator = KedroProjectTranslator(",
        "translator.to_dagster()",
        "dg.Definitions(",
        "io_manager",
        "multiprocess_executor",
        "default_executor =",
    ]
    for snippet in expected_snippets:
        assert snippet in code, f"Template missing expected snippet: {snippet!r}"


def test_dg_toml_template_placeholders_and_renders(tmp_path):
    """dg.toml template includes expected placeholders and renders with provided variables."""
    # Read raw template text
    text = _read_text_from_package("templates/dg.toml")

    # Contains expected placeholders
    assert 'root_module = "{{ project_name }}"' in text
    assert 'code_location_target_module = "{{ project_name }}.definitions"' in text
    assert 'code_location_name = "{{ package_name }}"' in text
    assert 'defs_module = "{{ project_name }}"' in text

    # Render using utils to ensure Jinja processing is valid
    pkg_root = files("kedro_dagster")
    template_path = pkg_root.joinpath("templates", "dg.toml")
    rendered = render_jinja_template(str(template_path), project_name="my_pkg", package_name="My Project")

    # Check replacements happened
    assert 'root_module = "my_pkg"' in rendered
    assert 'code_location_target_module = "my_pkg.definitions"' in rendered
    assert 'code_location_name = "My Project"' in rendered
    # Ensure defs_module remains the constant used by the integration
    assert 'defs_module = "my_pkg"' in rendered


def test_dagster_yml_template_contains_file_logger_configuration():
    """dagster.yml template includes file_logger with RotatingFileHandler configuration."""
    text = _read_text_from_package("templates/dagster.yml")

    # Check for the new file_logger structure
    assert "file_logger:" in text
    assert "log_level: INFO" in text
    assert "formatters:" in text
    assert "simple:" in text
    assert 'format: "[%(asctime)s] %(levelname)s - %(message)s"' in text
    assert "handlers:" in text
    assert "class: logging.handlers.RotatingFileHandler" in text
    assert "filename: dagster_run_info.log" in text
    assert "maxBytes: 10485760" in text  # 10MB
    assert "backupCount: 20" in text
    assert "encoding: utf8" in text
    assert "delay: True" in text

    # Check that console logger is removed
    assert "console:" not in text or text.count("console:") == 0


def test_dagster_yml_template_default_job_uses_file_logger():
    """dagster.yml template default job configuration references file_logger."""
    text = _read_text_from_package("templates/dagster.yml")

    # Check that the default job includes the file_logger
    assert 'loggers: ["file_logger"]' in text

    # Verify the structure around the default job
    default_job_section = text[text.find("default:") :]
    assert "pipeline:" in default_job_section
    assert "pipeline_name: __default__" in default_job_section
    assert 'loggers: ["file_logger"]' in default_job_section
    assert "schedule: daily" in default_job_section
    assert "executor: sequential" in default_job_section


def test_dagster_yml_template_is_valid_yaml():
    """dagster.yml template is valid YAML that can be parsed."""
    # Constants for file handler configuration
    MAX_BYTES = 10485760  # 10MB
    BACKUP_COUNT = 20

    text = _read_text_from_package("templates/dagster.yml")

    # Should parse without errors
    config = yaml.safe_load(text)

    # Check the structure is as expected
    assert "loggers" in config
    assert "file_logger" in config["loggers"]
    assert "jobs" in config
    assert "default" in config["jobs"]
    assert "loggers" in config["jobs"]["default"]
    assert config["jobs"]["default"]["loggers"] == ["file_logger"]

    # Check file_logger configuration
    file_logger = config["loggers"]["file_logger"]
    assert file_logger["log_level"] == "INFO"
    assert "formatters" in file_logger
    assert "simple" in file_logger["formatters"]
    assert "handlers" in file_logger
    assert len(file_logger["handlers"]) == 1

    handler = file_logger["handlers"][0]
    assert handler["class"] == "logging.handlers.RotatingFileHandler"
    assert handler["filename"] == "dagster_run_info.log"
    assert handler["maxBytes"] == MAX_BYTES
    assert handler["backupCount"] == BACKUP_COUNT


def test_dagster_yml_template_parses_with_kedro_dagster_config():
    """dagster.yml template can be successfully parsed by KedroDagsterConfig."""
    text = _read_text_from_package("templates/dagster.yml")
    config_dict = yaml.safe_load(text)

    # Should parse without validation errors
    dagster_config = KedroDagsterConfig(**config_dict)

    # Verify the parsed structure
    assert "file_logger" in dagster_config.loggers
    assert dagster_config.loggers["file_logger"].log_level == "INFO"

    # Verify job configuration
    assert "default" in dagster_config.jobs
    default_job = dagster_config.jobs["default"]
    assert default_job.loggers == ["file_logger"]
    assert default_job.pipeline.pipeline_name == "__default__"
    assert default_job.schedule == "daily"
    assert default_job.executor == "sequential"
