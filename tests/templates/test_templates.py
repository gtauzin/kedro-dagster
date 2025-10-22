from __future__ import annotations

import ast
from importlib.resources import files

import yaml


def _read_text_from_package(relative_path: str) -> str:
    # Resolve path relative to the installed package root to avoid relying on
    # the `templates` directory being a Python package.
    pkg_root = files("kedro_dagster")
    path = pkg_root.joinpath(*relative_path.split("/"))
    assert path.is_file(), f"Missing template file: {relative_path}"
    return path.read_text(encoding="utf-8")


def test_dagster_yml_template_is_valid_and_has_expected_structure():
    content = _read_text_from_package("templates/dagster.yml")

    # Parse YAML
    data = yaml.safe_load(content)
    assert isinstance(data, dict), "dagster.yml should load to a mapping"

    # Inspired by kedro-mlflow's template test: assert full expected structure
    expected = {
        "dev": {
            "log_level": "info",
            "log_format": "colored",
            "port": "3000",
            "host": "127.0.0.1",
            "live_data_poll_rate": "2000",
        },
        "schedules": {
            "daily": {
                "cron_schedule": "0 0 * * *",
            }
        },
        "executors": {
            "sequential": {
                "in_process": None,
            }
        },
        "jobs": {
            "default": {
                "pipeline": {"pipeline_name": "__default__"},
                "schedule": "daily",
                "executor": "sequential",
            }
        },
    }
    assert data == expected, "dagster.yml deviates from the expected default structure"

    # Top-level keys
    for key in ("dev", "schedules", "executors", "jobs"):
        assert key in data, f"Missing top-level key `{key}` in dagster.yml"

    # Dev section sanity
    dev = data["dev"]
    assert isinstance(dev, dict)
    for k in ("log_level", "log_format", "port", "host", "live_data_poll_rate"):
        assert k in dev

    # Schedules section sanity
    schedules = data["schedules"]
    assert isinstance(schedules, dict)
    assert "daily" in schedules
    assert "cron_schedule" in schedules["daily"], "`daily` schedule must define `cron_schedule`"

    # Executors section sanity
    executors = data["executors"]
    assert isinstance(executors, dict)
    assert "sequential" in executors
    assert "in_process" in executors["sequential"], "`sequential` must map to `in_process` parameters"

    # Jobs section sanity
    jobs = data["jobs"]
    assert isinstance(jobs, dict)
    assert "default" in jobs
    default_job = jobs["default"]
    assert default_job.get("schedule") == "daily"
    assert default_job.get("executor") == "sequential"

    pipeline = default_job.get("pipeline")
    assert isinstance(pipeline, dict)
    assert pipeline.get("pipeline_name") == "__default__"


def test_definitions_template_is_valid_python_and_contains_expected_constructs():
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
