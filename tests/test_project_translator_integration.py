# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.translator import KedroProjectTranslator


@pytest.mark.parametrize("env", ["base", "local"])
def test_kedro_project_translator_end_to_end(env, request):
    """End-to-end project translation yields jobs, schedules, sensors, assets, and resources."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path

    # Initialize Kedro and run the full translator like definitions.py would
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()  # ensures pipelines resolved and config loaded

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # Jobs
    assert "default" in location.named_jobs
    assert isinstance(location.named_jobs["default"], dg.JobDefinition)

    # Schedules
    assert "default" in location.named_schedules
    assert isinstance(location.named_schedules["default"], dg.ScheduleDefinition)

    # Sensors
    assert "on_pipeline_error_sensor" in location.named_sensors

    # Assets: expect external input_dataset and node-produced assets
    asset_keys = set(location.named_assets.keys())
    # external dataset "input_ds" + asset for each pipeline node (node0..node4)
    expected = {"input_ds", "node0", "node1", "node2", "node3", "node4"}
    assert expected.issubset(asset_keys)

    # Resources include dataset-specific IO manager for file-backed output2
    expected_io_manager_key = f"{env}__output2_ds_io_manager"
    assert expected_io_manager_key in location.named_resources


def test_translator_uses_cwd_when_find_kedro_project_returns_none(monkeypatch, tmp_path):
    """When find_kedro_project returns None, the translator should fall back to Path.cwd()."""
    # Ensure current working directory is a temporary folder for isolation
    monkeypatch.chdir(tmp_path)

    # Patch the project discovery to return None
    monkeypatch.setattr("kedro_dagster.translator.find_kedro_project", lambda cwd: None)

    # Patch initialize_kedro to a no-op to avoid heavy Kedro bootstrapping
    monkeypatch.setattr(
        "kedro_dagster.translator.KedroProjectTranslator.initialize_kedro",
        lambda self, conf_source=None: None,
    )

    # Provide a minimal settings object with dict-like _CONFIG_LOADER_ARGS to avoid import-time errors
    monkeypatch.setattr(
        "kedro_dagster.translator.settings",
        SimpleNamespace(_CONFIG_LOADER_ARGS={"default_run_env": ""}),
    )

    translator = KedroProjectTranslator(project_path=None)

    assert translator._project_path == Path.cwd()
