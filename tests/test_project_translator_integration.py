# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.translator import KedroProjectTranslator


@pytest.mark.parametrize(
    "env_fixture",
    [
        "kedro_project_exec_filebacked_base",
        "kedro_project_exec_filebacked_local",
    ],
)
def test_kedro_project_translator_end_to_end(request, env_fixture):
    project_path, env = request.getfixturevalue(env_fixture)

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
