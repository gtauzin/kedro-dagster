# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.translator import KedroProjectTranslator


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_project_translates_all_jobs(env, request):
    """Verify that the spaceflights quickstart scenario produces all documented jobs."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # Verify both documented jobs exist
    assert "default" in location.named_jobs
    assert "parallel_data_processing" in location.named_jobs

    # All should be valid JobDefinitions
    for job_name in ["default", "parallel_data_processing"]:
        assert isinstance(location.named_jobs[job_name], dg.JobDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_schedules_created(env, request):
    """Verify that scheduled jobs produce corresponding Dagster schedules."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # parallel_data_processing has schedule: daily
    assert "parallel_data_processing" in location.named_schedules
    assert isinstance(location.named_schedules["parallel_data_processing"], dg.ScheduleDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_executors_created(env, request):
    """Verify that both sequential and multiprocess executors are available."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # Verify both executor types are available
    assert "sequential" in location.named_executors
    assert "multiprocess" in location.named_executors

    assert isinstance(location.named_executors["sequential"], dg.ExecutorDefinition)
    assert isinstance(location.named_executors["multiprocess"], dg.ExecutorDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_assets_from_pipelines(env, request):
    """Verify that assets are created for datasets and nodes in both pipelines."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    asset_keys = set(location.named_assets.keys())

    # External inputs (raw data) - these are source assets
    assert "companies" in asset_keys
    assert "shuttles" in asset_keys
    assert "reviews" in asset_keys

    # Node-produced assets from data_processing pipeline
    assert "preprocess_companies_node" in asset_keys
    assert "preprocess_shuttles_node" in asset_keys
    assert "create_model_input_table_node" in asset_keys

    # Data science nodes that produce outputs also become assets
    assert "split_data_node" in asset_keys
    assert "train_model_node" in asset_keys
    # Note: evaluate_model_node has no outputs (returns None), so may not be a named asset


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_parallel_data_processing_job_nodes(env, request):
    """Verify parallel_data_processing job only contains the specified nodes."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # Get the parallel_data_processing job
    job = location.named_jobs["parallel_data_processing"]

    # The job graph should contain the filtered nodes plus hook ops
    # (before_pipeline_run, after_pipeline_run)
    node_names = {node.name for node in job.graph.nodes}

    # Should contain the two specified nodes from the docs
    assert "preprocess_companies_node" in node_names
    assert "preprocess_shuttles_node" in node_names

    # Should NOT contain the full pipeline nodes
    assert "create_model_input_table_node" not in node_names


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_data_science_job_includes_all_ds_nodes(env, request):
    """Verify default job contains all data science pipeline nodes (via __default__)."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # The default job uses __default__ which includes both pipelines
    job = location.named_jobs["default"]
    node_names = {node.name for node in job.graph.nodes}

    # Should contain all data science nodes (via __default__ = data_processing + data_science)
    assert "split_data_node" in node_names
    assert "train_model_node" in node_names
    assert "evaluate_model_node" in node_names


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_loggers_created(env, request):
    """Verify that custom loggers are available."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # Verify console_logger is available
    assert "console_logger" in location.named_loggers
    assert isinstance(location.named_loggers["console_logger"], dg.LoggerDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_error_sensor_exists(env, request):
    """Verify that the on_pipeline_error sensor is created for hook preservation."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # The sensor for on_pipeline_error hook should exist
    assert "on_pipeline_error_sensor" in location.named_sensors


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_resources_created(env, request):
    """Verify that resources are created including kedro_run and IO managers."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    # The kedro_run resource should always be present
    assert "kedro_run" in location.named_resources

    # Resources should not be empty
    assert len(location.named_resources) >= 1


@pytest.mark.parametrize("env", ["base", "local"])
def test_spaceflights_quickstart_default_job_uses_full_pipeline(env, request):
    """Verify default job uses __default__ pipeline which combines both pipelines."""
    options = request.getfixturevalue(f"kedro_project_spaceflights_quickstart_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    session.load_context()

    translator = KedroProjectTranslator(project_path=project_path, env=env)
    location = translator.to_dagster()

    job = location.named_jobs["default"]
    node_names = {node.name for node in job.graph.nodes}

    # Should contain nodes from BOTH pipelines since __default__ = data_processing + data_science
    # Data processing nodes
    assert "preprocess_companies_node" in node_names
    assert "preprocess_shuttles_node" in node_names
    assert "create_model_input_table_node" in node_names

    # Data science nodes
    assert "split_data_node" in node_names
    assert "train_model_node" in node_names
    assert "evaluate_model_node" in node_names
