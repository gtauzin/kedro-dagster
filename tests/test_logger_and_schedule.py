# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import LoggerTranslator, ScheduleCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator


@pytest.mark.parametrize("env", ["base", "local"])
def test_logger_translator_builds_package_loggers(env, request):
    """Build Dagster LoggerDefinitions for package modules declared in config."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path

    metadata = bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)
    logger_translator = LoggerTranslator(dagster_config=dagster_config, package_name=metadata.package_name)
    named_loggers = logger_translator.to_dagster()

    # Expect a logger for the "pipe" pipeline (default registry in conftest)
    assert any(key.endswith(".pipelines.pipe.nodes") for key in named_loggers.keys())
    # Definitions should be Dagster LoggerDefinition
    assert all(isinstance(v, dg.LoggerDefinition) for v in named_loggers.values())


@pytest.mark.parametrize("env", ["base", "local"])
def test_schedule_creator_uses_named_schedule(env, request):
    """Create a named schedule for the 'default' job with the expected cron expression."""
    # Use the integration scenario which includes executors, schedules and a default job
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Minimal path to jobs to feed into ScheduleCreator: compose with PipelineTranslator
    default_pipeline = pipelines.get("__default__")
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()
    node_translator = NodeTranslator(
        pipelines=[default_pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
    )
    named_op_factories, named_assets = node_translator.to_dagster()
    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        session_id=session.session_id,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors={"seq": dg.in_process_executor},
        enable_mlflow=False,
    )
    named_jobs = pipeline_translator.to_dagster()

    schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
    named_schedules = schedule_creator.create_schedules()

    assert "default" in named_schedules
    schedule = named_schedules["default"]
    assert isinstance(schedule, dg.ScheduleDefinition)
    assert schedule.cron_schedule == "0 6 * * *"
