# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator, LoggerTranslator, ScheduleCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator
from kedro_dagster.utils import _kedro_version
from tests.scenarios.kedro_projects import pipeline_registry_default
from tests.scenarios.project_factory import KedroProjectOptions


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

    if _kedro_version()[0] >= 1:
        run_id_kwargs = {"run_id": session.session_id}
    else:
        run_id_kwargs = {"session_id": session.session_id}

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
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
        **run_id_kwargs,
    )
    named_op_factories, named_assets = node_translator.to_dagster()
    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors={"seq": dg.in_process_executor},
        enable_mlflow=False,
        **run_id_kwargs,
    )
    named_jobs = pipeline_translator.to_dagster()

    schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
    named_schedules = schedule_creator.create_schedules()

    assert "default" in named_schedules
    schedule = named_schedules["default"]
    assert isinstance(schedule, dg.ScheduleDefinition)
    assert schedule.cron_schedule == "0 6 * * *"


@pytest.mark.parametrize("env", ["base", "local"])
def test_executor_translator_creates_multiple_executors(env, request):
    """Build multiple executor definitions from config and validate their names/types."""
    # Arrange: project with multiple executors and a default job
    options = request.getfixturevalue(f"kedro_project_multi_executors_{env}")
    project_path = options.project_path

    # Act: parse dagster config and build executors
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    dagster_config = get_dagster_config(context)
    executors = ExecutorCreator(dagster_config=dagster_config).create_executors()

    # Assert: both executors are registered
    assert "seq" in executors
    assert "multiproc" in executors

    assert len(executors) == 2  # noqa: PLR2004

    assert isinstance(executors["seq"], dg.ExecutorDefinition)
    assert executors["seq"].name == "in_process"
    assert isinstance(executors["multiproc"], dg.ExecutorDefinition)
    assert executors["multiproc"].name == "multiprocess"


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_schedule_creator_raises_for_unknown_named_schedule(env, project_scenario_factory):
    """When a job references a non-existent named schedule, raise a clear ValueError."""
    # Build a Kedro project with a job referencing schedule "unknown" and no schedules defined
    dagster_cfg = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq", "schedule": "unknown"}},
    }
    opts = KedroProjectOptions(env=env, dagster=dagster_cfg, pipeline_registry_py=pipeline_registry_default())
    options = project_scenario_factory(opts, project_name="kedro-project-unknown-schedule")

    # Bootstrap and build Dagster objects
    bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Provide a minimal, valid Dagster job mapping to the ScheduleCreator without
    # relying on Kedro translators (we're testing schedule resolution only here).
    @dg.op
    def _noop():
        pass

    @dg.job
    def default():
        _noop()

    named_jobs = {"default": default}

    schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
    with pytest.raises(ValueError) as e:
        schedule_creator.create_schedules()

    assert "Schedule defined by unknown not found" in str(e.value)


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_executor_creator_unsupported_executor_raises(env, project_scenario_factory):
    """If an optional executor's package isn't installed, creating it should raise a ValueError."""
    # Use a k8s executor which relies on `dagster_k8s` being importable; assume it's not installed in test env.
    dagster_cfg = {
        "executors": {"k8s": {"k8s_job_executor": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "k8s"}},
    }
    options = project_scenario_factory(
        KedroProjectOptions(env=env, dagster=dagster_cfg, pipeline_registry_py=pipeline_registry_default()),
        project_name="kedro-project-k8s-executor",
    )

    bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    creator = ExecutorCreator(dagster_config=dagster_config)
    with pytest.raises(ValueError) as e:
        creator.create_executors()

    assert "not supported" in str(e.value)


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_logger_translator_exact_key_and_description(env, request):
    """LoggerTranslator should emit exact module key and description for non-default pipelines."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")

    metadata = bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    translator = LoggerTranslator(dagster_config=dagster_config, package_name=metadata.package_name)
    named_loggers = translator.to_dagster()

    expected_key = f"{metadata.package_name}.pipelines.pipe.nodes"
    assert expected_key in named_loggers.keys()
    assert all(".pipelines.__default__." not in k for k in named_loggers.keys())

    # Validate the description text for the emitted logger definition
    assert named_loggers[expected_key].description == "Logger for pipeline`pipe`."
