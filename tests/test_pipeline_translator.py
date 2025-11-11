# mypy: ignore-errors

from __future__ import annotations

import logging

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.config.job import PipelineOptions
from kedro_dagster.config.logging import LoggerOptions
from kedro_dagster.dagster import ExecutorCreator, LoggerCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator


@pytest.mark.parametrize("env", ["base", "local"])
def test_pipeline_translator_to_dagster_with_executor(env, request):
    """Translate a Kedro pipeline to Dagster jobs with configured executors and resources."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Catalog -> IO managers and partition metadata
    default_pipeline = pipelines.get("__default__")
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Nodes -> op factories and assets
    node_translator = NodeTranslator(
        pipelines=[default_pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
        run_id=session.session_id,
    )
    # Obtain op factories and assets via the NodeTranslator API
    named_op_factories, named_assets = node_translator.to_dagster()

    # Executors from config
    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()
    assert "seq" in named_executors

    # Loggers from config
    logger_creator = LoggerCreator(dagster_config=dagster_config)
    named_loggers = logger_creator.create_loggers()

    # Build jobs
    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors=named_executors,
        named_loggers=named_loggers,
        enable_mlflow=False,
        run_id=session.session_id,
    )
    jobs = pipeline_translator.to_dagster()
    assert "default" in jobs
    assert isinstance(jobs["default"], dg.JobDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_after_pipeline_run_hook_inputs_fan_in_for_partitions(env, request):
    """Ensure the after-pipeline-run hook op declares a Nothing input per partition.

    We configure a partitioned path intermediate -> output2 with identity mapping,
    then build the job and introspect the hook op input names to confirm they include
    the per-partition fan-in inputs (e.g., node2__p1_after_pipeline_run_hook_input).
    """
    options = request.getfixturevalue(f"kedro_project_partitioned_intermediate_output2_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

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
        run_id=session.session_id,
    )
    named_op_factories, named_assets = node_translator.to_dagster()

    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()

    logger_creator = LoggerCreator(dagster_config=dagster_config)
    named_loggers = logger_creator.create_loggers()

    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors=named_executors,
        named_loggers=named_loggers,
        enable_mlflow=False,
        run_id=session.session_id,
    )
    jobs = pipeline_translator.to_dagster()
    job = jobs["default"]

    # Fetch the after-pipeline-run hook op definition from the job
    hook_name = "after_pipeline_run_hook_default"
    node_def = None
    # Try common Dagster APIs to retrieve node definitions
    if hasattr(job, "graph") and hasattr(job.graph, "node_defs"):
        for nd in job.graph.node_defs:
            if getattr(nd, "name", None) == hook_name:
                node_def = nd
                break
    if node_def is None and hasattr(job, "all_node_defs"):
        for nd in job.all_node_defs:
            if getattr(nd, "name", None) == hook_name:
                node_def = nd
                break

    assert node_def is not None, "Hook op definition not found in job"
    ins_keys = set(getattr(node_def, "ins").keys())
    # Expect a Nothing input per partition for the last node (node2)
    # The naming includes both upstream and downstream partition keys for clarity
    assert "node2__p1__p1_after_pipeline_run_hook_input" in ins_keys
    assert "node2__p2__p2_after_pipeline_run_hook_input" in ins_keys


@pytest.mark.parametrize(
    "env_fixture",
    [
        "kedro_project_exec_filebacked_base",
        "kedro_project_exec_filebacked_local",
        "kedro_project_partitioned_intermediate_output2_base",
        "kedro_project_partitioned_intermediate_output2_local",
        "kedro_project_partitioned_static_mapping_base",
        "kedro_project_partitioned_static_mapping_local",
        "kedro_project_multiple_inputs_base",
        "kedro_project_multiple_inputs_local",
        "kedro_project_multiple_outputs_tuple_base",
        "kedro_project_multiple_outputs_tuple_local",
        "kedro_project_multiple_outputs_dict_base",
        "kedro_project_multiple_outputs_dict_local",
        "kedro_project_no_outputs_node_base",
        "kedro_project_no_outputs_node_local",
        "kedro_project_nothing_assets_base",
        "kedro_project_nothing_assets_local",
    ],
)
def test_pipeline_translator_builds_jobs_for_scenarios(request, env_fixture):
    """Ensure PipelineTranslator can build a job across diverse scenarios without errors."""
    options = request.getfixturevalue(env_fixture)
    project_path = options.project_path
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    default_pipeline = pipelines.get("__default__")

    # Catalog -> IO managers and partition metadata
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Nodes -> op factories and assets via the NodeTranslator
    node_translator = NodeTranslator(
        pipelines=[default_pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
        run_id=session.session_id,
    )

    named_op_factories, named_assets = node_translator.to_dagster()

    # Executors from config
    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()

    # Loggers from config
    logger_creator = LoggerCreator(dagster_config=dagster_config)
    named_loggers = logger_creator.create_loggers()

    # Build jobs
    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors=named_executors,
        named_loggers=named_loggers,
        enable_mlflow=False,
        run_id=session.session_id,
    )
    jobs = pipeline_translator.to_dagster()
    assert "default" in jobs
    assert isinstance(jobs["default"], dg.JobDefinition)


def _patch_minimal_kedro_pipelines(monkeypatch):
    """Patch kedro.framework.project.pipelines.get to avoid real Kedro dependency."""

    class _DummyGetter:
        def __call__(self, _name: str):
            class _Pipe:
                def filter(self, **_kwargs):  # pragma: no cover - trivial shim
                    return object()

            return _Pipe()

    monkeypatch.setattr("kedro.framework.project.pipelines.get", _DummyGetter(), raising=False)


def _make_translator_with(monkeypatch, named_loggers=None, named_executors=None):
    """Create a PipelineTranslator with minimal viable context and a stub translate_pipeline.

    Returns a tuple (translator, captured) where captured is a dict populated by the stub
    translate_pipeline with the logger_defs and executor_def it received.
    """

    class _Ctx:
        catalog = object()
        _hook_manager = object()

    captured: dict[str, object] = {}

    def _fake_translate(self, *, pipeline, pipeline_name, filter_params, job_name, executor_def, logger_defs):
        captured["logger_defs"] = logger_defs
        captured["executor_def"] = executor_def
        # Return a harmless sentinel to satisfy to_dagster contract
        return f"job:{job_name}"

    # Ensure Kedro pipeline access is stubbed
    _patch_minimal_kedro_pipelines(monkeypatch)

    translator = PipelineTranslator(
        dagster_config=type("Cfg", (), {"jobs": {}})(),
        context=_Ctx(),
        project_path="/tmp/project",
        env="dev",
        run_id="rid",
        named_assets={},
        asset_partitions={},
        named_op_factories={},
        named_resources={},
        named_executors=({} if named_executors is None else named_executors),
        named_loggers=({} if named_loggers is None else named_loggers),
        enable_mlflow=False,
    )

    # Patch the instance method to capture logger_defs
    monkeypatch.setattr(PipelineTranslator, "translate_pipeline", _fake_translate, raising=False)
    return translator, captured


def test_pipeline_translator_logger_string_reference_found(monkeypatch):
    # Arrange: a named logger exists and job references it by string
    ld = dg.LoggerDefinition(logger_fn=lambda ctx: logging.getLogger("t"))
    translator, captured = _make_translator_with(monkeypatch, {"console": ld})

    # Inject a single job with a string logger reference
    translator._dagster_config.jobs = {
        "jobA": type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": ["console"],
                "executor": None,
            },
        )()
    }

    # Act
    named_jobs = translator.to_dagster()

    # Assert: translate_pipeline received the logger_defs with the named logger
    assert "jobA" in named_jobs
    assert captured.get("logger_defs") == {"console": ld}


def test_pipeline_translator_logger_string_reference_missing(monkeypatch):
    # Arrange: no named loggers; job references a missing name
    translator, _ = _make_translator_with(monkeypatch, {})
    translator._dagster_config.jobs = {
        "jobA": type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": ["missing"],
                "executor": None,
            },
        )()
    }

    # Act / Assert
    with pytest.raises(ValueError, match=r"Logger `missing` not found\."):
        translator.to_dagster()


def test_pipeline_translator_inline_logger_found(monkeypatch):
    # Arrange: a job has an inline logger (non-string), and a job-specific named logger exists
    job_name = "jobB"
    specific_name = f"{job_name}__logger_0"
    ld = dg.LoggerDefinition(logger_fn=lambda ctx: logging.getLogger("t"))
    translator, captured = _make_translator_with(monkeypatch, {specific_name: ld})

    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": [LoggerOptions(logger_name="inline.logger", log_level="INFO")],
                "executor": None,
            },
        )()
    }

    # Act
    translator.to_dagster()

    # Assert: job-specific logger name is looked up and passed through
    assert captured.get("logger_defs") == {specific_name: ld}


def test_pipeline_translator_inline_logger_missing(monkeypatch):
    # Arrange: a job has an inline logger (non-string), but the job-specific named logger is absent
    job_name = "jobC"
    translator, _ = _make_translator_with(monkeypatch, {})
    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": [LoggerOptions(logger_name="inline.logger", log_level="INFO")],
                "executor": None,
            },
        )()
    }

    # Act / Assert
    with pytest.raises(ValueError, match=rf"Job-specific logger `{job_name}__logger_0` not found\."):
        translator.to_dagster()


def test_pipeline_translator_executor_string_reference_found(monkeypatch):
    # Arrange: a named executor exists and job references it by string
    exec_def = object()
    translator, captured = _make_translator_with(monkeypatch, named_executors={"seq": exec_def})

    translator._dagster_config.jobs = {
        "jobA": type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "executor": "seq",
                "loggers": None,
            },
        )()
    }

    # Act
    named_jobs = translator.to_dagster()

    # Assert
    assert "jobA" in named_jobs
    assert captured.get("executor_def") is exec_def


def test_pipeline_translator_executor_string_reference_missing(monkeypatch):
    # Arrange: job references a missing executor by string
    translator, _ = _make_translator_with(monkeypatch)
    translator._dagster_config.jobs = {
        "jobA": type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "executor": "missing",
                "loggers": None,
            },
        )()
    }

    # Act / Assert
    with pytest.raises(ValueError, match=r"Executor `missing` not found\."):
        translator.to_dagster()


def test_pipeline_translator_executor_inline_found(monkeypatch):
    # Arrange: job uses inline executor (non-string), job-specific executor name exists in registry
    job_name = "jobB"
    job_exec_name = f"{job_name}__executor"
    exec_def = object()
    translator, captured = _make_translator_with(monkeypatch, named_executors={job_exec_name: exec_def})

    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "executor": object(),  # non-string triggers inline branch
                "loggers": None,
            },
        )()
    }

    # Act
    translator.to_dagster()

    # Assert
    assert captured.get("executor_def") is exec_def


def test_pipeline_translator_executor_inline_missing(monkeypatch):
    # Arrange: job uses inline executor but job-specific executor is not present
    job_name = "jobC"
    translator, _ = _make_translator_with(monkeypatch)

    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "executor": object(),
                "loggers": None,
            },
        )()
    }

    # Act / Assert
    with pytest.raises(ValueError, match=rf"Job-specific executor `{job_name}__executor` not found\."):
        translator.to_dagster()
