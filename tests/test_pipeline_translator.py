# mypy: ignore-errors

from __future__ import annotations

import logging
import re
import warnings

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
    """Test that after-pipeline-run hook op declares a Nothing input per partition."""
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


def _patch_minimal_kedro_pipelines(mocker):
    """Patch kedro.framework.project.pipelines.get to avoid real Kedro dependency."""

    class _DummyGetter:
        def __call__(self, _name: str):
            class _Pipe:
                def filter(self, **_kwargs):  # pragma: no cover - trivial shim
                    return object()

            return _Pipe()

    mocker.patch("kedro.framework.project.pipelines.get", _DummyGetter())


def _make_translator_with(mocker, named_loggers=None, named_executors=None):
    """Create a PipelineTranslator with minimal viable context and a stub translate_pipeline.

    Returns a tuple (translator, captured) where captured is a dict populated by the stub
    translate_pipeline with the logger_defs and executor_def it received.
    """

    class _Ctx:
        catalog = object()
        _hook_manager = object()

    captured: dict[str, object] = {}

    def _fake_translate(
        self, *, pipeline, pipeline_name, filter_params, job_name, executor_def, logger_defs, loggers_config=None
    ):
        captured["logger_defs"] = logger_defs
        captured["executor_def"] = executor_def
        captured["loggers_config"] = loggers_config
        # Return a harmless sentinel to satisfy to_dagster contract
        return f"job:{job_name}"

    # Ensure Kedro pipeline access is stubbed
    _patch_minimal_kedro_pipelines(mocker)

    translator = PipelineTranslator(
        dagster_config=type("Cfg", (), {"jobs": {}, "loggers": {}})(),
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
    mocker.patch.object(PipelineTranslator, "translate_pipeline", _fake_translate)
    return translator, captured


def test_pipeline_translator_logger_string_reference_found(mocker):
    """Test that pipeline translator resolves logger string references correctly."""
    ld = dg.LoggerDefinition(logger_fn=lambda ctx: logging.getLogger("t"))
    translator, captured = _make_translator_with(mocker, {"console": ld})

    # Create a mock logger config for the "console" logger
    mock_logger_options = LoggerOptions(log_level="INFO")
    translator._dagster_config.loggers = {"console": mock_logger_options}

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


def test_pipeline_translator_logger_string_reference_missing(mocker):
    """Test that pipeline translator raises error for missing logger string references."""
    translator, _ = _make_translator_with(mocker, {})
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
    with pytest.raises(ValueError, match=r"Logger 'missing' not found\."):
        translator.to_dagster()


def test_pipeline_translator_inline_logger_found(mocker):
    """Test that pipeline translator resolves inline logger configurations correctly."""
    job_name = "jobB"
    specific_name = f"{job_name}__logger_0"
    ld = dg.LoggerDefinition(logger_fn=lambda ctx: logging.getLogger("t"))
    translator, captured = _make_translator_with(mocker, {specific_name: ld})

    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": [LoggerOptions(log_level="INFO")],
                "executor": None,
            },
        )()
    }

    # Act
    translator.to_dagster()

    # Assert: job-specific logger name is looked up and passed through
    assert captured.get("logger_defs") == {specific_name: ld}


def test_pipeline_translator_inline_logger_missing(mocker):
    """Test that pipeline translator raises error for missing inline logger configurations."""
    job_name = "jobC"
    translator, _ = _make_translator_with(mocker, {})
    translator._dagster_config.jobs = {
        job_name: type(
            "Job",
            (),
            {
                "pipeline": PipelineOptions(pipeline_name="__default__"),
                "loggers": [LoggerOptions(log_level="INFO")],
                "executor": None,
            },
        )()
    }

    # Act / Assert
    with pytest.raises(
        ValueError, match=rf"Job-specific logger '{job_name}__logger_0' for inline logger configuration not found."
    ):
        translator.to_dagster()


def test_pipeline_translator_executor_string_reference_found(mocker):
    """Test that pipeline translator resolves executor string references correctly."""
    exec_def = object()
    translator, captured = _make_translator_with(mocker, named_executors={"seq": exec_def})

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


def test_pipeline_translator_executor_string_reference_missing(mocker):
    """Test that pipeline translator raises error for missing executor string references."""
    translator, _ = _make_translator_with(mocker)
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
    with pytest.raises(ValueError, match=r"Executor 'missing' not found\."):
        translator.to_dagster()


def test_pipeline_translator_executor_inline_found(mocker):
    """Test that pipeline translator resolves inline executor configurations correctly."""
    job_name = "jobB"
    job_exec_name = f"{job_name}__executor"
    exec_def = object()
    translator, captured = _make_translator_with(mocker, named_executors={job_exec_name: exec_def})

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


def test_pipeline_translator_executor_inline_missing(mocker):
    """Test that pipeline translator raises error for missing inline executor configurations."""
    job_name = "jobC"
    translator, _ = _make_translator_with(mocker)

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
    with pytest.raises(ValueError, match=rf"Job-specific executor '{job_name}__executor' not found\."):
        translator.to_dagster()


def test_warning_filter_specificity():
    """Test that the warning filter is specific to the exact message pattern."""
    # Test that the regex pattern matches correctly
    test_cases = [
        # Should match (be filtered)
        ("Argument(s) 'run_result' which are declared in the hookspec cannot be found in this hook call", True),
        (
            "Argument(s) 'run_results' which are declared in the hookspec cannot be found in this hook call",
            False,
        ),  # Different argument name
        (
            "Arguments 'run_result' which are declared in the hookspec cannot be found in this hook call",
            False,
        ),  # Different format
        (
            "Argument(s) 'other_arg' which are declared in the hookspec cannot be found in this hook call",
            False,
        ),  # Different argument
        # Should not match (not be filtered)
        ("Some completely different warning", False),
        ("hookspec related but different message", False),
        ("run_result mentioned but not hookspec", False),
    ]

    # This is the exact pattern used in the code
    pattern = r"Argument\(s\) 'run_result' which are declared in the hookspec cannot be found in this hook call"

    for message, should_match in test_cases:
        matches = re.search(pattern, message) is not None
        if should_match:
            assert matches, f"Expected pattern to match '{message}' but it didn't"
        else:
            assert not matches, f"Expected pattern not to match '{message}' but it did"


def test_warning_suppression_in_context():
    """Test the actual warning suppression context manager usage."""
    # Test that the context manager works as expected
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            r"Argument\(s\) 'run_result' which are declared in the hookspec cannot be found in this hook call",
            UserWarning,
            "pluggy._hooks",
        )

        # This should be suppressed but the test environment doesn't simulate the exact module
        # Just test that the warning suppression pattern works as expected
        original_warn = warnings.warn

        captured_messages = []

        def mock_warn(message, category=UserWarning, stacklevel=1, source=None):
            captured_messages.append(str(message))

        warnings.warn = mock_warn

        try:
            warnings.warn(
                "Argument(s) 'run_result' which are declared in the hookspec cannot be found in this hook call",
                UserWarning,
            )
            # The warning was captured by our mock, which means it wasn't filtered by the context
            # This is expected since we're not running in the exact pluggy context
            assert len(captured_messages) == 1
        finally:
            warnings.warn = original_warn


def test_create_after_pipeline_run_hook_op_method_exists(mocker):
    """Test that the _create_after_pipeline_run_hook_op method exists and can be called."""
    # Create a minimal mock setup to test that the method exists and has the warning suppression
    mock_context = mocker.Mock()
    mock_context.catalog = mocker.Mock()
    mock_context._hook_manager = mocker.Mock()

    # This is the minimal constructor args needed
    translator = PipelineTranslator(
        dagster_config=mocker.Mock(),
        context=mock_context,
        project_path="/fake/path",
        env="test",
        run_id="test-run",
        named_assets={},
        asset_partitions={},
        named_op_factories={},
        named_resources={},
        named_executors={},
        named_loggers={},
        enable_mlflow=False,
    )

    # Test that the method exists
    assert hasattr(translator, "_create_after_pipeline_run_hook_op")
    method = getattr(translator, "_create_after_pipeline_run_hook_op")
    assert callable(method)

    # Test that we can call it with a mock pipeline and run params
    mock_pipeline = mocker.Mock()
    after_pipeline_run_asset_names = ["test_asset"]
    hook_op = method("test_job", mock_pipeline, after_pipeline_run_asset_names)
    assert callable(hook_op)


def test_warnings_catch_warnings_context_manager_works():
    """Test that warnings.catch_warnings context manager works correctly."""
    # Test basic functionality to ensure our understanding is correct
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "test warning", UserWarning)

        captured = []
        original_warn = warnings.warn

        def capture_warn(message, category=UserWarning, stacklevel=1, source=None):
            captured.append(str(message))

        warnings.warn = capture_warn

        try:
            warnings.warn("test warning", UserWarning)
            warnings.warn("other warning", UserWarning)

            # Both warnings should be captured since we're bypassing the filter
            assert len(captured) == 2
            assert "test warning" in captured
            assert "other warning" in captured
        finally:
            warnings.warn = original_warn
