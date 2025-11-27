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
        "kedro.framework.project.settings",
        SimpleNamespace(_CONFIG_LOADER_ARGS={"default_run_env": ""}),
    )

    translator = KedroProjectTranslator(env="local", project_path=None)

    assert translator._project_path == Path.cwd()


def test_translator_passes_mlflow_config_to_node_translator(monkeypatch):
    """Test that KedroProjectTranslator extracts mlflow_config and passes it to NodeTranslator."""
    pytest.importorskip("mlflow")
    import mlflow  # noqa: F401

    # Create a mock project path
    tmp_project = Path(__file__).parent / "scenarios"

    # Mock MLflow configuration
    mock_mlflow_config = SimpleNamespace(
        tracking=SimpleNamespace(experiment=SimpleNamespace(name="test_exp")),
        server=SimpleNamespace(mlflow_tracking_uri="http://localhost:5000"),
        ui=SimpleNamespace(host="localhost", port=5000),
    )

    # Mock context with MLflow config
    mock_context = SimpleNamespace(
        mlflow=mock_mlflow_config,
        catalog=SimpleNamespace(list=lambda: []),
        _hook_manager=SimpleNamespace(),
    )

    # Patch is_mlflow_enabled to return True
    monkeypatch.setattr("kedro_dagster.translator.is_mlflow_enabled", lambda: True)

    # Patch initialize_kedro to avoid bootstrapping but set required attributes
    def mock_initialize_kedro(self, conf_source=None):
        self._context = mock_context
        self._session_id = "test_session_123"
        self._catalog = mock_context.catalog
        self._pipelines = {}

    monkeypatch.setattr(
        "kedro_dagster.translator.KedroProjectTranslator.initialize_kedro",
        mock_initialize_kedro,
    )

    # Mock get_dagster_config to avoid config loading errors
    from kedro_dagster.config.kedro_dagster import KedroDagsterConfig  # noqa: F401

    mock_dagster_config = KedroDagsterConfig(jobs={})
    monkeypatch.setattr(
        "kedro_dagster.translator.get_dagster_config",
        lambda context: mock_dagster_config,
    )

    # We don't need to patch KedroSession since initialize_kedro is mocked

    # Mock pipelines
    monkeypatch.setattr(
        "kedro.framework.project.pipelines",
        SimpleNamespace(
            get=lambda name: SimpleNamespace(nodes=[], tags=set()),
            __contains__=lambda self, name: True,
        ),
    )

    # Track calls to NodeTranslator
    node_translator_calls = []

    def mock_node_translator_init(self, *args, **kwargs):
        node_translator_calls.append(kwargs)
        # Store the mlflow_config parameter
        self._mlflow_config = kwargs.get("mlflow_config")
        # Create minimal attributes to avoid errors
        self._pipelines = kwargs.get("pipelines", [])
        self._catalog = kwargs.get("catalog")
        self._hook_manager = kwargs.get("hook_manager")
        self._asset_partitions = kwargs.get("asset_partitions", {})
        self._named_resources = kwargs.get("named_resources", {})
        self._env = kwargs.get("env", "base")
        self._run_id = kwargs.get("run_id", "test_run_id")

    from kedro_dagster.catalog import CatalogTranslator
    from kedro_dagster.dagster import ExecutorCreator, ScheduleCreator
    from kedro_dagster.kedro import KedroRunTranslator
    from kedro_dagster.nodes import NodeTranslator
    from kedro_dagster.pipelines import PipelineTranslator

    # Patch NodeTranslator.__init__
    monkeypatch.setattr(NodeTranslator, "__init__", mock_node_translator_init)

    # Also mock to_dagster to return empty results
    monkeypatch.setattr(NodeTranslator, "to_dagster", lambda self: ({}, {}))

    # Mock CatalogTranslator
    monkeypatch.setattr(CatalogTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(CatalogTranslator, "to_dagster", lambda self: ({}, {}))

    # Mock KedroRunTranslator
    import dagster as dg

    class MockKedroRunResource(dg.ConfigurableResource):
        def after_context_created_hook(self):
            pass

        def after_catalog_created_hook(self):
            pass

    mock_resource = MockKedroRunResource()
    monkeypatch.setattr(KedroRunTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(KedroRunTranslator, "to_dagster", lambda self, *args, **kwargs: mock_resource)
    monkeypatch.setattr(KedroRunTranslator, "_translate_on_pipeline_error_hook", lambda self, *args, **kwargs: {})

    # Mock PipelineTranslator
    monkeypatch.setattr(PipelineTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(PipelineTranslator, "to_dagster", lambda self: {})

    # Mock ExecutorCreator and ScheduleCreator
    monkeypatch.setattr(ExecutorCreator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(ExecutorCreator, "create_executors", lambda self: {})
    monkeypatch.setattr(ScheduleCreator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(ScheduleCreator, "create_schedules", lambda self: {})

    # Create translator
    translator = KedroProjectTranslator(project_path=tmp_project, env="base")

    # This should trigger initialization
    _ = translator.to_dagster()

    # Verify NodeTranslator was called with mlflow_config
    assert len(node_translator_calls) > 0
    assert "mlflow_config" in node_translator_calls[0]
    assert node_translator_calls[0]["mlflow_config"] == mock_mlflow_config


def test_translator_mlflow_config_none_when_not_configured(monkeypatch):
    """Test that mlflow_config is None when MLflow is not configured on context."""
    pytest.importorskip("mlflow")
    import mlflow  # noqa: F401

    tmp_project = Path(__file__).parent / "scenarios"

    # Mock context WITHOUT MLflow config
    mock_context = SimpleNamespace(
        catalog=SimpleNamespace(list=lambda: []),
        _hook_manager=SimpleNamespace(),
    )
    # Note: no mlflow attribute

    # Patch is_mlflow_enabled to return True (installed but not configured)
    monkeypatch.setattr("kedro_dagster.translator.is_mlflow_enabled", lambda: True)

    # Patch initialize_kedro to avoid bootstrapping but set required attributes
    def mock_initialize_kedro(self, conf_source=None):
        self._context = mock_context
        self._session_id = "test_session_456"
        self._catalog = mock_context.catalog
        self._pipelines = {}

    monkeypatch.setattr(
        "kedro_dagster.translator.KedroProjectTranslator.initialize_kedro",
        mock_initialize_kedro,
    )

    # Mock get_dagster_config to avoid config loading errors
    from kedro_dagster.config.kedro_dagster import KedroDagsterConfig  # noqa: F401

    mock_dagster_config = KedroDagsterConfig(jobs={})
    monkeypatch.setattr(
        "kedro_dagster.translator.get_dagster_config",
        lambda context: mock_dagster_config,
    )

    monkeypatch.setattr(
        "kedro.framework.project.pipelines",
        SimpleNamespace(
            get=lambda name: SimpleNamespace(nodes=[], tags=set()),
            __contains__=lambda self, name: True,
        ),
    )

    node_translator_calls = []

    def mock_node_translator_init(self, *args, **kwargs):
        node_translator_calls.append(kwargs)
        self._mlflow_config = kwargs.get("mlflow_config")
        self._pipelines = kwargs.get("pipelines", [])
        self._catalog = kwargs.get("catalog")
        self._hook_manager = kwargs.get("hook_manager")
        self._asset_partitions = kwargs.get("asset_partitions", {})
        self._named_resources = kwargs.get("named_resources", {})
        self._env = kwargs.get("env", "base")
        self._run_id = kwargs.get("run_id", "test_run_id")

    from kedro_dagster.catalog import CatalogTranslator
    from kedro_dagster.dagster import ExecutorCreator, ScheduleCreator
    from kedro_dagster.kedro import KedroRunTranslator
    from kedro_dagster.nodes import NodeTranslator
    from kedro_dagster.pipelines import PipelineTranslator

    monkeypatch.setattr(NodeTranslator, "__init__", mock_node_translator_init)
    monkeypatch.setattr(NodeTranslator, "to_dagster", lambda self: ({}, {}))

    # Mock CatalogTranslator
    monkeypatch.setattr(CatalogTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(CatalogTranslator, "to_dagster", lambda self: ({}, {}))

    # Mock KedroRunTranslator
    import dagster as dg

    class MockKedroRunResource(dg.ConfigurableResource):
        def after_context_created_hook(self):
            pass

        def after_catalog_created_hook(self):
            pass

    mock_resource = MockKedroRunResource()
    monkeypatch.setattr(KedroRunTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(KedroRunTranslator, "to_dagster", lambda self, *args, **kwargs: mock_resource)
    monkeypatch.setattr(KedroRunTranslator, "_translate_on_pipeline_error_hook", lambda self, *args, **kwargs: {})

    # Mock PipelineTranslator
    monkeypatch.setattr(PipelineTranslator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(PipelineTranslator, "to_dagster", lambda self: {})

    # Mock ExecutorCreator and ScheduleCreator
    monkeypatch.setattr(ExecutorCreator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(ExecutorCreator, "create_executors", lambda self: {})
    monkeypatch.setattr(ScheduleCreator, "__init__", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(ScheduleCreator, "create_schedules", lambda self: {})

    translator = KedroProjectTranslator(project_path=tmp_project, env="base")

    try:
        _ = translator.to_dagster()
    except Exception:
        pass

    # Verify NodeTranslator was called with mlflow_config=None
    assert len(node_translator_calls) > 0
    assert "mlflow_config" in node_translator_calls[0]
    assert node_translator_calls[0]["mlflow_config"] is None
