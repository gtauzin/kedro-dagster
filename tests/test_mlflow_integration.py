# mypy: ignore-errors

"""Tests for MLflow integration in kedro-dagster."""

import sys
from types import SimpleNamespace

import dagster as dg
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline, node

from kedro_dagster.nodes import NodeTranslator


def dummy_func(x):
    """Simple function for testing."""
    return x * 2


@pytest.fixture
def simple_pipeline():
    """Create a simple pipeline for testing."""
    return Pipeline([
        node(
            func=dummy_func,
            inputs="input_data",
            outputs="output_data",
            name="test_node",
        )
    ])


@pytest.fixture
def simple_catalog():
    """Create a simple catalog for testing."""
    return DataCatalog(
        datasets={
            "input_data": MemoryDataset(),
            "output_data": MemoryDataset(),
        }
    )


def test_node_translator_mlflow_config_parameter(mocker):
    """Test that NodeTranslator accepts and stores mlflow_config parameter."""
    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    catalog = DataCatalog()
    pipeline = Pipeline([])

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_config",
        asset_partitions={},
        named_resources={},
        env="base",
        mlflow_config=mock_config,
    )

    # Verify mlflow_config is stored
    assert node_translator._mlflow_config == mock_config


def test_node_translator_mlflow_config_none(mocker):
    """Test that NodeTranslator accepts None for mlflow_config."""
    catalog = DataCatalog()
    pipeline = Pipeline([])

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_none",
        asset_partitions={},
        named_resources={},
        env="base",
        mlflow_config=None,
    )

    # Verify mlflow_config is None
    assert node_translator._mlflow_config is None


def test_node_translator_creates_op_with_mlflow_config(simple_pipeline, simple_catalog, mocker):
    """Test that ops can be created when mlflow_config is provided."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    mocker.patch("kedro_dagster.nodes._get_node_pipeline_name", return_value="test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_123",
        asset_partitions={},
        named_resources={},
        env="base",
        mlflow_config=mock_config,
    )

    test_node = simple_pipeline.nodes[0]
    op = node_translator.create_op(test_node)

    # Verify op was created successfully
    assert isinstance(op, dg.OpDefinition)
    assert op.name == "test_node"


def test_op_execution_without_mlflow_resource(simple_pipeline, simple_catalog, mocker):
    """Test that op executes correctly when MLflow resource is not available."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    mocker.patch("kedro_dagster.nodes._get_node_pipeline_name", return_value="test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_no_mlflow",
        asset_partitions={},
        named_resources={},  # No MLflow resource
        env="base",
        mlflow_config=mock_config,
    )

    test_node = simple_pipeline.nodes[0]
    op_def = node_translator.create_op(test_node)

    # Create proper execution context using build_op_context
    context = dg.build_op_context(resources={}, op_config={})

    # Execute the op
    simple_catalog.save("input_data", 5)
    result = op_def(context, input_data=5)

    # Verify execution completed
    # Op returns (output, None) because it's in the last layer with the hook output
    assert result[0] == 10


def test_op_execution_with_mlflow_resource_no_active_run(simple_pipeline, simple_catalog, mocker):
    """Test that op executes correctly when MLflow resource exists but no active run."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    mocker.patch("kedro_dagster.nodes._get_node_pipeline_name", return_value="test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    # Create MLflow resource
    mlflow_resource = mocker.Mock()
    named_resources = {"mlflow": dg.ResourceDefinition.hardcoded_resource(mlflow_resource)}

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_no_active",
        asset_partitions={},
        named_resources=named_resources,
        env="base",
        mlflow_config=mock_config,
    )

    test_node = simple_pipeline.nodes[0]
    op_def = node_translator.create_op(test_node)

    # Mock mlflow module
    mock_mlflow = mocker.Mock()
    mock_mlflow.active_run.return_value = None  # No active run

    # Patch mlflow import
    mocker.patch.dict(sys.modules, {"mlflow": mock_mlflow})

    # Create proper execution context with MLflow resource
    context = dg.build_op_context(resources={"mlflow": mlflow_resource}, op_config={})

    # Execute the op
    simple_catalog.save("input_data", 7)
    result = op_def(context, input_data=7)

    # Verify execution completed
    # Op returns (output, None) because it's in the last layer with the hook output
    assert result[0] == 14


def test_op_execution_with_active_mlflow_run(simple_pipeline, simple_catalog, mocker):
    """Test that op captures MLflow metadata when an active run exists."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    mocker.patch("kedro_dagster.nodes._get_node_pipeline_name", return_value="test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    # Create MLflow resource
    mlflow_resource = mocker.Mock()
    named_resources = {"mlflow": dg.ResourceDefinition.hardcoded_resource(mlflow_resource)}

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_active",
        asset_partitions={},
        named_resources=named_resources,
        env="base",
        mlflow_config=mock_config,
    )

    test_node = simple_pipeline.nodes[0]
    op_def = node_translator.create_op(test_node)

    # Mock MLflow active run
    mock_run_info = mocker.Mock()
    mock_run_info.experiment_id = "exp_123"
    mock_run_info.run_id = "run_456"

    mock_active_run = mocker.Mock()
    mock_active_run.info = mock_run_info

    mock_mlflow = mocker.Mock()
    mock_mlflow.active_run.return_value = mock_active_run
    mock_mlflow.get_tracking_uri.return_value = "http://localhost:5000"

    # Mock get_mlflow_run_url
    expected_url = "http://localhost:5000/#/experiments/exp_123/runs/run_456"
    mocker.patch("kedro_dagster.nodes.get_mlflow_run_url", return_value=expected_url)

    # Patch mlflow import
    mocker.patch.dict(sys.modules, {"mlflow": mock_mlflow})

    # Create proper execution context with MLflow resource
    context = dg.build_op_context(resources={"mlflow": mlflow_resource}, op_config={})
    # Mock instance.add_run_tags to avoid DagsterRunNotFoundError
    context.instance.add_run_tags = mocker.Mock()

    # Execute the op
    simple_catalog.save("input_data", 3)
    result = op_def(context, input_data=3)

    # Verify execution completed
    # Op returns (output, None) because it's in the last layer with the hook output
    assert result[0] == 6

    # Verify add_run_tags was called with MLflow metadata
    assert context.instance.add_run_tags.called
    call_args = context.instance.add_run_tags.call_args
    tags = call_args[0][1]
    assert tags["mlflow_experiment_id"] == "exp_123"
    assert tags["mlflow_run_id"] == "run_456"
    assert tags["mlflow_tracking_uri"] == "http://localhost:5000"
    assert tags["mlflow_run_url"] == expected_url


def test_op_execution_mlflow_metadata_includes_url_metadata_value(simple_pipeline, simple_catalog, mocker):
    """Test that MLflow URL is wrapped in MetadataValue.url for asset materialization."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    mocker.patch("kedro_dagster.nodes._get_node_pipeline_name", return_value="test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    # Create MLflow resource
    mlflow_resource = mocker.Mock()
    named_resources = {"mlflow": dg.ResourceDefinition.hardcoded_resource(mlflow_resource)}

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=mocker.Mock(),
        run_id="test_run_metadata",
        asset_partitions={},
        named_resources=named_resources,
        env="base",
        mlflow_config=mock_config,
    )

    test_node = simple_pipeline.nodes[0]
    op_def = node_translator.create_op(test_node)

    # Mock MLflow active run
    mock_run_info = mocker.Mock()
    mock_run_info.experiment_id = "exp_999"
    mock_run_info.run_id = "run_888"

    mock_active_run = mocker.Mock()
    mock_active_run.info = mock_run_info

    mock_mlflow = mocker.Mock()
    mock_mlflow.active_run.return_value = mock_active_run
    mock_mlflow.get_tracking_uri.return_value = "https://mlflow.example.com"

    # Mock get_mlflow_run_url
    expected_url = "https://mlflow.example.com/#/experiments/exp_999/runs/run_888"
    mocker.patch("kedro_dagster.nodes.get_mlflow_run_url", return_value=expected_url)

    # Patch mlflow import
    mocker.patch.dict(sys.modules, {"mlflow": mock_mlflow})

    # Create proper execution context with MLflow resource
    context = dg.build_op_context(resources={"mlflow": mlflow_resource}, op_config={})
    context.instance.add_run_tags = mocker.Mock()

    # Mock log_event to capture materialization
    logged_events = []
    context.log_event = lambda event: logged_events.append(event)

    # Execute the op
    simple_catalog.save("input_data", 4)
    result = op_def(context, input_data=4)

    # Verify execution completed
    assert result[0] == 8

    # Verify that an AssetMaterialization was logged
    assert len(logged_events) > 0
    materialization = logged_events[0]
    assert isinstance(materialization, dg.AssetMaterialization)

    # Verify MLflow metadata is in the materialization
    assert materialization.metadata is not None
    assert "mlflow_experiment_id" in materialization.metadata
    assert "mlflow_run_id" in materialization.metadata
    assert "mlflow_tracking_uri" in materialization.metadata
    assert "mlflow_run_url" in materialization.metadata

    # Verify the URL is wrapped in MetadataValue.url
    url_value = materialization.metadata["mlflow_run_url"]
    assert isinstance(url_value, dg.MetadataValue)
