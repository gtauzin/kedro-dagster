# mypy: ignore-errors

"""Tests for MLflow integration in kedro-dagster."""

from types import SimpleNamespace
from unittest.mock import Mock

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


def test_node_translator_mlflow_config_parameter():
    """Test that NodeTranslator accepts and stores mlflow_config parameter."""
    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    catalog = DataCatalog()
    pipeline = Pipeline([])

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=catalog,
        hook_manager=Mock(),
        run_id="test_run_config",
        asset_partitions={},
        named_resources={},
        env="base",
        mlflow_config=mock_config,
    )

    # Verify mlflow_config is stored
    assert node_translator._mlflow_config == mock_config


def test_node_translator_mlflow_config_none():
    """Test that NodeTranslator accepts None for mlflow_config."""
    catalog = DataCatalog()
    pipeline = Pipeline([])

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=catalog,
        hook_manager=Mock(),
        run_id="test_run_none",
        asset_partitions={},
        named_resources={},
        env="base",
        mlflow_config=None,
    )

    # Verify mlflow_config is None
    assert node_translator._mlflow_config is None


def test_node_translator_creates_op_with_mlflow_config(simple_pipeline, simple_catalog, monkeypatch):
    """Test that ops can be created when mlflow_config is provided."""
    # Mock _get_node_pipeline_name to avoid Kedro project lookups
    monkeypatch.setattr("kedro_dagster.nodes._get_node_pipeline_name", lambda node: "test_pipeline")

    mock_config = SimpleNamespace(ui=SimpleNamespace(host="localhost", port=5000))

    node_translator = NodeTranslator(
        pipelines=[simple_pipeline],
        catalog=simple_catalog,
        hook_manager=Mock(),
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
