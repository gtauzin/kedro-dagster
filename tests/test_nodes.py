# mypy: ignore-errors

from unittest.mock import MagicMock

import pytest
from kedro.framework.session import KedroSession
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_dagster.nodes import NodeTranslator


class DummyNode(Node):
    def __init__(self):
        def func(inputs):
            return {"output1": "result"}

        super().__init__(func=func, inputs=["input1"], outputs=["output1"], name="node1", tags=["tag1"])


class DummyCatalog:
    def load(self, name):
        return 42

    def _get_dataset(self, name):
        return MagicMock()


class DummyHookManager:
    def __init__(self):
        self.hook = MagicMock()


@pytest.fixture
def node_translator(kedro_project, metadata):
    # Use the real Kedro project's catalog and hook manager
    session = KedroSession.create(project_path=kedro_project, env="base")
    context = session.load_context()
    catalog = getattr(context, "catalog", None)
    hook_manager = getattr(context, "_hook_manager", None)
    pipeline = Pipeline([DummyNode()])
    return NodeTranslator(
        pipelines=[pipeline],
        catalog=catalog,
        hook_manager=hook_manager,
        session_id=session.session_id,
        named_resources={},
        env="base",
    )


def test_create_op_returns_dagster_op(node_translator):
    op = node_translator.create_op(DummyNode())
    assert callable(op)


def test_create_asset_returns_dagster_asset(node_translator):
    asset = node_translator.create_asset(DummyNode())
    assert callable(asset)
    assert asset.key.parts == ("base", "output1")


def test_create_op_auto_names_op(node_translator):
    unnamed_node = Node(
        func=lambda inputs: {"output1": "result"},
        inputs=["input1"],
        outputs=["output1"],
        name=None,  # No name provided
    )
    op = node_translator.create_op(unnamed_node)
    assert callable(op)
    # Check if the name is auto-generated
    assert op.name.startswith("unnamed_node_")
