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
def node_translator(kedro_project):
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
    assert hasattr(op, "__call__")


def test_create_asset_returns_dagster_asset(node_translator):
    asset = node_translator.create_asset(DummyNode())
    assert callable(asset)
    assert hasattr(asset, "__call__")
