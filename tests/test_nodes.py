from typing import Any
from unittest.mock import MagicMock

import pytest

from kedro_dagster.nodes import NodeTranslator


class DummyNode:
    def __init__(self) -> None:
        self.name = "node1"
        self.inputs = ["input1"]
        self.outputs = ["output1"]
        self.tags = ["tag1"]

    def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        return {"output1": "result"}


class DummyCatalog:
    def load(self, name: str) -> int:
        return 42

    def _get_dataset(self, name: str) -> MagicMock:
        return MagicMock()


class DummyHookManager:
    def __init__(self) -> None:
        self.hook = MagicMock()


@pytest.fixture
def node_translator() -> NodeTranslator:
    return NodeTranslator(
        pipelines=[],
        catalog=DummyCatalog(),
        hook_manager=DummyHookManager(),
        session_id="sess",
        named_resources={},
        env="testenv",
    )


def test_create_op_returns_dagster_op(node_translator: NodeTranslator) -> None:
    op = node_translator.create_op(DummyNode())
    assert callable(op)
    assert hasattr(op, "__call__")


@pytest.mark.skip(reason="Need to use kedro project")
def test_create_asset_returns_dagster_asset(node_translator: NodeTranslator) -> None:
    asset = node_translator.create_asset(DummyNode())
    assert callable(asset)
    assert hasattr(asset, "__call__")
