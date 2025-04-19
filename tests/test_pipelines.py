from unittest.mock import MagicMock

import pytest

from kedro_dagster.pipelines import PipelineTranslator


class DummyPipeline:
    def __init__(self):
        self.nodes = []

    def inputs(self):
        return ["input1"]

    def all_inputs(self):
        return set(["input1"])

    def all_outputs(self):
        return set(["output1"])

    def grouped_nodes(self):
        return []


class DummyContext:
    catalog = MagicMock()
    _hook_manager = MagicMock()


@pytest.fixture
def pipeline_translator():
    return PipelineTranslator(
        dagster_config=MagicMock(jobs={}),
        context=DummyContext(),
        project_path="/tmp",
        env="testenv",
        session_id="sess",
        named_assets={},
        named_ops={},
        named_resources={},
        named_executors={},
    )


def test_materialize_input_assets_empty(pipeline_translator):
    pipeline = DummyPipeline()
    result = pipeline_translator._materialize_input_assets(pipeline)
    assert isinstance(result, dict)
