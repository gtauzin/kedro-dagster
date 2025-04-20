from unittest.mock import MagicMock

import dagster as dg
import pytest
from kedro.pipeline.node import Node

from kedro_dagster.pipelines import PipelineTranslator


class DummyPipeline:
    def __init__(self) -> None:
        self.nodes: list[Node] = []

    def inputs(self) -> list[str]:
        return ["input1"]

    def all_inputs(self) -> set[str]:
        return set(["input1"])

    def all_outputs(self) -> set[str]:
        return set(["output1"])

    @property
    def grouped_nodes(self) -> list[Node]:
        return []


class DummyContext:
    catalog = MagicMock()
    _hook_manager = MagicMock()


@pytest.fixture
def pipeline_translator() -> PipelineTranslator:
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


def test_materialize_input_assets_empty(pipeline_translator: PipelineTranslator) -> None:
    pipeline = DummyPipeline()
    result = pipeline_translator.translate_pipeline(
        pipeline=pipeline,
        pipeline_name="test_pipeline",
        filter_params={},
        job_name="test_job",
    )
    assert isinstance(result, dg.JobDefinition)
