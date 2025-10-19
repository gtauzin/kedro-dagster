# mypy: ignore-errors

from unittest.mock import MagicMock

import dagster as dg
import pytest
from kedro.framework.session import KedroSession

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

    @property
    def grouped_nodes(self):
        return []


class DummyContext:
    catalog = MagicMock()
    _hook_manager = MagicMock()


@pytest.fixture
def pipeline_translator(kedro_project):
    # Create a Kedro session and context as in translator.py
    session = KedroSession.create(project_path=kedro_project, env="base")
    context = session.load_context()
    return PipelineTranslator(
        dagster_config=MagicMock(jobs={}),
        context=context,
        project_path=str(kedro_project),
        env="base",
        session_id=session.session_id,
        named_assets={},
        named_op_factories={},
        named_resources={},
        named_executors={},
        enable_mlflow=False,
    )


def test_materialize_in_assets_empty(pipeline_translator):
    # Use the real pipeline from the Kedro context if available
    pipeline = DummyPipeline()
    result = pipeline_translator.translate_pipeline(
        pipeline=pipeline,
        pipeline_name="test_pipeline",
        filter_params={},
        job_name="test_job",
    )
    assert isinstance(result, dg.JobDefinition)
