# mypy: ignore-errors

import pytest
from kedro.framework.session import KedroSession

from kedro_dagster.kedro import KedroRunTranslator


@pytest.fixture
def kedro_run_translator(kedro_project, metadata):
    session = KedroSession.create(project_path=kedro_project, env="base")
    context = session.load_context()
    return KedroRunTranslator(
        context=context,
        project_path=str(kedro_project),
        env="base",
        session_id=session.session_id,
    )


def test_kedro_run_translator_to_dagster(kedro_run_translator):
    resource = kedro_run_translator.to_dagster(
        pipeline_name="__default__",
        filter_params={},
    )
    assert hasattr(resource, "model_dump")
    assert hasattr(resource, "after_context_created_hook")
