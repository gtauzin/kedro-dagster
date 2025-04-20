from unittest.mock import MagicMock

import pytest

from kedro_dagster.kedro import KedroRunTranslator


class DummyContext:
    catalog = MagicMock()
    _hook_manager = MagicMock()


@pytest.fixture
def kedro_run_translator() -> KedroRunTranslator:
    return KedroRunTranslator(
        context=DummyContext(),
        project_path="/tmp",
        env="testenv",
        session_id="sess",
    )


def test_kedro_run_translator_to_dagster(kedro_run_translator: KedroRunTranslator) -> None:
    resource = kedro_run_translator.to_dagster(
        pipeline_name="__default__",
        filter_params={},
    )
    assert hasattr(resource, "model_dump")
    assert hasattr(resource, "after_context_created_hook")
