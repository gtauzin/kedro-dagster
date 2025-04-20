# mypy: ignore-errors

import pytest

from kedro_dagster.translator import DagsterCodeLocation, KedroProjectTranslator


@pytest.fixture
def kedro_project_translator(kedro_project):
    return KedroProjectTranslator(
        project_path=kedro_project,
        env="base",
        conf_source=None,
    )


def test_translator_initialization(kedro_project_translator):
    assert isinstance(kedro_project_translator, KedroProjectTranslator)


def test_dagster_code_location_fields():
    location = DagsterCodeLocation(
        named_ops={},
        named_assets={},
        named_resources={},
        named_jobs={},
        named_executors={},
        named_schedules={},
        named_sensors={},
        named_loggers={},
    )
    assert hasattr(location, "named_ops")
    assert hasattr(location, "named_assets")
    assert hasattr(location, "named_jobs")
