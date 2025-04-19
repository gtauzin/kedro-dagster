from unittest.mock import MagicMock

import pytest

from kedro_dagster.translator import DagsterCodeLocation, KedroProjectTranslator


class DummyContext:
    catalog = MagicMock()
    _hook_manager = MagicMock()
    mlflow = MagicMock()


class DummyProjectMetadata:
    package_name = "fake_project"


@pytest.fixture
def kedro_project_translator():
    return KedroProjectTranslator(
        project_path=None,
        env="testenv",
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
