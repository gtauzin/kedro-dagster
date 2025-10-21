import dagster as dg
import pytest

from kedro_dagster.dagster import ExecutorCreator, LoggerTranslator, ScheduleCreator


class DummyDagsterConfig:
    jobs: dict[str, dg.OpDefinition] = {}
    executors: dict[str, dg.ExecutorDefinition] = {}
    loggers: dict[str, dg.LoggerDefinition] = {}
    schedules: dict[str, dg.ScheduleDefinition] = {}


@pytest.fixture
def dagster_config() -> DummyDagsterConfig:
    return DummyDagsterConfig()


def test_executor_creator_instantiation(dagster_config: DummyDagsterConfig) -> None:
    creator = ExecutorCreator(dagster_config=dagster_config)
    assert isinstance(creator, ExecutorCreator)


def test_logger_translator_instantiation(dagster_config: DummyDagsterConfig) -> None:
    translator = LoggerTranslator(dagster_config=dagster_config, package_name="foo")
    assert isinstance(translator, LoggerTranslator)


def test_schedule_creator_instantiation(dagster_config: DummyDagsterConfig) -> None:
    creator = ScheduleCreator(dagster_config=dagster_config, named_jobs={})
    assert isinstance(creator, ScheduleCreator)
