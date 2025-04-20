import pytest

from kedro_dagster.dagster import ExecutorCreator, LoggerTranslator, ScheduleCreator


class DummyDagsterConfig:
    jobs = {}
    executors = {}
    loggers = {}
    schedules = {}


@pytest.fixture
def dagster_config():
    return DummyDagsterConfig()


def test_executor_creator_instantiation(dagster_config):
    creator = ExecutorCreator(dagster_config=dagster_config)
    assert isinstance(creator, ExecutorCreator)


def test_logger_translator_instantiation(dagster_config):
    translator = LoggerTranslator(dagster_config=dagster_config, package_name="foo")
    assert isinstance(translator, LoggerTranslator)


def test_schedule_creator_instantiation(dagster_config):
    creator = ScheduleCreator(dagster_config=dagster_config, named_jobs={})
    assert isinstance(creator, ScheduleCreator)
