# mypy: ignore-errors

from __future__ import annotations

import io
import logging

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pydantic import ValidationError

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.config.automation import ScheduleOptions
from kedro_dagster.config.execution import InProcessExecutorOptions, MultiprocessExecutorOptions
from kedro_dagster.config.job import JobOptions, PipelineOptions
from kedro_dagster.config.kedro_dagster import KedroDagsterConfig
from kedro_dagster.config.logging import LoggerOptions
from kedro_dagster.dagster import ExecutorCreator, LoggerCreator, ScheduleCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator
from tests.scenarios.kedro_projects import pipeline_registry_default
from tests.scenarios.project_factory import KedroProjectOptions


# Helper classes for testing without Mock
class MockJob:
    """Simple mock job class for testing."""

    def __init__(self, loggers=None, schedule=None, executor=None):
        self.loggers = loggers
        self.schedule = schedule
        self.executor = executor


class MockConfig:
    """Simple mock config class for testing."""

    def __init__(self, loggers=None, jobs=None, schedules=None, executors=None):
        self.loggers = loggers or {}
        self.jobs = jobs or {}
        self.schedules = schedules or {}
        self.executors = executors or {}


@pytest.mark.parametrize("env", ["base", "local"])
def test_schedule_creator_uses_named_schedule(env, request):
    """Create a named schedule for the 'default' job with the expected cron expression."""
    # Use the integration scenario which includes executors, schedules and a default job
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Minimal path to jobs to feed into ScheduleCreator: compose with PipelineTranslator
    default_pipeline = pipelines.get("__default__")
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()
    node_translator = NodeTranslator(
        pipelines=[default_pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
        run_id=session.session_id,
    )
    named_op_factories, named_assets = node_translator.to_dagster()

    # Create loggers for PipelineTranslator
    logger_creator = LoggerCreator(dagster_config=dagster_config)
    named_loggers = logger_creator.create_loggers()

    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors={"seq": dg.in_process_executor},
        named_loggers=named_loggers,
        enable_mlflow=False,
        run_id=session.session_id,
    )
    named_jobs = pipeline_translator.to_dagster()

    schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
    named_schedules = schedule_creator.create_schedules()

    assert "default" in named_schedules
    schedule = named_schedules["default"]
    assert isinstance(schedule, dg.ScheduleDefinition)
    assert schedule.cron_schedule == "0 6 * * *"


@pytest.mark.parametrize("env", ["base", "local"])
def test_executor_translator_creates_multiple_executors(env, request):
    """Build multiple executor definitions from config and validate their names/types."""
    # Arrange: project with multiple executors and a default job
    options = request.getfixturevalue(f"kedro_project_multi_executors_{env}")
    project_path = options.project_path

    # Act: parse dagster config and build executors
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    dagster_config = get_dagster_config(context)
    executors = ExecutorCreator(dagster_config=dagster_config).create_executors()

    # Assert: both executors are registered
    assert "seq" in executors
    assert "multiproc" in executors

    assert len(executors) == 2  # noqa: PLR2004

    assert isinstance(executors["seq"], dg.ExecutorDefinition)
    assert executors["seq"].name == "in_process"
    assert isinstance(executors["multiproc"], dg.ExecutorDefinition)
    assert executors["multiproc"].name == "multiprocess"


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_schedule_creator_raises_for_unknown_named_schedule(env, project_scenario_factory):
    """When a job references a non-existent named schedule, raise a clear ValueError."""
    # Build a Kedro project with a job referencing schedule "unknown" and no schedules defined
    dagster_cfg = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq", "schedule": "unknown"}},
    }
    opts = KedroProjectOptions(env=env, dagster=dagster_cfg, pipeline_registry_py=pipeline_registry_default())
    options = project_scenario_factory(opts, project_name="kedro-project-unknown-schedule")

    # Bootstrap and build Dagster objects
    bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Provide a minimal, valid Dagster job mapping to the ScheduleCreator without
    # relying on Kedro translators (we're testing schedule resolution only here).
    @dg.op
    def _noop():
        pass

    @dg.job
    def default():
        _noop()

    named_jobs = {"default": default}

    schedule_creator = ScheduleCreator(dagster_config=dagster_config, named_jobs=named_jobs)
    with pytest.raises(ValueError) as e:
        schedule_creator.create_schedules()

    assert "Schedule named 'unknown' for job 'default' not found in available schedules" in str(e.value)


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_executor_creator_unsupported_executor_raises(env, project_scenario_factory):
    """If an optional executor's package isn't installed, creating it should raise a ValueError."""
    # Use a k8s executor which relies on `dagster_k8s` being importable; assume it's not installed in test env.
    dagster_cfg = {
        "executors": {"k8s": {"k8s_job_executor": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "k8s"}},
    }
    options = project_scenario_factory(
        KedroProjectOptions(env=env, dagster=dagster_cfg, pipeline_registry_py=pipeline_registry_default()),
        project_name="kedro-project-k8s-executor",
    )

    bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    creator = ExecutorCreator(dagster_config=dagster_config)
    with pytest.raises(ValueError) as e:
        creator.create_executors()

    assert "not supported" in str(e.value)


@pytest.mark.parametrize("env", ["base"])  # keep fast
def test_logger_translator_exact_key_and_description(env, request):
    """LoggerCreator should emit exact module key and description for non-default pipelines."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")

    bootstrap_project(options.project_path)
    session = KedroSession.create(project_path=options.project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    translator = LoggerCreator(dagster_config=dagster_config)
    named_loggers = translator.create_loggers()

    # Should have console and file_logger from config
    assert "console" in named_loggers
    assert "file_logger" in named_loggers

    # Validate the description text for the emitted logger definitions
    assert "console" in named_loggers["console"].description
    assert "file_logger" in named_loggers["file_logger"].description


def test_logger_create_simple_logger():
    """Test creating a simple logger with minimal configuration."""
    config = KedroDagsterConfig(
        loggers={
            "simple": LoggerOptions(
                log_level="INFO",
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "simple" in loggers
    assert "Logger definition `simple`" in loggers["simple"].description


def test_logger_create_multiple_loggers():
    """Test creating multiple loggers with different configurations."""
    config = KedroDagsterConfig(
        loggers={
            "info_logger": LoggerOptions(
                log_level="INFO",
            ),
            "debug_logger": LoggerOptions(
                log_level="DEBUG",
            ),
            "error_logger": LoggerOptions(
                log_level="ERROR",
            ),
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    expected_logger_count = 3
    assert len(loggers) == expected_logger_count
    assert "info_logger" in loggers
    assert "debug_logger" in loggers
    assert "error_logger" in loggers


def test_logger_no_loggers_config():
    """Test when no loggers are configured."""
    config = KedroDagsterConfig()

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert len(loggers) == 0


def test_logger_empty_loggers_config():
    """Test when loggers config is empty dict."""
    config = KedroDagsterConfig(loggers={})

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert len(loggers) == 0


def test_logger_with_file_handler():
    """Test creating logger with file handler."""
    config = KedroDagsterConfig(
        loggers={
            "file_logger": LoggerOptions(
                log_level="DEBUG",
                handlers=[
                    {
                        "class": "logging.FileHandler",
                        "filename": "test.log",
                        "level": "DEBUG",
                    }
                ],
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "file_logger" in loggers
    logger_def = loggers["file_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `file_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_logger_with_stream_handler():
    """Test creating logger with stream handler."""
    config = KedroDagsterConfig(
        loggers={
            "stream_logger": LoggerOptions(
                log_level="INFO",
                handlers=[
                    {
                        "class": "logging.StreamHandler",
                        "level": "INFO",
                    }
                ],
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "stream_logger" in loggers
    logger_def = loggers["stream_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `stream_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_logger_with_custom_formatter():
    """Test creating logger with custom formatter."""
    config = KedroDagsterConfig(
        loggers={
            "formatted_logger": LoggerOptions(
                log_level="INFO",
                formatters={
                    "detailed": {
                        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    }
                },
                handlers=[{"class": "logging.StreamHandler", "level": "INFO", "formatter": "detailed"}],
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "formatted_logger" in loggers
    logger_def = loggers["formatted_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `formatted_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_logger_with_multiple_formatters():
    """Test creating logger with multiple formatters."""
    config = KedroDagsterConfig(
        loggers={
            "multi_format_logger": LoggerOptions(
                log_level="DEBUG",
                formatters={
                    "simple": {"format": "%(levelname)s: %(message)s"},
                    "detailed": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
                },
                handlers=[{"class": "logging.StreamHandler", "level": "INFO", "formatter": "simple"}],
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "multi_format_logger" in loggers
    logger_def = loggers["multi_format_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `multi_format_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_logger_invalid_log_level_validation():
    """Test that invalid log levels are caught during config validation."""
    with pytest.raises(ValidationError, match="Invalid log level"):
        KedroDagsterConfig(
            loggers={
                "invalid_logger": LoggerOptions(
                    log_level="INVALID_LEVEL",  # This should fail validation
                )
            }
        )


def test_logger_invalid_logger_name_validation():
    """Test that logger options can be created without logger_name field."""
    # Logger name validation was removed, this test now just checks basic creation
    config = KedroDagsterConfig(
        loggers={
            "valid_logger": LoggerOptions(
                log_level="INFO",
            )
        }
    )
    assert "valid_logger" in config.loggers


def test_logger_nonexistent_formatter_reference():
    """Test that referencing non-existent formatter raises validation error."""
    with pytest.raises(ValidationError, match="references unknown formatter"):
        KedroDagsterConfig(
            loggers={
                "bad_logger": LoggerOptions(
                    log_level="INFO",
                    handlers=[{"class": "logging.StreamHandler", "formatter": "nonexistent_formatter"}],
                )
            }
        )


def test_logger_case_insensitive_log_levels():
    """Test that log levels are case insensitive."""
    for level in ["info", "INFO", "Info", "iNfO"]:
        config = KedroDagsterConfig(
            loggers={
                "case_test": LoggerOptions(
                    log_level=level,
                )
            }
        )

        translator = LoggerCreator(dagster_config=config)
        loggers = translator.create_loggers()

        assert "case_test" in loggers
        logger_def = loggers["case_test"]
        assert logger_def.description == "Logger definition `case_test`."


def test_logger_complex_configuration():
    """Test a complex logger configuration with multiple handlers, formatters."""
    config = KedroDagsterConfig(
        loggers={
            "complex_logger": LoggerOptions(
                log_level="DEBUG",
                formatters={
                    "console_format": {"format": "%(name)s - %(levelname)s - %(message)s"},
                    "file_format": {
                        "format": "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    },
                },
                handlers=[
                    {"class": "logging.StreamHandler", "level": "INFO", "formatter": "console_format"},
                    {
                        "class": "logging.FileHandler",
                        "filename": "test.log",
                        "level": "DEBUG",
                        "formatter": "file_format",
                    },
                ],
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    assert "complex_logger" in loggers
    logger_def = loggers["complex_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `complex_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_logger_with_default_handler():
    """Test that logger gets default handler when none specified."""
    config = KedroDagsterConfig(
        loggers={
            "default_handler_logger": LoggerOptions(
                log_level="INFO",
                # No handlers specified - should get default StreamHandler
            )
        }
    )

    translator = LoggerCreator(dagster_config=config)
    loggers = translator.create_loggers()

    logger_def = loggers["default_handler_logger"]

    # Check that the logger definition was created properly
    assert logger_def.description == "Logger definition `default_handler_logger`."
    assert hasattr(logger_def, "logger_fn")


def test_job_options_with_logger_string_references():
    """Test JobOptions with logger string references."""
    job_config = JobOptions(pipeline=PipelineOptions(pipeline_name="test_pipeline"), loggers=["console", "file_logger"])

    assert job_config.loggers == ["console", "file_logger"]
    assert isinstance(job_config.loggers[0], str)


def test_job_options_with_inline_logger_options():
    """Test JobOptions with inline LoggerOptions."""
    inline_logger = LoggerOptions(log_level="INFO")

    job_config = JobOptions(pipeline=PipelineOptions(pipeline_name="test_pipeline"), loggers=[inline_logger])

    assert len(job_config.loggers) == 1
    assert isinstance(job_config.loggers[0], LoggerOptions)
    assert job_config.loggers[0].log_level == "INFO"


def test_job_options_mixed_logger_types():
    """Test JobOptions with mixed logger types (both strings and LoggerOptions)."""
    inline_logger = LoggerOptions(log_level="DEBUG")

    job_config = JobOptions(pipeline=PipelineOptions(pipeline_name="test_pipeline"), loggers=["console", inline_logger])

    expected_logger_count = 2
    assert len(job_config.loggers) == expected_logger_count
    assert isinstance(job_config.loggers[0], str)
    assert job_config.loggers[0] == "console"
    assert isinstance(job_config.loggers[1], LoggerOptions)


def test_pipeline_translator_handles_inline_loggers():
    """Test that PipelineTranslator can handle inline LoggerOptions in job config."""
    # This would normally be an integration test, but let's test the logger processing logic

    # Create a config with an inline logger for a job
    inline_logger = LoggerOptions(log_level="WARNING")

    # Test creating a logger from inline configuration
    temp_config = KedroDagsterConfig(loggers={"job_test_logger": inline_logger})
    logger_creator = LoggerCreator(dagster_config=temp_config)
    loggers = logger_creator.create_loggers()

    assert "job_test_logger" in loggers
    logger_def = loggers["job_test_logger"]
    assert "Logger definition `job_test_logger`" in logger_def.description
    assert hasattr(logger_def, "logger_fn")


def test_logger_creator_with_job_inline_loggers(monkeypatch):
    """Test LoggerCreator handling inline loggers defined directly in job configurations."""

    # Create a mock config object using a simple class
    class MockConfig:
        def __init__(self):
            self.loggers = {"global_logger": LoggerOptions(log_level="INFO")}

            # Create mock jobs with inline logger configurations
            self.jobs = {}

    # Set up the mock config
    mock_config = MockConfig()

    # Create mock job objects
    class MockJob:
        def __init__(self, loggers=None):
            self.loggers = loggers

    mock_config.jobs = {"job_with_inline": MockJob(), "job_with_reference": MockJob(), "job_without_loggers": MockJob()}

    # Job with inline logger configuration
    inline_logger = LoggerOptions(log_level="DEBUG")
    mock_config.jobs["job_with_inline"].loggers = [inline_logger]

    # Job with string reference to global logger
    mock_config.jobs["job_with_reference"].loggers = ["global_logger"]

    # Job without loggers
    mock_config.jobs["job_without_loggers"].loggers = None

    logger_creator = LoggerCreator(dagster_config=mock_config)
    loggers = logger_creator.create_loggers()

    # Should have global logger and job-specific inline logger
    assert "global_logger" in loggers
    assert "job_with_inline__logger_0" in loggers

    # Verify the inline logger was created correctly
    inline_logger_def = loggers["job_with_inline__logger_0"]
    assert "Logger definition `job_with_inline__logger_0`" in inline_logger_def.description


def test_logger_creator_validates_job_string_references():
    """Test LoggerCreator validates job logger string references against available loggers."""

    # Create a mock config with global loggers
    mock_config = MockConfig(
        loggers={"available_logger": LoggerOptions(log_level="INFO")},
        jobs={"job_with_bad_ref": MockJob(loggers=["nonexistent_logger"])},
    )

    logger_creator = LoggerCreator(dagster_config=mock_config)

    # Should raise ValueError for invalid reference
    with pytest.raises(ValueError) as exc_info:
        logger_creator.create_loggers()

    assert "Logger 'nonexistent_logger' referenced in job 'job_with_bad_ref'" in str(exc_info.value)
    assert "not found in available loggers" in str(exc_info.value)


def test_logger_creator_mixed_job_logger_types():
    """Test LoggerCreator handling jobs with mixed logger types (strings and inline configs)."""

    inline_logger = LoggerOptions(log_level="WARNING")

    # Create a mock config with global loggers
    mock_config = MockConfig(
        loggers={"shared_logger": LoggerOptions(log_level="INFO")},
        jobs={
            "mixed_job": MockJob(
                loggers=[
                    "shared_logger",  # String reference
                    inline_logger,  # Inline configuration
                ]
            )
        },
    )

    logger_creator = LoggerCreator(dagster_config=mock_config)
    loggers = logger_creator.create_loggers()

    # Should have both global logger and job-specific inline logger
    assert "shared_logger" in loggers
    assert "mixed_job__logger_1" in loggers  # Second item gets index 1

    # Verify the inline logger was created correctly
    inline_logger_def = loggers["mixed_job__logger_1"]
    assert "Logger definition `mixed_job__logger_1`" in inline_logger_def.description


def test_schedule_creator_with_inline_schedule(monkeypatch):
    """Test ScheduleCreator handling inline schedule configurations in job definitions."""

    inline_schedule = ScheduleOptions(
        cron_schedule="0 12 * * *", execution_timezone="UTC", description="Inline schedule for testing"
    )

    # Create a mock config with no global schedules
    mock_config = MockConfig(schedules={}, jobs={"job_with_inline_schedule": MockJob(schedule=inline_schedule)})

    # Mock the actual schedule creation to avoid Dagster validation issues
    def mock_create_schedule(*args, **kwargs):
        name = kwargs.get("name", "test_schedule")
        cron_schedule = kwargs.get("cron_schedule", "0 0 * * *")
        mock_schedule = type("MockSchedule", (), {"name": name, "cron_schedule": cron_schedule})()
        return mock_schedule

    # Patch ScheduleDefinition creation
    monkeypatch.setattr("dagster.ScheduleDefinition", mock_create_schedule)

    schedule_creator = ScheduleCreator(dagster_config=mock_config, named_jobs={"job_with_inline_schedule": "mock_job"})
    schedules = schedule_creator.create_schedules()

    # Should create schedule with job-based name
    assert "job_with_inline_schedule" in schedules
    schedule_def = schedules["job_with_inline_schedule"]
    assert schedule_def.name == "job_with_inline_schedule__schedule"
    assert schedule_def.cron_schedule == "0 12 * * *"


def test_schedule_creator_validates_string_references():
    """Test ScheduleCreator validates job schedule string references against available schedules."""

    # Create a mock config with global schedules
    mock_config = MockConfig(
        schedules={"available_schedule": ScheduleOptions(cron_schedule="0 6 * * *", execution_timezone="UTC")},
        jobs={"job_with_bad_schedule_ref": MockJob(schedule="nonexistent_schedule")},
    )

    # Create mock jobs dictionary for ScheduleCreator
    mock_job = "mock_job"
    named_jobs = {"job_with_bad_schedule_ref": mock_job}

    schedule_creator = ScheduleCreator(dagster_config=mock_config, named_jobs=named_jobs)

    # Should raise ValueError for invalid reference
    with pytest.raises(ValueError) as exc_info:
        schedule_creator.create_schedules()

    assert (
        "Schedule named 'nonexistent_schedule' for job 'job_with_bad_schedule_ref' not found in available schedules"
        in str(exc_info.value)
    )


def test_schedule_creator_mixed_schedule_types(monkeypatch):
    """Test ScheduleCreator handling mixed schedule types (strings and inline configs)."""

    inline_schedule = ScheduleOptions(
        cron_schedule="0 18 * * *", execution_timezone="Europe/London", description="Evening schedule"
    )

    # Create a mock config with global schedules
    mock_config = MockConfig(
        schedules={"global_schedule": ScheduleOptions(cron_schedule="0 6 * * *", execution_timezone="UTC")},
        jobs={
            "job_with_string_ref": MockJob(schedule="global_schedule"),
            "job_with_inline": MockJob(schedule=inline_schedule),
            "job_without_schedule": MockJob(schedule=None),
        },
    )

    # Mock the actual schedule creation to avoid Dagster validation issues
    def mock_create_schedule(*args, **kwargs):
        name = kwargs.get("name", "test_schedule")
        cron_schedule = kwargs.get("cron_schedule", "0 0 * * *")
        mock_schedule = type("MockSchedule", (), {"name": name, "cron_schedule": cron_schedule})()
        return mock_schedule

    # Patch ScheduleDefinition creation
    monkeypatch.setattr("dagster.ScheduleDefinition", mock_create_schedule)

    # Create mock jobs dictionary for ScheduleCreator
    mock_jobs = {
        "job_with_string_ref": "mock_job_1",
        "job_with_inline": "mock_job_2",
        "job_without_schedule": "mock_job_3",
    }

    schedule_creator = ScheduleCreator(dagster_config=mock_config, named_jobs=mock_jobs)
    schedules = schedule_creator.create_schedules()

    # Should create schedules for jobs with schedule configurations
    expected_schedule_count = 2
    assert len(schedules) == expected_schedule_count
    assert "job_with_string_ref" in schedules
    assert "job_with_inline" in schedules
    assert "job_without_schedule" not in schedules


def test_schedule_creator_handles_none_schedule():
    """Test ScheduleCreator properly handles jobs with schedule=None."""

    # Create a mock config with job that has no schedule
    mock_config = MockConfig(schedules={}, jobs={"job_without_schedule": MockJob(schedule=None)})

    # Create mock jobs dictionary for ScheduleCreator
    mock_job = "mock_job"
    named_jobs = {"job_without_schedule": mock_job}

    schedule_creator = ScheduleCreator(dagster_config=mock_config, named_jobs=named_jobs)
    schedules = schedule_creator.create_schedules()

    # Should create no schedules
    assert len(schedules) == 0
    assert "job_without_schedule" not in schedules


def test_executor_creator_with_job_inline_executors():
    """Test ExecutorCreator handling inline executor configurations in job definitions."""

    inline_executor = InProcessExecutorOptions()

    # Create a mock config with no global executors
    mock_config = MockConfig(executors={}, jobs={"job_with_inline_executor": MockJob(executor=inline_executor)})

    executor_creator = ExecutorCreator(dagster_config=mock_config)
    executors = executor_creator.create_executors()

    # Should create executor with job-based name
    assert "job_with_inline_executor__executor" in executors
    executor_def = executors["job_with_inline_executor__executor"]
    assert executor_def is not None


def test_executor_creator_validates_job_string_references():
    """Test ExecutorCreator validates job executor string references against available executors."""

    # Create a mock config with global executors
    mock_config = MockConfig(
        executors={"available_executor": InProcessExecutorOptions()},
        jobs={"job_with_bad_ref": MockJob(executor="nonexistent_executor")},
    )

    executor_creator = ExecutorCreator(dagster_config=mock_config)

    # Should raise ValueError for invalid reference
    with pytest.raises(ValueError) as exc_info:
        executor_creator.create_executors()

    assert "Executor named 'nonexistent_executor' for job 'job_with_bad_ref' not found in available executors" in str(
        exc_info.value
    )


def test_executor_creator_mixed_job_executor_types():
    """Test ExecutorCreator handling jobs with mixed executor types (strings and inline configs)."""

    inline_executor = MultiprocessExecutorOptions()

    # Create a mock config with global executors
    mock_config = MockConfig(
        executors={"shared_executor": InProcessExecutorOptions()},
        jobs={
            "job_with_string_ref": MockJob(executor="shared_executor"),
            "job_with_inline": MockJob(executor=inline_executor),
            "job_without_executor": MockJob(executor=None),
        },
    )

    executor_creator = ExecutorCreator(dagster_config=mock_config)
    executors = executor_creator.create_executors()

    # Should have both global executor and job-specific inline executor
    assert "shared_executor" in executors
    assert "job_with_inline__executor" in executors

    # Job without executor should not create an executor
    assert "job_without_executor__executor" not in executors

    # Verify the number of executors
    expected_executor_count = 2  # shared_executor + job_with_inline_executor
    assert len(executors) == expected_executor_count


def test_executor_creator_unsupported_inline_executor_type():
    """Test ExecutorCreator error handling for unsupported inline executor types."""

    # Create an unsupported executor type
    class UnsupportedExecutor:
        pass

    unsupported_executor = UnsupportedExecutor()

    # Create a mock config with unsupported executor
    mock_config = MockConfig(executors={}, jobs={"job_with_unsupported": MockJob(executor=unsupported_executor)})

    executor_creator = ExecutorCreator(dagster_config=mock_config)

    # Should raise ValueError for unsupported type
    with pytest.raises(ValueError) as exc_info:
        executor_creator.create_executors()

    assert "Executor type" in str(exc_info.value)
    assert "not supported" in str(exc_info.value)


def test_executor_creator_handles_none_executor():
    """Test ExecutorCreator properly handles jobs with executor=None."""

    # Create a mock config with job that has no executor
    mock_config = MockConfig(executors={}, jobs={"job_without_executor": MockJob(executor=None)})

    executor_creator = ExecutorCreator(dagster_config=mock_config)
    executors = executor_creator.create_executors()

    # Should create no executors for this job
    assert len(executors) == 0
    assert "job_without_executor__executor" not in executors


class DummyFilter(logging.Filter):
    def __init__(self, keyword: str):
        super().__init__()
        self.keyword = keyword

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - executed via logging
        return self.keyword in record.getMessage()


class DummyFormatter(logging.Formatter):
    def __init__(self, prefix: str, format: str | None = None, datefmt: str | None = None, style: str = "%", **kwargs):
        super().__init__(fmt=format, datefmt=datefmt, style=style, **kwargs)
        self.prefix = prefix

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - executed via logging
        base = super().format(record)
        return f"{self.prefix}:{base}"


def _build_logger_definition(cfg: KedroDagsterConfig, name: str) -> dg.LoggerDefinition:
    creator = LoggerCreator(dagster_config=cfg)
    definitions = creator.create_loggers()
    return definitions[name]


def test_logger_runtime_basic_configuration():
    cfg = KedroDagsterConfig(
        loggers={
            "basic": LoggerOptions(
                log_level="debug",  # lower case to ensure normalization
                handlers=[{"class": "logging.StreamHandler", "level": "DEBUG"}],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "basic")

    # Simulate Dagster calling the logger_fn with proper logger configuration
    logger_config = cfg.loggers["basic"].model_dump()
    context = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(context)

    assert logger_obj.name == "basic"
    assert logger_obj.level == logging.DEBUG
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    # Since no formatter registry was provided, default formatter should be attached
    assert handler.formatter is not None


def test_logger_runtime_formatters_and_filters(tmp_path):
    log_file = tmp_path / "filtered.log"
    cfg = KedroDagsterConfig(
        loggers={
            "rich": LoggerOptions(
                log_level="INFO",
                formatters={
                    "plain": {"format": "%(levelname)s|%(message)s"},
                    "custom": {
                        "()": "tests.test_dagster_creators.DummyFormatter",
                        "prefix": "PFX",
                        "format": "%(message)s",
                    },
                },
                filters={
                    "kw": {"()": "tests.test_dagster_creators.DummyFilter", "keyword": "keep"},
                },
                handlers=[
                    {
                        "class": "logging.FileHandler",
                        "filename": str(log_file),
                        "level": "INFO",
                        "formatter": "plain",
                        "filters": ["kw"],
                    },
                    {
                        # standard stream handler using custom formatter
                        "class": "logging.StreamHandler",
                        "level": "INFO",
                        "formatter": "custom",
                    },
                ],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "rich")
    logger_config = cfg.loggers["rich"].model_dump()
    context = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(context)

    # Validate two handlers attached
    EXPECTED_HANDLER_COUNT = 2
    assert len(logger_obj.handlers) == EXPECTED_HANDLER_COUNT

    # Emit logs
    logger_obj.info("we keep this")
    logger_obj.info("drop me")

    # Ensure file handler flushes to disk
    for h in logger_obj.handlers:
        if isinstance(h, logging.FileHandler):
            h.flush()

    with open(log_file, encoding="utf-8") as fh:
        contents = fh.read().strip().splitlines()

    # Only one line should pass filter
    assert len(contents) == 1
    assert "keep" in contents[0]

    # Cleanup: close handlers to avoid ResourceWarning for unclosed files
    for h in list(logger_obj.handlers):
        try:
            h.close()
        finally:
            logger_obj.removeHandler(h)


def test_logger_runtime_job_inline_logger_isolated_handlers():
    # Inline logger in job should receive unique handler instances even if logger name collides
    base_options = LoggerOptions(log_level="INFO")
    job_inline = LoggerOptions(log_level="ERROR")

    cfg = KedroDagsterConfig(
        loggers={"base": base_options},
        jobs={"job1": JobOptions(pipeline=PipelineOptions(pipeline_name="p"), loggers=[job_inline])},
    )

    creator = LoggerCreator(dagster_config=cfg)
    defs = creator.create_loggers()

    base_def = defs["base"]
    inline_def = defs["job1__logger_0"]

    # Build both actual logger objects
    base_config = base_options.model_dump()
    base_ctx = type("Ctx", (), {"logger_config": base_config})()

    # First creation with base config
    base_logger = base_def.logger_fn(base_ctx)
    assert base_logger.level == logging.INFO
    first_handler_ids = [id(h) for h in base_logger.handlers]

    # Second creation with inline config overrides the same named logger
    inline_config = job_inline.model_dump()
    inline_ctx = type("Ctx", (), {"logger_config": inline_config})()
    inline_logger = inline_def.logger_fn(inline_ctx)
    assert inline_logger.level == logging.ERROR

    # The underlying logger object is the same; handlers should have been replaced
    second_handler_ids = [id(h) for h in inline_logger.handlers]
    assert first_handler_ids != second_handler_ids


def test_logger_runtime_override_context_config():
    # Provide a logger configuration via the InitLoggerContext to override base definition
    cfg = KedroDagsterConfig(
        loggers={
            "override": LoggerOptions(
                log_level="WARNING",
                handlers=[{"class": "logging.StreamHandler", "level": "WARNING"}],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "override")

    # Simulate Dagster providing dynamic config overriding level
    dynamic_conf = {"log_level": "debug"}
    ctx = type("Ctx", (), {"logger_config": dynamic_conf})()
    dyn_logger = logger_def.logger_fn(ctx)

    assert dyn_logger.name == "override"
    assert dyn_logger.level == logging.DEBUG

    # When context provides no handlers, a default StreamHandler is attached at the new log level
    assert len(dyn_logger.handlers) == 1
    assert isinstance(dyn_logger.handlers[0], logging.StreamHandler)
    assert dyn_logger.handlers[0].level == logging.DEBUG


def test_logger_runtime_default_handler_when_none_specified():
    cfg = KedroDagsterConfig(
        loggers={
            "default_handler": LoggerOptions(
                log_level="INFO",
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "default_handler")
    logger_config = cfg.loggers["default_handler"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should attach exactly one default StreamHandler with a formatter
    assert len(logger_obj.handlers) == 1
    assert isinstance(logger_obj.handlers[0], logging.StreamHandler)
    assert logger_obj.handlers[0].formatter is not None


@pytest.mark.parametrize(
    "level",
    ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
)
def test_logger_runtime_all_levels(level):
    cfg = KedroDagsterConfig(
        loggers={
            "lv": LoggerOptions(
                log_level=level,
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "lv")
    logger_config = cfg.loggers["lv"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)
    assert logger_obj.level == getattr(logging, level)


def test_logger_runtime_filter_class_path(tmp_path):
    """Filter instantiation via 'class' reference with direct kwargs should attach and filter records."""
    log_file = tmp_path / "class_filter.log"
    cfg = KedroDagsterConfig(
        loggers={
            "class_filter_logger": LoggerOptions(
                log_level="INFO",
                filters={
                    "kw": {
                        "class": "tests.test_dagster_creators.DummyFilter",
                        "keyword": "keep",
                    }
                },
                handlers=[
                    {
                        "class": "logging.FileHandler",
                        "filename": str(log_file),
                        "level": "INFO",
                        "filters": ["kw"],
                    }
                ],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "class_filter_logger")
    logger_config = cfg.loggers["class_filter_logger"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Ensure handler has the filter attached (branch executed)
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert any(isinstance(f, DummyFilter) for f in handler.filters)

    logger_obj.info("please keep this line")
    logger_obj.info("discard")
    for h in logger_obj.handlers:
        if isinstance(h, logging.FileHandler):
            h.flush()

    contents = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(contents) == 1
    assert "keep" in contents[0]

    # Cleanup: close handlers to avoid ResourceWarning for unclosed files
    for h in list(logger_obj.handlers):
        try:
            h.close()
        finally:
            logger_obj.removeHandler(h)


def test_logger_runtime_handler_callable_path():
    """Handler construction via '()' callable path should work and attach default formatter."""
    cfg = KedroDagsterConfig(
        loggers={
            "callable_handler": LoggerOptions(
                log_level="INFO",
            )
        }
    )

    # Provide context override using '()' style for handler with a custom stream
    stream = io.StringIO()
    override = {
        "log_level": "INFO",
        "handlers": [
            {
                "()": "logging.StreamHandler",
                "stream": stream,
            }
        ],
    }

    logger_def = _build_logger_definition(cfg, "callable_handler")
    ctx = type("Ctx", (), {"logger_config": override})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have exactly one StreamHandler with our stream and a default formatter
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert getattr(handler, "stream", None) is stream
    assert handler.formatter is not None

    logger_obj.info("hello world")
    handler.flush()
    assert "hello world" in stream.getvalue()


def test_logger_reference_invalid_typeerror():
    """Non-string reference for '()' should raise TypeError in _resolve_reference."""
    cfg = KedroDagsterConfig(loggers={"bad": LoggerOptions(log_level="INFO")})
    logger_def = _build_logger_definition(cfg, "bad")
    # Supply invalid non-string reference via context override to bypass Pydantic validation
    override = {"handlers": [{"()": 123}]}
    ctx = type("Ctx", (), {"logger_config": override})()
    with pytest.raises(TypeError):
        logger_def.logger_fn(ctx)


def test_logger_formatter_class_key_support():
    """Test that formatters can be created using 'class' key (new feature from git diff)."""
    cfg = KedroDagsterConfig(
        loggers={
            "class_formatter": LoggerOptions(
                log_level="INFO",
                formatters={
                    "custom": {
                        "class": "tests.test_dagster_creators.DummyFormatter",
                        "prefix": "TEST",
                        "format": "%(message)s",
                    }
                },
                handlers=[{"class": "logging.StreamHandler", "level": "INFO", "formatter": "custom"}],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "class_formatter")
    logger_config = cfg.loggers["class_formatter"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one handler with the custom formatter
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler.formatter, DummyFormatter)
    assert handler.formatter.prefix == "TEST"


def test_logger_filter_params_backward_compatibility():
    """Test that filters support direct kwargs."""
    cfg = KedroDagsterConfig(
        loggers={
            "filter_params": LoggerOptions(
                log_level="INFO",
                filters={
                    "test_filter": {
                        "class": "tests.test_dagster_creators.DummyFilter",
                        "keyword": "compatible",
                    }
                },
                handlers=[{"class": "logging.StreamHandler", "level": "INFO", "filters": ["test_filter"]}],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "filter_params")
    logger_config = cfg.loggers["filter_params"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one handler with the filter attached
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert len(handler.filters) == 1
    assert isinstance(handler.filters[0], DummyFilter)
    assert handler.filters[0].keyword == "compatible"


def test_logger_filter_direct_kwargs_support():
    """Test that filters support direct kwargs (new feature from git diff)."""
    cfg = KedroDagsterConfig(
        loggers={
            "filter_kwargs": LoggerOptions(
                log_level="INFO",
                filters={
                    "test_filter": {
                        "class": "tests.test_dagster_creators.DummyFilter",
                        "keyword": "direct",
                    }
                },
                handlers=[{"class": "logging.StreamHandler", "level": "INFO", "filters": ["test_filter"]}],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "filter_kwargs")
    logger_config = cfg.loggers["filter_kwargs"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one handler with the filter attached
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert len(handler.filters) == 1
    assert isinstance(handler.filters[0], DummyFilter)
    assert handler.filters[0].keyword == "direct"


def test_logger_handler_args_kwargs_support(tmp_path):
    """Test that handlers support direct kwargs."""
    log_file = tmp_path / "args_test.log"
    cfg = KedroDagsterConfig(
        loggers={
            "handler_args": LoggerOptions(
                log_level="INFO",
                handlers=[
                    {
                        "class": "logging.FileHandler",
                        "filename": str(log_file),
                        "mode": "w",
                        "level": "INFO",
                    }
                ],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "handler_args")
    logger_config = cfg.loggers["handler_args"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one FileHandler
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler, logging.FileHandler)

    # Test that it works by logging a message
    logger_obj.info("test message")
    handler.flush()
    assert log_file.exists()
    assert "test message" in log_file.read_text()

    # Cleanup: close handler to avoid ResourceWarning
    handler.close()
    logger_obj.removeHandler(handler)


def test_logger_handler_direct_kwargs_support(tmp_path):
    """Test that handlers support direct kwargs (new feature from git diff)."""
    log_file = tmp_path / "direct_kwargs_test.log"
    cfg = KedroDagsterConfig(
        loggers={
            "handler_direct": LoggerOptions(
                log_level="INFO",
                handlers=[
                    {
                        "class": "logging.FileHandler",
                        "filename": str(log_file),
                        "mode": "w",
                        "level": "INFO",
                    }
                ],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "handler_direct")
    logger_config = cfg.loggers["handler_direct"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one FileHandler
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler, logging.FileHandler)

    # Test that it works by logging a message
    logger_obj.info("direct kwargs test")
    handler.flush()
    assert log_file.exists()
    assert "direct kwargs test" in log_file.read_text()

    # Cleanup: close handler to avoid ResourceWarning
    handler.close()
    logger_obj.removeHandler(handler)


def test_logger_handler_mixed_args_kwargs_direct():
    """Test that handlers support direct kwargs."""
    cfg = KedroDagsterConfig(
        loggers={
            "handler_mixed": LoggerOptions(
                log_level="INFO",
                handlers=[
                    {
                        "class": "logging.StreamHandler",
                        "stream": io.StringIO(),
                        "level": "DEBUG",
                    }
                ],
            )
        }
    )

    logger_def = _build_logger_definition(cfg, "handler_mixed")
    logger_config = cfg.loggers["handler_mixed"].model_dump()
    ctx = type("Ctx", (), {"logger_config": logger_config})()
    logger_obj = logger_def.logger_fn(ctx)

    # Should have one StreamHandler
    assert len(logger_obj.handlers) == 1
    handler = logger_obj.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    # Handler level should be set from the level specified
    assert handler.level == logging.DEBUG

    # Test that it works by logging a message
    logger_obj.debug("mixed params test")
    # Can't easily verify stream content due to Dagster internals, but handler creation worked
