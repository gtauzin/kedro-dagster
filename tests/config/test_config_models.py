# mypy: ignore-errors

import pytest
from pydantic import ValidationError

from kedro_dagster.config.automation import ScheduleOptions
from kedro_dagster.config.execution import (
    CeleryDockerExecutorOptions,
    CeleryExecutorOptions,
    CeleryK8sJobExecutorOptions,
    DockerExecutorOptions,
    InProcessExecutorOptions,
    K8sJobExecutorOptions,
    MultiprocessExecutorOptions,
)
from kedro_dagster.config.job import JobOptions, PipelineOptions
from kedro_dagster.config.kedro_dagster import KedroDagsterConfig
from kedro_dagster.config.logging import LoggerOptions
from kedro_dagster.utils import KEDRO_VERSION


def test_schedule_options_happy_path():
    """ScheduleOptions accepts minimal fields and optional metadata/timezone."""
    s = ScheduleOptions(cron_schedule="*/5 * * * *", description="every 5m")
    assert s.cron_schedule.startswith("*/5")
    assert s.execution_timezone is None
    assert s.metadata is None


def test_pipeline_options_forbid_extra_and_defaults():
    """PipelineOptions defaults to None for filters and forbids unknown fields."""
    p = PipelineOptions()
    assert p.pipeline_name is None
    assert p.from_nodes is None
    assert p.to_nodes is None
    assert p.node_names is None
    assert p.from_inputs is None
    assert p.to_outputs is None
    assert p.tags is None
    if KEDRO_VERSION[0] >= 1:
        assert p.node_namespaces is None
    else:
        assert p.node_namespace is None

    with pytest.raises(ValidationError):
        PipelineOptions(unknown="x")


def test_pipeline_options_node_namespaces_list_shape():
    """For Kedro >= 1.0, node_namespaces accepts and exposes list[str]; alias property matches."""
    if KEDRO_VERSION[0] < 1:
        pytest.skip("Only relevant for Kedro >= 1.0")

    p = PipelineOptions(node_namespaces=["ns1", "ns2"])
    assert p.node_namespaces == ["ns1", "ns2"]
    assert not hasattr(p, "node_namespace")


def test_job_options_requires_pipeline_and_forbid_extra():
    """JobOptions require a PipelineOptions instance and reject extra fields."""
    with pytest.raises(ValidationError):
        JobOptions()

    job = JobOptions(pipeline=PipelineOptions())
    assert isinstance(job.pipeline, PipelineOptions)

    with pytest.raises(ValidationError):
        JobOptions(pipeline=PipelineOptions(), extra_field=1)


def test_inprocess_and_multiprocess_executor_defaults():
    """Executor option defaults include retries for in_process and max_concurrent for multiprocess."""
    inproc = InProcessExecutorOptions()
    # default retries enabled structure
    assert hasattr(inproc.retries, "enabled") or hasattr(inproc.retries, "disabled")

    multi = MultiprocessExecutorOptions()
    assert multi.max_concurrent == 1


def test_docker_executor_defaults_and_mutability():
    """DockerExecutorOptions default to empty lists and optional container kwargs."""
    d = DockerExecutorOptions()
    assert d.env_vars == []
    assert d.networks == []
    assert d.container_kwargs is None


def test_k8s_executor_defaults_subset():
    """K8sJobExecutorOptions default namespace, labels, volumes, and metadata shape."""
    k = K8sJobExecutorOptions()
    assert k.job_namespace == "dagster"
    assert isinstance(k.step_k8s_config.job_metadata, dict)
    assert k.labels == {}
    assert k.volumes == []


def test_celery_and_combined_executors_construct():
    """Celery-based executor option classes can be instantiated without arguments."""
    CeleryExecutorOptions()
    CeleryDockerExecutorOptions()
    CeleryK8sJobExecutorOptions()


def test_kedrodagster_config_parses_executors_map_happy_path():
    """KedroDagsterConfig parses executors mapping into strongly-typed option classes."""
    cfg = KedroDagsterConfig(
        executors={
            "local": {"in_process": {}},
            "multi": {"multiprocess": {"max_concurrent": 3}},
            "dock": {"docker_executor": {"image": "alpine"}},
            "k8s": {"k8s_job_executor": {"job_namespace": "prod"}},
        }
    )

    assert isinstance(cfg.executors, dict)
    assert isinstance(cfg.executors["local"], InProcessExecutorOptions)
    assert isinstance(cfg.executors["multi"], MultiprocessExecutorOptions)
    MAX_CONCURRENCY = 3
    assert cfg.executors["multi"].max_concurrent == MAX_CONCURRENCY
    assert isinstance(cfg.executors["dock"], DockerExecutorOptions)
    assert isinstance(cfg.executors["k8s"], K8sJobExecutorOptions)


def test_kedrodagster_config_unknown_executor_raises():
    """Unknown executor identifiers result in a ValueError during parsing."""
    with pytest.raises(ValueError):
        KedroDagsterConfig(executors={"weird": {"unknown": {}}})


def test_logger_options_minimal_config():
    """LoggerOptions accepts minimal configuration with logger_name only."""
    logger_opts = LoggerOptions(logger_name="test.logger")

    assert logger_opts.logger_name == "test.logger"
    assert logger_opts.log_level == "INFO"  # default
    assert logger_opts.handlers is None
    assert logger_opts.formatters is None
    assert logger_opts.filters is None


def test_logger_options_case_insensitive_log_levels():
    """LoggerOptions accepts log levels in any case and normalizes to uppercase."""
    test_cases = [
        ("info", "INFO"),
        ("DEBUG", "DEBUG"),
        ("Warning", "WARNING"),
        ("error", "ERROR"),
        ("Critical", "CRITICAL"),
        ("notset", "NOTSET"),
    ]

    for input_level, expected_level in test_cases:
        logger_opts = LoggerOptions(logger_name="test.logger", log_level=input_level)
        assert logger_opts.log_level == expected_level


def test_logger_options_log_level_whitespace_handling():
    """LoggerOptions handles whitespace in log levels correctly."""
    test_cases = [
        " info ",
        "\tDEBUG\t",
        "  ERROR  ",
        "\n INFO \n",
    ]

    for input_level in test_cases:
        logger_opts = LoggerOptions(logger_name="test.logger", log_level=input_level)
        # Should normalize to uppercase and strip whitespace
        assert logger_opts.log_level in ["INFO", "DEBUG", "ERROR"]


def test_logger_options_invalid_log_levels():
    """LoggerOptions rejects invalid log levels."""
    invalid_levels = ["INVALID", "trace", "VERBOSE", ""]

    for invalid_level in invalid_levels:
        with pytest.raises(ValidationError) as exc_info:
            LoggerOptions(logger_name="test.logger", log_level=invalid_level)
        assert "Invalid log level" in str(exc_info.value)


def test_logger_options_logger_name_validation():
    """LoggerOptions validates logger names follow Python module naming conventions."""
    # Valid logger names
    valid_names = [
        "logger",
        "my_logger",
        "my.module.logger",
        "app.service.logger",
        "_internal",
        "logger123",
    ]

    for valid_name in valid_names:
        logger_opts = LoggerOptions(logger_name=valid_name)
        assert logger_opts.logger_name == valid_name

    # Invalid logger names
    invalid_names = [
        "",  # empty
        "123invalid",  # starts with number
        "invalid-name",  # contains hyphen
        "invalid name",  # contains space
        "invalid@name",  # contains special char
        ".invalid",  # starts with dot
    ]

    for invalid_name in invalid_names:
        with pytest.raises(ValidationError):
            LoggerOptions(logger_name=invalid_name)


def test_logger_options_handler_validation():
    """LoggerOptions validates handler configurations."""
    # Valid handler
    valid_handler = LoggerOptions(
        logger_name="test.logger", handlers=[{"class": "logging.StreamHandler", "level": "INFO"}]
    )
    assert len(valid_handler.handlers) == 1

    # Handler missing 'class' field
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", handlers=[{"level": "INFO"}])
    assert "must specify a 'class' field" in str(exc_info.value)

    # Handler with non-string class
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", handlers=[{"class": 123}])
    assert "must be a string" in str(exc_info.value)

    # Handler not a dictionary
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", handlers=["invalid"])
    assert "Input should be a valid dictionary" in str(exc_info.value)


def test_logger_options_formatter_validation():
    """LoggerOptions validates formatter configurations."""
    # Valid formatter with 'format' field
    valid_formatter_format = LoggerOptions(logger_name="test.logger", formatters={"simple": {"format": "%(message)s"}})
    assert "simple" in valid_formatter_format.formatters

    # Valid formatter with '()' field (custom class)
    valid_formatter_class = LoggerOptions(
        logger_name="test.logger", formatters={"colored": {"()": "coloredlogs.ColoredFormatter"}}
    )
    assert "colored" in valid_formatter_class.formatters

    # Invalid formatter - no 'format' or '()' field
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", formatters={"invalid": {"other_field": "value"}})
    assert "must specify either 'format' field or '()'" in str(exc_info.value)

    # Formatter not a dictionary
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", formatters={"invalid": "not_a_dict"})
    assert "Input should be a valid dictionary" in str(exc_info.value)


def test_logger_options_filter_validation():
    """LoggerOptions validates filter configurations."""
    # Valid filter
    valid_filter = LoggerOptions(logger_name="test.logger", filters={"level_filter": {"()": "logging.Filter"}})
    assert "level_filter" in valid_filter.filters

    # Filter not a dictionary
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", filters={"invalid": "not_a_dict"})
    assert "Input should be a valid dictionary" in str(exc_info.value)


def test_logger_options_cross_reference_validation():
    """LoggerOptions validates references between handlers, formatters, and filters."""
    # Valid cross-references
    valid_config = LoggerOptions(
        logger_name="test.logger",
        handlers=[{"class": "logging.StreamHandler", "formatter": "detailed", "filters": ["level_filter"]}],
        formatters={"detailed": {"format": "%(asctime)s - %(levelname)s - %(message)s"}},
        filters={"level_filter": {"()": "logging.Filter"}},
    )
    assert valid_config.logger_name == "test.logger"

    # Handler references unknown formatter
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(
            logger_name="test.logger",
            handlers=[{"class": "logging.StreamHandler", "formatter": "unknown_formatter"}],
            formatters={"existing": {"format": "%(message)s"}},
        )
    assert "references unknown formatter 'unknown_formatter'" in str(exc_info.value)
    assert "Available formatters: ['existing']" in str(exc_info.value)

    # Handler references unknown filter
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(
            logger_name="test.logger",
            handlers=[{"class": "logging.StreamHandler", "filters": ["unknown_filter"]}],
            filters={"existing": {"()": "logging.Filter"}},
        )
    assert "references unknown filter 'unknown_filter'" in str(exc_info.value)
    assert "Available filters: ['existing']" in str(exc_info.value)


def test_logger_options_complex_valid_configuration():
    """LoggerOptions handles complex but valid configurations."""
    complex_config = LoggerOptions(
        logger_name="my.application.logger",
        log_level="debug",  # lowercase, should be normalized
        handlers=[
            {"class": "logging.StreamHandler", "level": "INFO", "formatter": "colored", "filters": ["level_filter"]},
            {"class": "logging.FileHandler", "filename": "/tmp/app.log", "level": "ERROR", "formatter": "detailed"},
        ],
        formatters={
            "colored": {"()": "coloredlogs.ColoredFormatter", "fmt": "%(asctime)s - %(levelname)s - %(message)s"},
            "detailed": {"format": "%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s"},
        },
        filters={"level_filter": {"()": "logging.Filter", "name": "my.application"}},
    )

    assert complex_config.logger_name == "my.application.logger"
    assert complex_config.log_level == "DEBUG"  # normalized to uppercase

    # Check expected counts
    EXPECTED_HANDLERS = 2
    EXPECTED_FORMATTERS = 2
    EXPECTED_FILTERS = 1
    assert len(complex_config.handlers) == EXPECTED_HANDLERS
    assert len(complex_config.formatters) == EXPECTED_FORMATTERS
    assert len(complex_config.filters) == EXPECTED_FILTERS

    # Verify handler references are valid
    assert complex_config.handlers[0]["formatter"] == "colored"
    assert complex_config.handlers[1]["formatter"] == "detailed"
    assert "level_filter" in complex_config.handlers[0]["filters"]


def test_logger_options_forbid_extra_fields():
    """LoggerOptions should forbid extra fields due to Pydantic strict config."""
    with pytest.raises(ValidationError) as exc_info:
        LoggerOptions(logger_name="test.logger", log_level="INFO", unknown_field="should_fail")
    # Check that extra inputs are not permitted
    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_logger_options_handler_missing_class():
    with pytest.raises(ValidationError, match="must specify a 'class' field"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.missingclass",
                    log_level="INFO",
                    handlers=[{}],  # missing 'class'
                )
            }
        )


def test_logger_options_handler_class_not_string():
    with pytest.raises(ValidationError, match="must be a string"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.classtype",
                    log_level="INFO",
                    handlers=[{"class": 123}],  # non-string class
                )
            }
        )


def test_logger_options_formatter_missing_keys():
    with pytest.raises(ValidationError, match=r"must specify either 'format' field or '\(\)'"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.formattermissing",
                    log_level="INFO",
                    formatters={"fmt": {"datefmt": "%Y"}},  # missing format and '()'
                )
            }
        )


def test_logger_options_log_level_not_string():
    with pytest.raises(ValidationError, match="Log level must be a string"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.leveltype",
                    log_level=123,  # non-string
                )
            }
        )


def test_logger_options_invalid_log_level_value():
    with pytest.raises(ValidationError, match="Invalid log level"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.levelvalue",
                    log_level="VERBOSE",  # invalid value
                )
            }
        )


def test_logger_options_handler_unknown_formatter_reference():
    with pytest.raises(ValidationError, match="references unknown formatter"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.unknownfmt",
                    log_level="INFO",
                    handlers=[{"class": "logging.StreamHandler", "formatter": "missing"}],
                )
            }
        )


def test_logger_options_handler_unknown_filter_reference():
    with pytest.raises(ValidationError, match="references unknown filter"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.unknownfilter",
                    log_level="INFO",
                    handlers=[{"class": "logging.StreamHandler", "filters": ["missing"]}],
                )
            }
        )


def test_logger_options_handler_not_dict():
    """Handler entry that's not a dict should raise a validation error.

    Matches either custom validator message or Pydantic's generic dict error depending on evaluation order.
    """
    with pytest.raises(ValidationError, match=r"Handler at index 0 must be a dictionary|valid dictionary"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    logger_name="test.bad.handlernotdict",
                    log_level="INFO",
                    handlers=["not-a-dict"],  # type: ignore[list-item]
                )
            }
        )


def test_logger_options_empty_name_validation():
    """LoggerOptions should raise ValidationError when logger_name is empty string.

    Covers the explicit ValueError branch; Pydantic's min_length fires first.
    """
    with pytest.raises(ValidationError, match="at least 1 character"):
        LoggerOptions(logger_name="", log_level="INFO")


def test_logger_options_handler_not_dict_validation():
    """Handlers list containing a non-dict element should raise ValidationError.

    Depending on Pydantic pre-validation, the error may be the generic dict-type message
    or our custom validator message. Match either to keep the test stable across versions.
    """
    with pytest.raises(ValidationError, match=r"Handler at index 0 must be a dictionary|valid dictionary"):
        LoggerOptions(logger_name="test.handlers", log_level="INFO", handlers=["not-a-dict"])  # type: ignore[list-item]
