# mypy: ignore-errors

import warnings

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
from kedro_dagster.utils import KEDRO_VERSION, PYDANTIC_VERSION, create_pydantic_config


def test_schedule_options_happy_path():
    """ScheduleOptions accepts minimal fields and optional metadata/timezone."""
    s = ScheduleOptions(cron_schedule="*/5 * * * *", description="every 5m")
    assert s.cron_schedule.startswith("*/5")
    assert s.execution_timezone is None
    assert s.metadata is None


def test_pipeline_options_forbid_extra_and_defaults():
    """PipelineOptions defaults to None for filters and forbids unknown fields."""
    p = PipelineOptions()
    assert p.pipeline_name == "__default__"
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
    """LoggerOptions accepts minimal configuration without any required fields."""
    logger_opts = LoggerOptions()

    assert logger_opts.log_level == "INFO"  # default
    assert len(logger_opts.handlers) == 0
    assert len(logger_opts.formatters) == 0
    assert len(logger_opts.filters) == 0


def test_logger_options_normalize_log_level_validation():
    """All log level normalization & validation scenarios (normalize_log_level validator)."""
    # Valid normalization (case-insensitive)
    normalization_cases = [
        ("info", "INFO"),
        ("DEBUG", "DEBUG"),
        ("Warning", "WARNING"),
        ("error", "ERROR"),
        ("Critical", "CRITICAL"),
        ("notset", "NOTSET"),
    ]
    for raw, expected in normalization_cases:
        assert LoggerOptions(log_level=raw).log_level == expected

    # Whitespace handling
    whitespace_cases = [" info ", "\tDEBUG\t", "  ERROR  ", "\n INFO \n"]
    for raw in whitespace_cases:
        cleaned = LoggerOptions(log_level=raw).log_level
        assert cleaned in {"INFO", "DEBUG", "ERROR"}

    # Invalid values (string type but not a valid level or empty)
    for invalid in ["INVALID", "trace", "VERBOSE", ""]:
        with pytest.raises(ValidationError, match="Invalid log level"):
            LoggerOptions(log_level=invalid)

    # Non-string type
    with pytest.raises(ValidationError, match="Log level must be a string"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    log_level=123,
                )
            }
        )

    # Invalid value via KedroDagsterConfig container to ensure same behavior
    with pytest.raises(ValidationError, match="Invalid log level"):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    log_level="VERBOSE",
                )
            }
        )


def test_logger_options_validate_handlers():
    """All handler validation scenarios (validate_handlers)."""
    # Happy path
    assert (
        len(
            LoggerOptions(
                handlers=[{"class": "logging.StreamHandler", "level": "INFO"}],
            ).handlers
        )
        == 1
    )

    # Missing 'class'
    with pytest.raises(ValidationError, match="must specify a 'class' field"):
        LoggerOptions(handlers=[{"level": "INFO"}])

    # Non-string class
    with pytest.raises(ValidationError, match="must be a string"):
        LoggerOptions(handlers=[{"class": 123}])

    # Not a dictionary entry
    with pytest.raises(
        ValidationError,
        match=r"Input should be a valid dictionary",
    ):
        KedroDagsterConfig(
            loggers={
                "bad": LoggerOptions(
                    handlers=["not-a-dict"],
                )
            }
        )

    # No handlers is empty list
    assert len(LoggerOptions().handlers) == 0


def test_logger_options_validate_formatters():
    """All formatter validation scenarios (validate_formatters)."""
    # Valid: has 'format'
    fmt_cfg = LoggerOptions(formatters={"simple": {"format": "%(msg)s"}})
    assert "simple" in fmt_cfg.formatters

    # Valid: has '()'
    custom_fmt = LoggerOptions(formatters={"colored": {"()": "coloredlogs.ColoredFormatter"}})
    assert "colored" in custom_fmt.formatters

    # Invalid: missing both keys
    with pytest.raises(ValidationError, match=r"must specify either 'format' field or '\(\)'"):
        KedroDagsterConfig(loggers={"bad": LoggerOptions(formatters={"bad": {"other": "v"}})})

    # Not a dict value
    with pytest.raises(ValidationError, match=r"Input should be a valid dictionary"):
        LoggerOptions(formatters={"bad": "not_a_dict"})

    # Non-string 'format'
    with pytest.raises(ValidationError, match=r"'format' must be a string"):
        LoggerOptions(formatters={"bad": {"format": 123}})

    # Non-string '()'
    with pytest.raises(ValidationError, match=r"'\(\)' must be a string import path"):
        LoggerOptions(formatters={"bad": {"()": 123}})

    # No formatters is empty dict
    assert len(LoggerOptions().formatters) == 0


def test_logger_options_validate_filters():
    """All filter validation scenarios (validate_filters)."""
    # Valid
    f_cfg = LoggerOptions(filters={"level": {"()": "logging.Filter"}})
    assert "level" in f_cfg.filters

    # Valid using class + params
    f_cfg2 = LoggerOptions(
        filters={"by_class": {"class": "logging.Filter", "params": {"name": "my.app"}}},
    )
    assert "by_class" in f_cfg2.filters

    # Not a dict
    with pytest.raises(ValidationError, match=r"Input should be a valid dictionary"):
        LoggerOptions(filters={"bad": "not_a_dict"})

    # Missing both '()' and 'class'
    with pytest.raises(ValidationError, match=r"must specify either '\(\)'.* or 'class'"):
        LoggerOptions(filters={"bad": {"name": "no-type"}})

    # Non-string '()'
    with pytest.raises(ValidationError, match=r"'\(\)' must be a string import path"):
        LoggerOptions(filters={"bad": {"()": 123}})

    # Non-string 'class'
    with pytest.raises(ValidationError, match=r"class must be a string"):
        LoggerOptions(filters={"bad": {"class": 123}})

    # No filters is empty dict
    assert len(LoggerOptions().filters) == 0


def test_logger_options_validate_references():
    """All cross-reference validation scenarios (validate_references)."""
    # Valid cross references
    valid = LoggerOptions(
        handlers=[{"class": "logging.StreamHandler", "formatter": "detailed", "filters": ["level_filter"]}],
        formatters={"detailed": {"format": "%(asctime)s - %(levelname)s - %(message)s"}},
        filters={"level_filter": {"()": "logging.Filter"}},
    )
    assert valid.log_level == "INFO"  # Check default value instead

    # Unknown formatter
    with pytest.raises(ValidationError, match="references unknown formatter 'unknown_formatter'"):
        LoggerOptions(
            handlers=[{"class": "logging.StreamHandler", "formatter": "unknown_formatter"}],
            formatters={"existing": {"format": "%(message)s"}},
        )

    # Unknown filter
    with pytest.raises(ValidationError, match="references unknown filter 'unknown_filter'"):
        LoggerOptions(
            handlers=[{"class": "logging.StreamHandler", "filters": ["unknown_filter"]}],
            filters={"existing": {"()": "logging.Filter"}},
        )


def test_logger_options_complex_valid_configuration():
    """LoggerOptions handles complex but valid configurations."""
    complex_config = LoggerOptions(
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
        LoggerOptions(log_level="INFO", unknown_field="should_fail")
    # Check that extra inputs are not permitted
    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_pydantic_version_detection():
    """PYDANTIC_VERSION should be correctly detected as a tuple of integers."""
    assert isinstance(PYDANTIC_VERSION, tuple)
    assert len(PYDANTIC_VERSION) >= 2
    assert all(isinstance(v, int) for v in PYDANTIC_VERSION)
    assert PYDANTIC_VERSION[0] in [1, 2, 3]  # Should be a valid major version


def test_create_pydantic_config_basic():
    """create_pydantic_config should return appropriate config type."""
    config = create_pydantic_config(extra="forbid")

    if PYDANTIC_VERSION[0] >= 2:
        # Pydantic v2 should return ConfigDict or dict-like object
        assert config is not None
    else:
        # Pydantic v1 should return a Config class
        assert hasattr(config, "__class__") or callable(config)


def test_create_pydantic_config_with_multiple_options():
    """create_pydantic_config should handle multiple config options."""
    config = create_pydantic_config(
        extra="forbid", validate_assignment=True, arbitrary_types_allowed=True, frozen=False
    )

    assert config is not None
    # Just verify we can create config with multiple options without errors


def test_pipeline_options_config_compatibility():
    """PipelineOptions should work with version-aware Pydantic config."""
    # Should work without errors regardless of Pydantic version
    options = PipelineOptions(pipeline_name="test", tags=["tag1"])
    assert options.pipeline_name == "test"
    assert options.tags == ["tag1"]

    # Should still forbid extra fields
    with pytest.raises(ValidationError):
        PipelineOptions(unknown_field="should_fail")


def test_job_options_config_compatibility():
    """JobOptions should work with version-aware Pydantic config."""
    pipeline_opts = PipelineOptions()
    job_opts = JobOptions(pipeline=pipeline_opts)
    assert isinstance(job_opts.pipeline, PipelineOptions)

    # Should still forbid extra fields
    with pytest.raises(ValidationError):
        JobOptions(pipeline=pipeline_opts, unknown_field="should_fail")


def test_kedro_dagster_config_compatibility():
    """KedroDagsterConfig should work with version-aware Pydantic config."""
    config = KedroDagsterConfig(executors={"local": {"in_process": {}}}, loggers={"test": LoggerOptions()})
    assert config.executors is not None
    assert config.loggers is not None

    # Should still forbid extra fields and validate assignment
    with pytest.raises(ValidationError):
        KedroDagsterConfig(unknown_field="should_fail")


def test_logger_options_config_compatibility():
    """LoggerOptions should work with version-aware Pydantic config."""
    logger = LoggerOptions(
        log_level="DEBUG",
        handlers=[{"class": "logging.StreamHandler"}],
        formatters={"simple": {"format": "%(message)s"}},
    )
    assert logger.log_level == "DEBUG"
    assert len(logger.handlers) == 1

    # Should still forbid extra fields
    with pytest.raises(ValidationError):
        LoggerOptions(unknown_field="should_fail")


def test_warning_filtering_context_manager():
    """Test that warnings.catch_warnings context manager works and capture the expected warning."""
    # Test that we can catch and validate the specific warning
    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")  # Capture all warnings

        # This should trigger the warning we want to validate
        warnings.warn(
            "Argument(s) 'run_result' which are declared in the hookspec cannot be found in this hook call", UserWarning
        )

        # Validate that the warning was captured
        assert len(captured_warnings) == 1
        warning = captured_warnings[0]
        assert issubclass(warning.category, UserWarning)
        assert "run_result" in str(warning.message)
        assert "hookspec" in str(warning.message)
        assert "cannot be found in this hook call" in str(warning.message)


def test_existing_pydantic_models_still_work():
    """Ensure that existing Pydantic model usage patterns still work."""
    # Test basic model creation
    options = PipelineOptions()
    assert options.pipeline_name == "__default__"

    # Test validation still works
    with pytest.raises(ValidationError):
        PipelineOptions(invalid_field="value")

    # Test nested models still work
    job = JobOptions(pipeline=options)
    assert job.pipeline == options


def test_config_dict_behavior_consistent():
    """Test that config behavior is consistent across Pydantic versions."""
    config1 = create_pydantic_config(extra="forbid")
    config2 = create_pydantic_config(extra="allow")

    # Both should be valid config objects
    assert config1 is not None
    assert config2 is not None

    # They should be different (different extra behavior)
    # Note: Can't easily compare ConfigDict objects, but they should at least be different instances
    assert config1 is not config2


def test_version_specific_imports_work():
    """Test that version-specific imports in create_pydantic_config work."""
    # This should not raise ImportError regardless of Pydantic version
    config = create_pydantic_config(arbitrary_types_allowed=True)
    assert config is not None

    if PYDANTIC_VERSION[0] >= 2:
        # Test that ConfigDict works for v2 - we can test the function works
        # without importing ConfigDict locally (avoiding linter issues)
        config_v2 = create_pydantic_config(extra="forbid")
        assert config_v2 is not None


def test_pydantic_version_conditional_logic():
    """Test that PYDANTIC_VERSION[0] >= 2 logic works correctly."""
    # This mirrors the logic added to multiple config classes
    if PYDANTIC_VERSION[0] >= 2:  # noqa: PLR2004
        model_config = create_pydantic_config(extra="forbid")
        assert model_config is not None
    else:
        Config = create_pydantic_config(extra="forbid")  # noqa: N806
        assert Config is not None

    # Both paths should work without errors


def test_config_class_vs_model_config_pattern():
    """Test the pattern used in multiple config classes."""
    # Test the specific pattern used in PipelineOptions, JobOptions, etc.
    config = create_pydantic_config(validate_assignment=True, extra="forbid")
    assert config is not None

    # This mirrors the actual usage in the codebase
    if PYDANTIC_VERSION[0] >= 2:  # noqa: PLR2004
        # In v2, this would be: model_config = create_pydantic_config(...)
        model_config = config  # noqa: F841
    else:
        # In v1, this would be: Config = create_pydantic_config(...)
        Config = config  # noqa: F841, N806

    # Both should work without errors
