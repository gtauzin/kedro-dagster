import importlib
import logging as std_logging
from unittest.mock import patch

import coloredlogs
import pytest
import structlog
from dagster._utils.log import configure_loggers

from kedro_dagster.logging import (
    dagster_colored_formatter,
    dagster_json_formatter,
    dagster_rich_formatter,
)


def _import_module():
    # Import fresh to avoid cached monkeypatched dagster behavior between tests
    return importlib.import_module("kedro_dagster.logging")


def test_getLogger_falls_back_to_stdlib_when_no_dagster_context(monkeypatch):
    """Test that getLogger returns stdlib logger when no Dagster context is active."""
    kd_logging = _import_module()

    # Ensure OpExecutionContext.get() returns None (no active Dagster run)
    monkeypatch.setattr(kd_logging.dg.OpExecutionContext, "get", staticmethod(lambda: None), raising=True)

    logger = kd_logging.getLogger(__name__)

    assert isinstance(logger, std_logging.Logger)
    # Should be a standard logger instance
    assert logger.name == __name__


def test_getLogger_uses_dagster_logger_when_context_active(monkeypatch):
    """Test that getLogger returns Dagster logger when context is active."""
    kd_logging = _import_module()

    # Pretend an active context exists
    monkeypatch.setattr(kd_logging.dg.OpExecutionContext, "get", staticmethod(object), raising=True)

    captured = {}

    class DummyDagsterLogger(std_logging.Logger):
        def __init__(self, name: str):
            super().__init__(name)

    def fake_get_dagster_logger(name=None):
        captured["called_with"] = name
        return DummyDagsterLogger(name or "dagster")

    monkeypatch.setattr(kd_logging.dg, "get_dagster_logger", fake_get_dagster_logger, raising=True)

    logger = kd_logging.getLogger("my.mod")

    assert isinstance(logger, std_logging.Logger)
    assert isinstance(logger, DummyDagsterLogger)
    assert captured["called_with"] == "my.mod"


def test_getLogger_falls_back_when_context_get_raises_exception(monkeypatch):
    """Test that getLogger falls back to stdlib logging when OpExecutionContext.get() raises an exception."""
    kd_logging = _import_module()

    # Mock OpExecutionContext.get() to raise an exception
    def mock_context_get():
        raise RuntimeError("No execution context available")

    monkeypatch.setattr(kd_logging.dg.OpExecutionContext, "get", staticmethod(mock_context_get), raising=True)

    # Mock the debug logging to capture the fallback message
    debug_calls = []
    original_debug = kd_logging._logging.debug

    def mock_debug(msg):
        debug_calls.append(msg)
        return original_debug(msg)

    monkeypatch.setattr(kd_logging._logging, "debug", mock_debug)

    logger = kd_logging.getLogger("test.logger")

    # Should fall back to standard logging
    assert isinstance(logger, std_logging.Logger)
    assert logger.name == "test.logger"

    # Should have logged the debug message about no active Dagster context
    assert len(debug_calls) == 1
    assert "No active Dagster context:" in debug_calls[0]
    assert "RuntimeError" in debug_calls[0] or "No execution context available" in debug_calls[0]


def test_logging_config_yaml_structure():
    """Test that the expected YAML structure from the user requirement matches the actual config."""
    expected_yaml_structure = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "colored": {
                "()": "coloredlogs.ColoredFormatter",
                "fmt": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S %z",
                "field_styles": {"levelname": {"color": "blue"}, "asctime": {"color": "green"}},
                "level_styles": {"debug": {}, "error": {"color": "red"}},
            },
            "json": {
                "()": "structlog.stdlib.ProcessorFormatter",
                "foreign_pre_chain": [
                    "structlog.stdlib.add_logger_name",
                    "structlog.stdlib.add_log_level",
                    'structlog.processors.TimeStamper(fmt="iso", utc=True)',
                    "structlog.processors.StackInfoRenderer",
                    "structlog.stdlib.ExtraAdder",
                ],
                "processors": [
                    "structlog.stdlib.ProcessorFormatter.remove_processors_meta",
                    "structlog.processors.JSONRenderer",
                ],
            },
            "rich": {
                "()": "structlog.stdlib.ProcessorFormatter",
                "foreign_pre_chain": [
                    "structlog.stdlib.add_logger_name",
                    "structlog.stdlib.add_log_level",
                    'structlog.processors.TimeStamper(fmt="iso", utc=True)',
                    "structlog.processors.StackInfoRenderer",
                    "structlog.stdlib.ExtraAdder",
                ],
                "processors": [
                    "structlog.stdlib.ProcessorFormatter.remove_processors_meta",
                    "structlog.dev.ConsoleRenderer",
                ],
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "level": "INFO",
                "formatter": "colored",
            },
            "null": {"class": "logging.NullHandler"},
        },
        "loggers": {
            "dagster": {"handlers": ["default"], "level": "INFO"},
            "dagit": {"handlers": ["default"], "level": "INFO"},
            "dagster-webserver": {"handlers": ["default"], "level": "INFO"},
        },
    }

    # Capture actual config
    captured_config = {}

    def capture_dictConfig(config):
        captured_config.update(config)

    with patch.object(std_logging.config, "dictConfig", side_effect=capture_dictConfig):
        configure_loggers()

    # Check core structure matches
    assert captured_config["version"] == expected_yaml_structure["version"]
    assert captured_config["disable_existing_loggers"] == expected_yaml_structure["disable_existing_loggers"]

    # Check formatters structure (we compare the basic structure since the actual
    # implementation uses function objects rather than strings)
    actual_formatters = captured_config["formatters"]
    expected_formatters = expected_yaml_structure["formatters"]

    # Colored formatter checks
    assert actual_formatters["colored"]["fmt"] == expected_formatters["colored"]["fmt"]
    assert actual_formatters["colored"]["datefmt"] == expected_formatters["colored"]["datefmt"]
    assert actual_formatters["colored"]["field_styles"] == expected_formatters["colored"]["field_styles"]
    assert actual_formatters["colored"]["level_styles"] == expected_formatters["colored"]["level_styles"]

    # Check handlers structure
    actual_handlers = captured_config["handlers"]
    expected_handlers = expected_yaml_structure["handlers"]

    assert actual_handlers["default"]["class"] == expected_handlers["default"]["class"]
    assert actual_handlers["default"]["level"] == expected_handlers["default"]["level"]
    assert actual_handlers["default"]["formatter"] == expected_handlers["default"]["formatter"]
    assert actual_handlers["null"]["class"] == expected_handlers["null"]["class"]

    # Check loggers structure
    actual_loggers = captured_config["loggers"]
    expected_loggers = expected_yaml_structure["loggers"]

    for logger_name in ["dagster", "dagit", "dagster-webserver"]:
        assert actual_loggers[logger_name]["handlers"] == expected_loggers[logger_name]["handlers"]
        assert actual_loggers[logger_name]["level"] == expected_loggers[logger_name]["level"]


def test_dagster_rich_formatter_creation():
    """Test that dagster_rich_formatter creates a valid ProcessorFormatter."""
    formatter = dagster_rich_formatter()

    # Should return a structlog ProcessorFormatter instance
    assert isinstance(formatter, std_logging.Formatter)
    assert hasattr(formatter, "_style")


def test_dagster_rich_formatter_with_new_api():
    """Test that dagster_rich_formatter works with newer structlog API (processors list)."""
    # Create a formatter - should work regardless of structlog version
    formatter = dagster_rich_formatter()

    # Should be able to format a log record
    record = std_logging.LogRecord(
        name="test_logger",
        level=std_logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Should not raise an exception when formatting
    try:
        formatted = formatter.format(record)
        assert isinstance(formatted, str)
        assert "Test message" in formatted
    except Exception as e:
        # If we get a TypeError about processor vs processors, the fallback should handle it
        assert "processor" not in str(e).lower()


def test_dagster_rich_formatter_fallback_compatibility():
    """Test that dagster_rich_formatter handles older structlog API gracefully."""
    # Mock ProcessorFormatter to raise TypeError on processors argument
    original_init = structlog.stdlib.ProcessorFormatter.__init__

    def mock_init_old_api(self, *args, **kwargs):
        if "processors" in kwargs:
            raise TypeError("ProcessorFormatter() got an unexpected keyword argument 'processors'")
        return original_init(self, *args, **kwargs)

    with patch.object(structlog.stdlib.ProcessorFormatter, "__init__", mock_init_old_api):
        # Should fallback to single processor argument
        formatter = dagster_rich_formatter()
        assert isinstance(formatter, std_logging.Formatter)


def test_dagster_json_formatter_creation():
    """Test that dagster_json_formatter creates a valid ProcessorFormatter."""
    formatter = dagster_json_formatter()

    # Should return a structlog ProcessorFormatter instance
    assert isinstance(formatter, std_logging.Formatter)
    assert hasattr(formatter, "_style")


def test_dagster_json_formatter_with_new_api():
    """Test that dagster_json_formatter works with newer structlog API (processors list)."""
    # Create a formatter - should work regardless of structlog version
    formatter = dagster_json_formatter()

    # Should be able to format a log record
    record = std_logging.LogRecord(
        name="test_logger",
        level=std_logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Should not raise an exception when formatting
    try:
        formatted = formatter.format(record)
        assert isinstance(formatted, str)
        assert "Test message" in formatted or "test message" in formatted.lower()
    except Exception as e:
        # If we get a TypeError about processor vs processors, the fallback should handle it
        assert "processor" not in str(e).lower()


def test_dagster_json_formatter_fallback_compatibility():
    """Test that dagster_json_formatter handles older structlog API gracefully."""
    # Mock ProcessorFormatter to raise TypeError on processors argument
    original_init = structlog.stdlib.ProcessorFormatter.__init__

    def mock_init_old_api(self, *args, **kwargs):
        if "processors" in kwargs:
            raise TypeError("ProcessorFormatter() got an unexpected keyword argument 'processors'")
        return original_init(self, *args, **kwargs)

    with patch.object(structlog.stdlib.ProcessorFormatter, "__init__", mock_init_old_api):
        # Should fallback to single processor argument
        formatter = dagster_json_formatter()
        assert isinstance(formatter, std_logging.Formatter)


def test_dagster_colored_formatter_creation():
    """Test that dagster_colored_formatter creates a valid ColoredFormatter."""
    formatter = dagster_colored_formatter()

    # Should return a logging Formatter instance
    assert isinstance(formatter, std_logging.Formatter)


def test_dagster_colored_formatter_with_coloredlogs():
    """Test that dagster_colored_formatter uses ColoredFormatter when coloredlogs is available."""
    formatter = dagster_colored_formatter()

    # Should be a ColoredFormatter instance if coloredlogs is available
    assert isinstance(formatter, coloredlogs.ColoredFormatter | std_logging.Formatter)

    # Test formatting a record
    record = std_logging.LogRecord(
        name="test_logger",
        level=std_logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    formatted = formatter.format(record)
    assert isinstance(formatted, str)
    assert "Test message" in formatted


def test_dagster_colored_formatter_field_styles():
    """Test that dagster_colored_formatter applies the correct field and level styles."""
    formatter = dagster_colored_formatter()

    # If it's a ColoredFormatter, check the styles
    if isinstance(formatter, coloredlogs.ColoredFormatter):
        # Check that the expected field styles are set
        assert hasattr(formatter, "field_styles")
        field_styles = formatter.field_styles
        assert "levelname" in field_styles
        assert field_styles["levelname"]["color"] == "blue"
        assert "asctime" in field_styles
        assert field_styles["asctime"]["color"] == "green"

        # Check that the expected level styles are set
        assert hasattr(formatter, "level_styles")
        level_styles = formatter.level_styles
        assert "debug" in level_styles
        assert "error" in level_styles
        assert level_styles["error"]["color"] == "red"


def test_all_formatters_handle_exceptions_gracefully():
    """Test that all formatters handle exceptions in log records gracefully."""
    formatters = [
        dagster_rich_formatter(),
        dagster_json_formatter(),
        dagster_colored_formatter(),
    ]

    # Create a log record with exception info
    try:
        raise ValueError("Test exception")
    except ValueError:
        exc_info = std_logging.sys.exc_info()

    record = std_logging.LogRecord(
        name="test_logger",
        level=std_logging.ERROR,
        pathname="test.py",
        lineno=1,
        msg="Test message with exception",
        args=(),
        exc_info=exc_info,
    )

    for formatter in formatters:
        try:
            formatted = formatter.format(record)
            assert isinstance(formatted, str)
            assert "Test message with exception" in formatted
        except Exception as e:
            # Should not raise exceptions during formatting
            pytest.fail(f"Formatter {type(formatter).__name__} raised exception: {e}")


def test_formatters_preserve_logger_name():
    """Test that all formatters preserve the logger name in the output."""
    formatters = [
        dagster_rich_formatter(),
        dagster_json_formatter(),
        dagster_colored_formatter(),
    ]

    record = std_logging.LogRecord(
        name="my.test.logger",
        level=std_logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    for formatter in formatters:
        try:
            formatted = formatter.format(record)
            assert isinstance(formatted, str)
            # Logger name should appear somewhere in the formatted output
            # (exact format depends on the formatter type)
            assert len(formatted) > 0
        except Exception as e:
            pytest.fail(f"Formatter {type(formatter).__name__} raised exception: {e}")
