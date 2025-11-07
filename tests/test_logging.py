import importlib
import logging as std_logging
import types


def _import_module():
    # Import fresh to avoid cached monkeypatched dagster behavior between tests
    return importlib.import_module("kedro_dagster.logging")


def test_getLogger_falls_back_to_stdlib_when_no_dagster_context(monkeypatch):
    kd_logging = _import_module()

    # Ensure OpExecutionContext.get() returns None (no active Dagster run)
    monkeypatch.setattr(kd_logging.dg.OpExecutionContext, "get", staticmethod(lambda: None), raising=True)

    logger = kd_logging.getLogger(__name__)

    assert isinstance(logger, std_logging.Logger)
    # Should be a standard logger instance
    assert logger.name == __name__


def test_getLogger_uses_dagster_logger_when_context_active(monkeypatch):
    kd_logging = _import_module()

    # Pretend an active context exists
    monkeypatch.setattr(kd_logging.dg.OpExecutionContext, "get", staticmethod(lambda: object()), raising=True)

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


def test_reexports_expose_stdlib_and_preserve_override():
    kd_logging = _import_module()

    # Re-exported names like INFO/basicConfig should exist
    assert hasattr(kd_logging, "INFO")
    assert isinstance(kd_logging.INFO, int)

    assert hasattr(kd_logging, "basicConfig")
    assert isinstance(kd_logging.basicConfig, types.BuiltinFunctionType) or callable(kd_logging.basicConfig)

    # Our getLogger should be the module's override, not stdlib's
    assert kd_logging.getLogger is not std_logging.getLogger
