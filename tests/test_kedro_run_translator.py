# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.kedro import KedroRunTranslator


class _FakeHook:
    def __init__(self) -> None:
        self.after_context_created_called_with: list[Any] = []
        self.on_pipeline_error_called_with: list[dict[str, Any]] = []

    # signature used in kedro.py
    def after_context_created(self, *, context: Any) -> None:  # noqa: D401
        self.after_context_created_called_with.append(context)

    def on_pipeline_error(self, *, error: Exception, run_params: dict[str, Any], pipeline: Any, catalog: Any) -> None:
        self.on_pipeline_error_called_with.append({
            "error": error,
            "run_params": run_params,
            "pipeline": pipeline,
            "catalog": catalog,
        })


class _FakeHookManager:
    def __init__(self) -> None:
        self.hook = _FakeHook()


@pytest.fixture()
def kedro_context_base(kedro_project_exec_filebacked_base) -> Any:
    """Create and return a real Kedro context for the base exec_filebacked scenario."""
    options = kedro_project_exec_filebacked_base
    project_path = str(options.project_path)
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=options.env)
    return session.load_context()


def test_to_dagster_creates_resource_and_merges_params(
    kedro_context_base: Any, kedro_project_exec_filebacked_base, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resource contains merged Kedro defaults, pipeline name, and filter params."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        project_path=str(options.project_path),
        env=options.env,
        session_id="sid-123",
    )

    resource = translator.to_dagster(
        pipeline_name="__default__",
        filter_params={
            "tags": ["a", "b"],
            "from_nodes": ["n1"],
            "to_nodes": None,
            "node_names": ["task"],
        },
    )

    # run_params include kedro params + pipeline name + defaults
    params = resource.run_params
    assert params["project_path"] == str(options.project_path)
    assert params["env"] == options.env
    assert params["session_id"] == "sid-123"
    assert params["pipeline_name"] == "__default__"
    # defaults set in to_dagster
    assert params["load_versions"] is None
    assert params["extra_params"] is None
    assert params["runner"] is None
    # filter values carried through
    assert params["tags"] == ["a", "b"]
    assert params["from_nodes"] == ["n1"]
    assert params["node_names"] == ["task"]


def test_resource_pipeline_filters_via_registry(
    kedro_context_base: Any, kedro_project_exec_filebacked_base, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resource.pipeline delegates to Kedro registry and calls filter with provided args."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        project_path=str(options.project_path),
        env=options.env,
        session_id="sid-xyz",
    )

    # Capture filter arguments received by the dummy pipeline
    captured: dict[str, Any] = {}

    class _DummyPipeline:
        def filter(
            self,
            *,
            tags=None,
            from_nodes=None,
            to_nodes=None,
            node_names=None,
            from_inputs=None,
            to_outputs=None,
            node_namespace=None,
        ) -> dict[str, Any]:
            captured.update(
                dict(
                    tags=tags,
                    from_nodes=from_nodes,
                    to_nodes=to_nodes,
                    node_names=node_names,
                    from_inputs=from_inputs,
                    to_outputs=to_outputs,
                    node_namespace=node_namespace,
                )
            )
            return {"ok": True}

    # Monkeypatch the Kedro pipelines registry getter used in kedro.py
    monkeypatch.setattr(pipelines, "get", lambda name: _DummyPipeline())

    resource = translator.to_dagster(
        pipeline_name="my_pipeline",
        filter_params={
            "tags": ["x"],
            "from_nodes": ["A"],
            "to_outputs": ["out"],
            "node_namespace": "ns",
        },
    )

    # Accessing the pipeline triggers the filter call with parameters
    pipe = resource.pipeline
    assert pipe == {"ok": True}
    assert captured == {
        "tags": ["x"],
        "from_nodes": ["A"],
        "to_nodes": None,
        "node_names": None,
        "from_inputs": None,
        "to_outputs": ["out"],
        "node_namespace": "ns",
    }


def test_after_context_created_hook_invokes_hook_manager(
    kedro_context_base: Any, kedro_project_exec_filebacked_base
) -> None:
    """after_context_created_hook triggers the Kedro hook with the current context."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        project_path=str(options.project_path),
        env=options.env,
        session_id="sid-123",
    )
    # Install fake hook manager BEFORE resource creation so the closure captures it
    fake_hook_mgr = _FakeHookManager()
    translator._context._hook_manager = fake_hook_mgr  # type: ignore[attr-defined]
    translator._hook_manager = fake_hook_mgr  # also update translator cache used in closure
    resource = translator.to_dagster(pipeline_name="__default__", filter_params={})

    # Call the hook and ensure the underlying kedro hook was triggered
    resource.after_context_created_hook()

    fake_ctx = translator._context  # type: ignore[attr-defined]
    assert fake_ctx._hook_manager.hook.after_context_created_called_with == [fake_ctx]


def test_translate_on_pipeline_error_hook_returns_sensor(
    kedro_context_base: Any, kedro_project_exec_filebacked_base, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_translate_on_pipeline_error_hook returns a sensor with expected metadata."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        project_path=str(options.project_path),
        env=options.env,
        session_id="sid-123",
    )

    # Provide a minimal job dict; we don't rely on real Dagster types
    named_jobs = {"default": object()}

    # Replace the Dagster decorator with a lightweight test double that records arguments
    @dataclass
    class _FakeSensorDefinition:
        name: str
        description: str
        monitored_jobs: list[Any]
        default_status: Any
        fn: Callable[..., Any]

    def fake_run_failure_sensor(name: str, description: str, monitored_jobs: list[Any], default_status: Any):
        def _decorator(fn: Callable[..., Any]) -> _FakeSensorDefinition:
            return _FakeSensorDefinition(
                name=name,
                description=description,
                monitored_jobs=monitored_jobs,
                default_status=default_status,
                fn=fn,
            )

        return _decorator

    monkeypatch.setattr(dg, "run_failure_sensor", fake_run_failure_sensor)

    # Also provide a sentinel for DefaultSensorStatus
    class _Sentinel:
        RUNNING = "RUNNING"

    monkeypatch.setattr(dg, "DefaultSensorStatus", _Sentinel)

    sensors = translator._translate_on_pipeline_error_hook(named_jobs)
    assert "on_pipeline_error_sensor" in sensors
    sensor_def = sensors["on_pipeline_error_sensor"]

    # Validate the decorator captured expected metadata
    assert sensor_def.name == "on_pipeline_error_sensor"
    assert isinstance(sensor_def.description, str) and len(sensor_def.description) > 0
    assert sensor_def.monitored_jobs == list(named_jobs.values())
    assert sensor_def.default_status == "RUNNING"
