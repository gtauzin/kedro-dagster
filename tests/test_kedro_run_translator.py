# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import dagster as dg
import pytest
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.utils import KEDRO_VERSION


class _FakeHook:
    def __init__(self) -> None:
        self.after_context_created_called_with: list[Any] = []
        self.after_catalog_created_called_with: list[dict[str, Any]] = []
        self.on_pipeline_error_called_with: list[dict[str, Any]] = []

    # signature used in kedro.py
    def after_context_created(self, *, context: Any) -> None:
        self.after_context_created_called_with.append(context)

    def after_catalog_created(
        self,
        *,
        catalog: Any,
        conf_catalog: dict[str, Any],
        conf_creds: dict[str, Any],
        save_version: str | None = None,
        load_versions: Any = None,
        parameters: dict[str, Any] | None = None,  # Kedro 1.x
        feed_dict: dict[str, Any] | None = None,  # Kedro 0.19
    ) -> None:
        self.after_catalog_created_called_with.append({
            "catalog": catalog,
            "parameters": parameters,
            "feed_dict": feed_dict,
            "conf_catalog": conf_catalog,
            "conf_creds": conf_creds,
            "save_version": save_version,
            "load_versions": load_versions,
        })

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
        catalog=kedro_context_base.catalog,
        project_path=str(options.project_path),
        env=options.env,
        run_id="sid-123",
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
    if KEDRO_VERSION[0] >= 1:
        run_id_key = "run_id"
    else:
        run_id_key = "session_id"
    assert params["project_path"] == str(options.project_path)
    assert params["env"] == options.env
    assert params[run_id_key] == "sid-123"
    assert params["pipeline_name"] == "__default__"
    # defaults set in to_dagster
    assert params["load_versions"] is None
    if KEDRO_VERSION[0] >= 1:
        assert params["runtime_params"] is None
    else:
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
        catalog=kedro_context_base.catalog,
        project_path=str(options.project_path),
        env=options.env,
        run_id="sid-xyz",
    )

    # Capture filter arguments received by the dummy pipeline
    captured: dict[str, Any] = {}

    if KEDRO_VERSION[0] >= 1:
        node_namespace_key = "node_namespaces"
        node_namespace_val = ["ns"]

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
                node_namespaces=None,
            ) -> dict[str, Any]:
                captured.update(
                    dict(
                        tags=tags,
                        from_nodes=from_nodes,
                        to_nodes=to_nodes,
                        node_names=node_names,
                        from_inputs=from_inputs,
                        to_outputs=to_outputs,
                        node_namespaces=node_namespaces,
                    )
                )
                return {"ok": True}

    else:
        node_namespace_key = "node_namespace"
        node_namespace_val = "ns"

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
    monkeypatch.setattr("kedro.framework.project.pipelines.get", lambda name: _DummyPipeline())

    resource = translator.to_dagster(
        pipeline_name="my_pipeline",
        filter_params={
            "tags": ["x"],
            "from_nodes": ["A"],
            "to_outputs": ["out"],
            node_namespace_key: node_namespace_val,
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
        node_namespace_key: node_namespace_val,
    }


def test_after_context_created_hook_invokes_hook_manager(
    kedro_context_base: Any, kedro_project_exec_filebacked_base
) -> None:
    """after_context_created_hook triggers the Kedro hook with the current context."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        catalog=kedro_context_base.catalog,
        project_path=str(options.project_path),
        env=options.env,
        run_id="sid-123",
    )
    # Install fake hook manager BEFORE resource creation so the closure captures it
    fake_hook_mgr = _FakeHookManager()
    translator._context._hook_manager = fake_hook_mgr
    translator._hook_manager = fake_hook_mgr  # also update translator cache used in closure
    resource = translator.to_dagster(pipeline_name="__default__", filter_params={})

    # Call the hook and ensure the underlying kedro hook was triggered
    resource.after_context_created_hook()

    fake_ctx = translator._context
    assert fake_ctx._hook_manager.hook.after_context_created_called_with == [fake_ctx]


def test_after_catalog_created_hook_invokes_hook_manager(
    kedro_context_base: Any, kedro_project_exec_filebacked_base
) -> None:
    """after_catalog_created_hook triggers the Kedro hook with all required parameters."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        catalog=kedro_context_base.catalog,
        project_path=str(options.project_path),
        env=options.env,
        run_id="sid-456",
    )
    # Install fake hook manager BEFORE resource creation so the closure captures it
    fake_hook_mgr = _FakeHookManager()
    translator._context._hook_manager = fake_hook_mgr
    translator._hook_manager = fake_hook_mgr  # also update translator cache used in closure
    resource = translator.to_dagster(pipeline_name="__default__", filter_params={})

    # Call the hook and ensure the underlying kedro hook was triggered
    resource.after_catalog_created_hook()

    # Verify the hook was called at least once by our explicit call
    # (Kedro may also call it internally when accessing the catalog)
    assert len(fake_hook_mgr.hook.after_catalog_created_called_with) >= 1

    # Get the last hook call (most recent)
    hook_call = fake_hook_mgr.hook.after_catalog_created_called_with[-1]

    # Verify all required parameters were passed
    # Catalog might be a different instance, but verify it's a catalog object
    assert hasattr(hook_call["catalog"], "_datasets")

    # Kedro 1.x uses 'parameters', Kedro 0.19 uses 'feed_dict'
    if KEDRO_VERSION[0] >= 1:
        assert hook_call["parameters"] == translator._context._get_parameters()
        assert hook_call["feed_dict"] is None
    else:
        assert hook_call["feed_dict"] == translator._context._get_feed_dict()
        assert hook_call["parameters"] is None

    assert isinstance(hook_call["conf_catalog"], dict)
    assert isinstance(hook_call["conf_creds"], dict)

    # Verify save_version matches run_id (Kedro >= 1.0) or session_id (Kedro < 1.0)
    assert hook_call["save_version"] == "sid-456"

    assert hook_call["load_versions"] is None


def test_translate_on_pipeline_error_hook_returns_sensor(
    kedro_context_base: Any, kedro_project_exec_filebacked_base, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_translate_on_pipeline_error_hook returns a sensor with expected metadata."""
    options = kedro_project_exec_filebacked_base
    translator = KedroRunTranslator(
        context=kedro_context_base,
        catalog=kedro_context_base.catalog,
        project_path=str(options.project_path),
        env=options.env,
        run_id="sid-123",
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
