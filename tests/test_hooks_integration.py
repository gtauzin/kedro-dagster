# mypy: ignore-errors

from __future__ import annotations

from dataclasses import dataclass, field

import dagster as dg
import pytest
from kedro.framework.hooks import hook_impl
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline, node

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator
from kedro_dagster.utils import _kedro_version


@dataclass
class RecordingHooks:
    before_dataset_loaded_calls: list[str] = field(default_factory=list)
    after_dataset_loaded_calls: list[str] = field(default_factory=list)
    before_dataset_saved_calls: list[str] = field(default_factory=list)
    after_dataset_saved_calls: list[str] = field(default_factory=list)
    before_node_run_calls: list[str] = field(default_factory=list)
    after_node_run_calls: list[str] = field(default_factory=list)
    before_pipeline_run_calls: int = 0
    after_pipeline_run_calls: int = 0

    @hook_impl
    def before_dataset_loaded(self, dataset_name, node):
        self.before_dataset_loaded_calls.append(dataset_name)

    @hook_impl
    def after_dataset_loaded(self, dataset_name, data, node):
        self.after_dataset_loaded_calls.append(dataset_name)

    @hook_impl
    def before_dataset_saved(self, dataset_name, data, node):
        self.before_dataset_saved_calls.append(dataset_name)

    @hook_impl
    def after_dataset_saved(self, dataset_name, data, node):
        self.after_dataset_saved_calls.append(dataset_name)

    @hook_impl
    def before_node_run(self, node, catalog, inputs, is_async):
        self.before_node_run_calls.append(node.name)

    @hook_impl
    def after_node_run(self, node, catalog, inputs, outputs, is_async):
        self.after_node_run_calls.append(node.name)

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        self.before_pipeline_run_calls += 1

    @hook_impl
    def after_pipeline_run(self, run_params, pipeline, catalog):
        self.after_pipeline_run_calls += 1


@pytest.mark.parametrize("env", ["base", "local"])
def test_hooks_are_invoked_end_to_end(env, request):
    """Execute a translated job and assert Kedro hooks are invoked (pipeline, node, dataset)."""
    # Arrange: use a project variant with file-backed datasets so IO managers are used
    options = request.getfixturevalue(f"kedro_project_hooks_filebacked_{env}")
    project_path = options.project_path

    # Bootstrap and session
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    if _kedro_version()[0] >= 1:
        run_id_kwargs = {"run_id": session.session_id}
    else:
        run_id_kwargs = {"session_id": session.session_id}

    # Register recording hooks on Kedro hook manager
    hooks = RecordingHooks()
    context._hook_manager.register(hooks)

    # Translate configurations
    dagster_config = get_dagster_config(context)
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
        **run_id_kwargs,
    )
    named_op_factories, named_assets = node_translator.to_dagster()

    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()

    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors=named_executors,
        enable_mlflow=False,
        **run_id_kwargs,
    )
    jobs = pipeline_translator.to_dagster()

    # Act: execute the job in process to trigger hooks
    result = jobs["default"].execute_in_process()
    assert result.success

    # Assert: Pipeline hooks called once each
    assert hooks.before_pipeline_run_calls == 1
    assert hooks.after_pipeline_run_calls == 1

    # Assert: Node hooks called for each node in the default pipeline
    DEFAULT_PIPELINE_NODE_COUNT = 5
    assert len(hooks.before_node_run_calls) == DEFAULT_PIPELINE_NODE_COUNT
    assert len(hooks.after_node_run_calls) == DEFAULT_PIPELINE_NODE_COUNT

    # Assert: Dataset IO hooks observed on save/load
    # At least one load for input_dataset and multiple loads for intermediate; saves for intermediate and outputs
    assert "input_ds" in hooks.before_dataset_loaded_calls
    assert "intermediate_ds" in hooks.before_dataset_saved_calls
    assert "output_ds" in hooks.after_dataset_saved_calls
    assert set(["output2_ds", "output3_ds", "output4_ds"]).issubset(set(hooks.after_dataset_saved_calls))


class DummyContext:
    def __init__(self, catalog: DataCatalog):
        self.catalog = catalog

        # create a minimal hook manager-like object
        class _HookManager:
            def __init__(self):
                class _Hook:
                    def before_pipeline_run(self, **kwargs):
                        return None

                    def after_pipeline_run(self, **kwargs):
                        return None

                self.hook = _Hook()

        self._hook_manager = _HookManager()


def _make_pipeline_translator(named_resources: dict | None = None) -> PipelineTranslator:
    catalog = DataCatalog()

    return PipelineTranslator(
        dagster_config={},
        context=DummyContext(catalog),
        project_path="/tmp/project",
        env="base",
        run_id="sess",
        named_assets={},
        asset_partitions={},
        named_op_factories={},
        named_resources=named_resources or {},
        named_executors={},
        enable_mlflow=False,
    )


def test_enumerate_partition_keys_none_returns_empty_list():
    """Enumerating partition keys for None yields an empty list."""
    t = _make_pipeline_translator()
    assert t._enumerate_partition_keys(None) == []


def test_before_after_pipeline_hooks_require_mlflow_conditionally():
    """Before/after pipeline hook ops require 'mlflow' resource only when enabled."""
    # without mlflow
    t1 = _make_pipeline_translator()
    op1 = t1._create_before_pipeline_run_hook("job", Pipeline([]))
    assert "mlflow" not in op1.required_resource_keys

    after1 = t1._create_after_pipeline_run_hook_op("job", Pipeline([]), ["x_after_pipeline_run_hook_input"])
    assert "mlflow" not in after1.required_resource_keys
    # input should be declared
    assert "x_after_pipeline_run_hook_input" in after1.ins

    # with mlflow
    t2 = _make_pipeline_translator({"mlflow": object()})
    op2 = t2._create_before_pipeline_run_hook("job", Pipeline([]))
    assert "mlflow" in op2.required_resource_keys
    after2 = t2._create_after_pipeline_run_hook_op("job", Pipeline([]), [])
    assert "mlflow" in after2.required_resource_keys


def test_node_op_declares_after_hook_output_and_mlflow_requirement():
    """Node op declares the after-pipeline-run Nothing output and mlflow resource conditionally."""
    # minimal NodeTranslator using a dummy pipeline and no catalog IO managers
    catalog = DataCatalog()
    nt_without_mlflow = NodeTranslator(
        pipelines=[Pipeline([])],
        catalog=catalog,
        hook_manager=DummyContext(catalog)._hook_manager,
        run_id="sess",
        asset_partitions={},
        named_resources={},
        env="base",
    )

    nt_with_mlflow = NodeTranslator(
        pipelines=[Pipeline([])],
        catalog=catalog,
        hook_manager=DummyContext(catalog)._hook_manager,
        run_id="sess",
        asset_partitions={},
        named_resources={"mlflow": object()},
        env="base",
    )

    n = node(func=lambda inputs: {"out": 1}, inputs=["inp"], outputs=["out"], name="N")
    op1 = nt_without_mlflow.create_op(n)
    # last-layer adds a synthetic Nothing output
    assert any(name.endswith("_after_pipeline_run_hook_input") for name in op1.outs)
    assert "mlflow" not in op1.required_resource_keys

    op2 = nt_with_mlflow.create_op(n)
    assert "mlflow" in op2.required_resource_keys
