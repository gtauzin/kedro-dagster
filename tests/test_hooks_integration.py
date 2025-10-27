# mypy: ignore-errors

from __future__ import annotations

from dataclasses import dataclass, field

import dagster as dg
import pytest
from kedro.framework.hooks import hook_impl
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator


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
    def before_node_run(self, node, catalog, inputs, is_async, session_id):
        self.before_node_run_calls.append(node.name)

    @hook_impl
    def after_node_run(self, node, catalog, inputs, outputs, is_async, session_id):
        self.after_node_run_calls.append(node.name)

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        self.before_pipeline_run_calls += 1

    @hook_impl
    def after_pipeline_run(self, run_params, pipeline, catalog):
        self.after_pipeline_run_calls += 1


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_hooks_are_invoked_end_to_end(env, request):
    # Arrange: use a project variant with file-backed datasets so IO managers are used
    project_path, _ = request.getfixturevalue(f"kedro_project_hooks_filebacked_{env}")

    # Bootstrap and session
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

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
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
    )
    named_op_factories, named_assets = node_translator.to_dagster()

    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()

    pipeline_translator = PipelineTranslator(
        dagster_config=dagster_config,
        context=context,
        project_path=str(project_path),
        env=env,
        session_id=session.session_id,
        named_assets=named_assets,
        asset_partitions=asset_partitions,
        named_op_factories=named_op_factories,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        named_executors=named_executors,
        enable_mlflow=False,
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
