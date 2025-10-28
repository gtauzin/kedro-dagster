# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.config import get_dagster_config
from kedro_dagster.dagster import ExecutorCreator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator


@pytest.mark.parametrize("env", ["base", "local"])
def test_pipeline_translator_to_dagster_with_executor(env, request):
    """Translate a Kedro pipeline to Dagster jobs with configured executors and resources."""
    options = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)

    # Catalog -> IO managers and partition metadata
    default_pipeline = pipelines.get("__default__")
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Nodes -> op factories and assets
    node_translator = NodeTranslator(
        pipelines=[default_pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )
    # Obtain op factories and assets via the NodeTranslator API
    named_op_factories, named_assets = node_translator.to_dagster()

    # Executors from config
    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()
    assert "seq" in named_executors

    # Build jobs
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
    assert "default" in jobs
    assert isinstance(jobs["default"], dg.JobDefinition)


@pytest.mark.parametrize("env", ["base", "local"])
def test_after_pipeline_run_hook_inputs_fan_in_for_partitions(env, request):
    """Ensure the after-pipeline-run hook op declares a Nothing input per partition.

    We configure a partitioned path intermediate -> output2 with identity mapping,
    then build the job and introspect the hook op input names to confirm they include
    the per-partition fan-in inputs (e.g., node2__p1_after_pipeline_run_hook_input).
    """
    options = request.getfixturevalue(f"kedro_project_partitioned_intermediate_output2_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

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
    job = jobs["default"]

    # Fetch the after-pipeline-run hook op definition from the job
    hook_name = "after_pipeline_run_hook_default"
    node_def = None
    # Try common Dagster APIs to retrieve node definitions
    if hasattr(job, "graph") and hasattr(job.graph, "node_defs"):
        for nd in job.graph.node_defs:
            if getattr(nd, "name", None) == hook_name:
                node_def = nd
                break
    if node_def is None and hasattr(job, "all_node_defs"):
        for nd in job.all_node_defs:
            if getattr(nd, "name", None) == hook_name:
                node_def = nd
                break

    assert node_def is not None, "Hook op definition not found in job"
    ins_keys = set(getattr(node_def, "ins").keys())
    # Expect a Nothing input per partition for the last node (node2)
    # The naming includes both upstream and downstream partition keys for clarity
    assert "node2__p1__p1_after_pipeline_run_hook_input" in ins_keys
    assert "node2__p2__p2_after_pipeline_run_hook_input" in ins_keys


@pytest.mark.parametrize(
    "env_fixture",
    [
        "kedro_project_exec_filebacked_base",
        "kedro_project_exec_filebacked_local",
        "kedro_project_partitioned_intermediate_output2_base",
        "kedro_project_partitioned_intermediate_output2_local",
        "kedro_project_partitioned_static_mapping_base",
        "kedro_project_partitioned_static_mapping_local",
        "kedro_project_multiple_inputs_base",
        "kedro_project_multiple_inputs_local",
        "kedro_project_multiple_outputs_tuple_base",
        "kedro_project_multiple_outputs_tuple_local",
        "kedro_project_multiple_outputs_dict_base",
        "kedro_project_multiple_outputs_dict_local",
        "kedro_project_no_outputs_node_base",
        "kedro_project_no_outputs_node_local",
        "kedro_project_nothing_assets_base",
        "kedro_project_nothing_assets_local",
    ],
)
def test_pipeline_translator_builds_jobs_for_scenarios(request, env_fixture):
    """Ensure PipelineTranslator can build a job across diverse scenarios without errors."""
    options = request.getfixturevalue(env_fixture)
    project_path = options.project_path
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    dagster_config = get_dagster_config(context)
    default_pipeline = pipelines.get("__default__")

    # Catalog -> IO managers and partition metadata
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[default_pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Nodes -> op factories and assets via the NodeTranslator
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

    # Executors from config
    executor_creator = ExecutorCreator(dagster_config=dagster_config)
    named_executors = executor_creator.create_executors()

    # Build jobs
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
    assert "default" in jobs
    assert isinstance(jobs["default"], dg.JobDefinition)
