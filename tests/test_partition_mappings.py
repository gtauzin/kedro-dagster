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
from kedro_dagster.utils import _kedro_version


@pytest.mark.parametrize("env", ["base", "local"])
def test_static_partitions_and_identity_mapping(env, request):
    """Identity mapping keeps upstream and downstream partition keys aligned (p -> p)."""
    options = request.getfixturevalue(f"kedro_project_partitioned_identity_mapping_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    if _kedro_version()[0] >= 1:
        run_id_kwargs = {"run_id": session.session_id}
    else:
        run_id_kwargs = {"session_id": session.session_id}

    pipeline = pipelines.get("__default__")

    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Validate partitions collected
    assert "intermediate_ds" in asset_partitions
    assert "output2_ds" in asset_partitions
    assert isinstance(asset_partitions["intermediate_ds"]["partitions_def"], dg.PartitionsDefinition)
    assert isinstance(asset_partitions["output2_ds"]["partitions_def"], dg.PartitionsDefinition)

    # Build node translator, compute partition keys mapping for the node producing output2
    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
        **run_id_kwargs,
    )

    # The internal helper will be used by PipelineTranslator, but we assert identity mapping indirectly
    # by creating an op with a specific upstream partition key and checking the emitted materialization tag.
    node = next(n for n in pipeline.nodes if "output2_ds" in n.outputs)
    op = node_translator.create_op(
        node,
        partition_keys={
            "upstream_partition_key": "intermediate_ds|p1",
            "downstream_partition_key": "output2_ds|p1",
        },
    )

    assert "upstream_partition_key" in op.tags and op.tags["upstream_partition_key"].endswith("|p1")
    assert "downstream_partition_key" in op.tags and op.tags["downstream_partition_key"].endswith("|p1")


@pytest.mark.parametrize("env", ["base", "local"])
def test_static_partitions_and_static_mapping(env, request):
    """Ensure StaticPartitionMapping routes upstream partitions to configured downstream keys."""
    options = request.getfixturevalue(f"kedro_project_partitioned_static_mapping_{env}")
    project_path = options.project_path

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    if _kedro_version()[0] >= 1:
        run_id_kwargs = {"run_id": session.session_id}
    else:
        run_id_kwargs = {"session_id": session.session_id}

    pipeline = pipelines.get("__default__")

    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
        **run_id_kwargs,
    )

    dagster_config = get_dagster_config(context)
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
    job = jobs["default"]

    # Locate the after-pipeline-run hook node and inspect its inputs
    hook_name = "after_pipeline_run_hook_default"
    node_def = None
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

    # Expect p1->a, p2->b
    assert "node2__p1__a_after_pipeline_run_hook_input" in ins_keys
    assert "node2__p2__b_after_pipeline_run_hook_input" in ins_keys
