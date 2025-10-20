# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.nodes import NodeTranslator

from ..scenarios.kedro_projects import options_exec_filebacked


def _get_node_producing_output(pipeline: Pipeline, dataset_name: str) -> Node:
    for n in pipeline.nodes:
        if dataset_name in n.outputs:
            return n
    raise AssertionError(f"No node produces dataset '{dataset_name}' in pipeline")


@pytest.mark.parametrize("env", ["base", "local"])
def test_create_op_wires_resources(project_variant_factory, env):
    project_path = project_variant_factory(options_exec_filebacked(env))

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    pipeline = pipelines.get("__default__")

    # Build named_resources via CatalogTranslator to simulate ProjectTranslator flow
    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,  # noqa: SLF001
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,  # noqa: SLF001
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )

    node = _get_node_producing_output(pipeline, "output2")
    op = node_translator.create_op(node)
    assert isinstance(op, dg.OpDefinition)

    # Ensure the file-backed dataset IO manager is required
    assert f"{env}__output2_io_manager" in op.required_resource_keys


@pytest.mark.parametrize("env", ["base", "local"])
def test_create_op_partition_tags_and_name_suffix(project_variant_factory, env):
    # Reuse scenario but make output2 a MemoryDataset for this test
    options = options_exec_filebacked(env)
    options.catalog["output2"] = {"type": "MemoryDataset"}
    project_path = project_variant_factory(options)

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    pipeline = pipelines.get("__default__")

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,  # noqa: SLF001
        session_id=session.session_id,
        asset_partitions={},
        named_resources={},
        env=env,
    )

    node = _get_node_producing_output(pipeline, "output2")
    partition_keys = {
        "upstream_partition_key": "input|a",
        "downstream_partition_key": "output2|a",
    }
    op = node_translator.create_op(node, partition_keys=partition_keys)

    # Name suffix should include formatted partition key
    assert "__a" in op.name
    # Tags should include upstream/downstream partition info
    assert op.tags.get("upstream_partition_key") == "input|a"
    assert op.tags.get("downstream_partition_key") == "output2|a"
