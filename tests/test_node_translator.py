# mypy: ignore-errors

from __future__ import annotations

import importlib
from pathlib import Path

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.utils import format_node_name, is_nothing_asset_name


def _get_node_producing_output(pipeline: Pipeline, dataset_name: str) -> Node:
    for n in pipeline.nodes:
        if dataset_name in n.outputs:
            return n
    raise AssertionError(f"No node produces dataset '{dataset_name}' in pipeline")


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_create_op_wires_resources(env, request):
    _fixture_val = request.getfixturevalue(f"kedro_project_exec_filebacked_{env}")
    project_path = _fixture_val
    while isinstance(project_path, (tuple, list)) and len(project_path) > 0:
        if isinstance(project_path[0], Path):
            project_path = project_path[0]
            break
        project_path = project_path[0]

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    # Ensure Kedro is configured to the just-created project's package to avoid
    # stale global configuration affecting hooks (e.g., telemetry importing pipelines).
    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)
    project_module = importlib.import_module("kedro.framework.project")

    pipeline = project_module.pipelines.get("__default__")

    # Build named_resources via CatalogTranslator to simulate ProjectTranslator flow
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
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )

    node = _get_node_producing_output(pipeline, "output2_ds")
    op = node_translator.create_op(node)
    assert isinstance(op, dg.OpDefinition)

    # Ensure the file-backed dataset IO manager is required
    assert f"{env}__output2_ds_io_manager" in op.required_resource_keys


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_create_op_partition_tags_and_name_suffix(env, request):
    _fixture_val = request.getfixturevalue(f"kedro_project_exec_filebacked_output2_memory_{env}")
    project_path = _fixture_val
    while isinstance(project_path, (tuple, list)) and len(project_path) > 0:
        if isinstance(project_path[0], Path):
            project_path = project_path[0]
            break
        project_path = project_path[0]

    # Configure project before accessing pipelines; then reload project module to avoid stale state
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    # Force Kedro to use the current project's package for the pipeline registry
    # by re-configuring the project with its detected package name.
    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)
    project_module = importlib.import_module("kedro.framework.project")
    pipeline = project_module.pipelines.get("__default__")

    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,
        session_id=session.session_id,
        asset_partitions={},
        named_resources={},
        env=env,
    )

    node = _get_node_producing_output(pipeline, "output2_ds")
    partition_keys = {
        "upstream_partition_key": "input_ds|a",
        "downstream_partition_key": "output2_ds|a",
    }
    op = node_translator.create_op(node, partition_keys=partition_keys)

    # Name suffix should include formatted partition key
    assert "__a" in op.name
    # Tags should include upstream/downstream partition info
    assert op.tags.get("upstream_partition_key") == "input_ds|a"
    assert op.tags.get("downstream_partition_key") == "output2_ds|a"


@pytest.mark.parametrize(
    "kedro_project_multi_in_out_env",
    [
        ("multiple_inputs", "base"),
        ("multiple_inputs", "local"),
        ("multiple_outputs_tuple", "base"),
        ("multiple_outputs_tuple", "local"),
        ("multiple_outputs_dict", "base"),
        ("multiple_outputs_dict", "local"),
    ],
    indirect=True,
)
def test_node_translator_handles_multiple_inputs_and_outputs(kedro_project_multi_in_out_env):
    project_path, options = kedro_project_multi_in_out_env
    env = options.env

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

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
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )

    # Pick a node with outputs and ensure op creation works (covers wiring for multi in/out)
    node_with_outputs = next(n for n in pipeline.nodes if len(n.outputs) > 0)
    op = node_translator.create_op(node_with_outputs)
    assert isinstance(op, dg.OpDefinition)


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_node_translator_handles_nothing_datasets(env, request):
    _fixture_val = request.getfixturevalue(f"kedro_project_nothing_assets_{env}")
    project_path = _fixture_val
    while isinstance(project_path, (tuple, list)) and len(project_path) > 0:
        if isinstance(project_path[0], Path):
            project_path = project_path[0]
            break
        project_path = project_path[0]

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

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
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )

    # Debug print removed to avoid noisy test output

    # Find nodes that use a Nothing dataset by inspecting catalog types
    def _has_nothing_output(n):
        return any(is_nothing_asset_name(context.catalog, ds) for ds in n.outputs)

    def _has_nothing_input(n):
        return any(is_nothing_asset_name(context.catalog, ds) for ds in n.inputs)

    produce_node = next((n for n in pipeline.nodes if _has_nothing_output(n)), None)
    gated_node = next((n for n in pipeline.nodes if _has_nothing_input(n)), None)

    # if not produce_node or not gated_node:
    #     pytest.skip("Scenario did not materialize Nothing-producing or Nothing-consuming nodes in this run")

    op_produce = node_translator.create_op(produce_node)
    op_gated = node_translator.create_op(gated_node)

    # The catalog must recognize the Nothing dataset type
    # At least one Nothing dataset must exist in the catalog
    assert any(is_nothing_asset_name(context.catalog, name) for name in context.catalog.list())

    # Ensure the op exposes the start_signal output and input respectively
    # Ensure op outs/ins include Nothing-typed assets by name presence
    assert any(
        is_nothing_asset_name(context.catalog, name.replace("__", "."))
        for name in getattr(op_produce, "outs").keys()  # type: ignore[attr-defined]
    )
    assert any(
        is_nothing_asset_name(context.catalog, name.replace("__", "."))
        for name in getattr(op_gated, "ins").keys()  # type: ignore[attr-defined]
    )


@pytest.mark.parametrize("env", ["base", "local"])  # use existing per-env fixtures
def test_node_translator_handles_no_output_node(env, request):
    _fixture_val = request.getfixturevalue(f"kedro_project_no_outputs_node_{env}")
    project_path = _fixture_val
    while isinstance(project_path, (tuple, list)) and len(project_path) > 0:
        if isinstance(project_path[0], Path):
            project_path = project_path[0]
            break
        project_path = project_path[0]

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    src_dir = project_path / "src"
    pkg_dirs = [p for p in src_dir.iterdir() if p.is_dir() and p.name != "__pycache__"]
    if pkg_dirs:
        package_name = pkg_dirs[0].name
        project_module = importlib.import_module("kedro.framework.project")
        project_module.configure_project(package_name)
    project_module = importlib.import_module("kedro.framework.project")
    pipeline = project_module.pipelines.get("__default__")

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
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources=named_io_managers,
        env=env,
    )

    # Select the known no-output node from the scenario
    no_out_node = next((n for n in pipeline.nodes if n.name == "sink"), None)
    # if no_out_node is None:
    #     pytest.skip("No-output node 'sink' not present in this scenario variant")

    # to_dagster should create an op factory but not an asset for this node
    named_op_factories, named_assets = node_translator.to_dagster()
    op_key = f"{format_node_name(no_out_node.name)}_graph"
    assert op_key in named_op_factories
    assert format_node_name(no_out_node.name) not in named_assets

    # The op should only expose the after_pipeline_run Nothing output (no dataset outs)
    op = node_translator.create_op(no_out_node)
    out_keys = list(getattr(op, "outs").keys())  # type: ignore[attr-defined]
    assert len(out_keys) == 1 and out_keys[0].endswith("_after_pipeline_run_hook_input")
