# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator

from .scenarios.kedro_projects import (
    options_exec_filebacked,
    options_multiple_inputs,
    options_multiple_outputs_dict,
    options_multiple_outputs_tuple,
    options_no_outputs_node,
    options_nothing_assets,
    options_partitioned_intermediate_output2,
    options_partitioned_static_mapping,
)


@pytest.mark.parametrize("env", ["base", "local"])  # extend with other envs later
def test_catalog_translator_io_managers_and_partitions(project_variant_factory, env):
    # Use base scenario then enrich with extra partitioned dataset for this test
    options = options_exec_filebacked(env)
    options.catalog.update({
        "output_ds": {
            "type": "pandas.CSVDataset",
            "filepath": "data/08_reporting/output.csv",
            "save_args": {"index": False},
        },
        "output2_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
        },
    })
    project_path = project_variant_factory(options)

    # Bootstrap the project (register package name) and spin a Kedro session on the variant
    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,  # noqa: SLF001
        env=env,
    )

    named_io_managers, asset_partitions = translator.to_dagster()

    # File-backed dataset should have an IO manager
    assert f"{env}__output2_ds_io_manager" in named_io_managers

    # Partitioned dataset should be present and expose partitions_def
    assert "output2_ds" in asset_partitions
    assert isinstance(asset_partitions["output2_ds"]["partitions_def"], dg.PartitionsDefinition)

    # MemoryDataset should not have a dedicated IO manager
    assert f"{env}__intermediate_ds_io_manager" not in named_io_managers


@pytest.mark.parametrize(
    "env,options_builder",
    [
        ("base", options_exec_filebacked),
        ("local", options_exec_filebacked),
        ("base", options_partitioned_intermediate_output2),
        ("local", options_partitioned_intermediate_output2),
        ("base", options_partitioned_static_mapping),
        ("local", options_partitioned_static_mapping),
        ("base", options_multiple_inputs),
        ("local", options_multiple_inputs),
        ("base", options_multiple_outputs_tuple),
        ("local", options_multiple_outputs_tuple),
        ("base", options_multiple_outputs_dict),
        ("local", options_multiple_outputs_dict),
        ("base", options_no_outputs_node),
        ("local", options_no_outputs_node),
        ("base", options_nothing_assets),
        ("local", options_nothing_assets),
    ],
)
def test_catalog_translator_covers_scenarios(project_variant_factory, env, options_builder):
    """Smoke-test CatalogTranslator across diverse scenarios and assert core invariants.

    - File-backed datasets (CSVDataset) get IO managers.
    - Partitioned datasets expose partitions_def in asset_partitions.
    - Memory datasets do not result in dedicated IO managers.
    """
    options = options_builder(env)
    project_path = project_variant_factory(options)

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()

    pipeline = pipelines.get("__default__")
    translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,  # noqa: SLF001
        env=env,
    )

    named_io_managers, asset_partitions = translator.to_dagster()

    assert isinstance(named_io_managers, dict)
    assert isinstance(asset_partitions, dict)

    # Validate IO managers for file-backed datasets and absence for MemoryDataset
    for ds_name, ds_cfg in options.catalog.items():
        ds_type = ds_cfg.get("type")
        if ds_type == "pandas.CSVDataset":
            assert f"{env}__{ds_name}_io_manager" in named_io_managers
        elif ds_type == "MemoryDataset":
            assert f"{env}__{ds_name}_io_manager" not in named_io_managers
        elif ds_type == "kedro_dagster.datasets.DagsterPartitionedDataset":
            # Partitioned dataset should have partitions_def registered
            assert ds_name in asset_partitions
            assert isinstance(asset_partitions[ds_name]["partitions_def"], dg.PartitionsDefinition)
            # IO manager naming is allowed but not required; do not assert here to avoid false negatives
        # Other dataset types (e.g., DagsterNothingDataset) have no IO managers
