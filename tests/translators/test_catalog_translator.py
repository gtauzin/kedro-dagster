# mypy: ignore-errors

from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator

from ..scenarios.kedro_projects import options_exec_filebacked


@pytest.mark.parametrize("env", ["base", "local"])  # extend with other envs later
def test_catalog_translator_io_managers_and_partitions(project_variant_factory, env):
    # Use base scenario then enrich with extra partitioned dataset for this test
    options = options_exec_filebacked(env)
    options.catalog.update({
        "output": {
            "type": "pandas.CSVDataset",
            "filepath": "data/08_reporting/output.csv",
            "save_args": {"index": False},
        },
        "output2": {
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
    assert f"{env}__output2_io_manager" in named_io_managers

    # Partitioned dataset should be present and expose partitions_def
    assert "output2" in asset_partitions
    assert isinstance(asset_partitions["output2"]["partitions_def"], dg.PartitionsDefinition)

    # MemoryDataset should not have a dedicated IO manager
    assert f"{env}__intermediate_io_manager" not in named_io_managers
