from __future__ import annotations

import dagster as dg
import pytest
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.catalog import CatalogTranslator
from kedro_dagster.nodes import NodeTranslator

from ..scenarios.kedro_projects import options_partitioned_identity_mapping


@pytest.mark.parametrize("env", ["base", "local"])
def test_static_partitions_and_identity_mapping(project_variant_factory, env):
    project_path = project_variant_factory(options_partitioned_identity_mapping(env))

    bootstrap_project(project_path)
    session = KedroSession.create(project_path=project_path, env=env)
    context = session.load_context()
    pipeline = pipelines.get("__default__")

    catalog_translator = CatalogTranslator(
        catalog=context.catalog,
        pipelines=[pipeline],
        hook_manager=context._hook_manager,  # noqa: SLF001
        env=env,
    )
    named_io_managers, asset_partitions = catalog_translator.to_dagster()

    # Validate partitions collected
    assert "intermediate" in asset_partitions
    assert "output2" in asset_partitions
    assert isinstance(asset_partitions["intermediate"]["partitions_def"], dg.PartitionsDefinition)
    assert isinstance(asset_partitions["output2"]["partitions_def"], dg.PartitionsDefinition)

    # Build node translator, compute partition keys mapping for the node producing output2
    node_translator = NodeTranslator(
        pipelines=[pipeline],
        catalog=context.catalog,
        hook_manager=context._hook_manager,  # noqa: SLF001
        session_id=session.session_id,
        asset_partitions=asset_partitions,
        named_resources={**named_io_managers, "io_manager": dg.fs_io_manager},
        env=env,
    )

    # The internal helper will be used by PipelineTranslator, but we assert identity mapping indirectly
    # by creating an op with a specific upstream partition key and checking the emitted materialization tag.
    node = next(n for n in pipeline.nodes if "output2" in n.outputs)
    op = node_translator.create_op(node, partition_keys={
        "upstream_partition_key": "intermediate|p1",
        "downstream_partition_key": "output2|p1",
    })

    assert "upstream_partition_key" in op.tags and op.tags["upstream_partition_key"].endswith("|p1")
    assert "downstream_partition_key" in op.tags and op.tags["downstream_partition_key"].endswith("|p1")
