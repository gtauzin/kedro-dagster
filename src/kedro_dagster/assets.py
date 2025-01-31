"""Dagster asset definitons from Kedro nodes."""

from logging import getLogger

import dagster as dg
from kedro.framework.project import pipelines
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pluggy import PluginManager
from pydantic import ConfigDict

from kedro_dagster.utils import _create_pydantic_model_from_dict

LOGGER = getLogger(__name__)


def _define_node_multi_asset(
    node: Node,
    pipeline_name: str,
    catalog: DataCatalog,
    hook_manager: PluginManager,
    session_id: str,
) -> dg.AssetsDefinition:
    """Wrap a kedro Node inside a Dagster multi asset.

    Args:
        node: The Kedro ``Node`` for which a Dagster multi asset is
        being created.
        pipeline_name: Name of the pipeline that the node belongs to.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.

    Returns:
        AssetsDefinition: Dagster multi assset definition that wraps the
        Kedro ``Node``.
    """
    ins, params = {}, {}
    for asset_name in node.inputs:
        if not asset_name.startswith("params:"):
            ins[asset_name] = dg.AssetIn(
                key=asset_name,
                # input_manager_key=f"{asset_name}_io_manager",
            )
        else:
            params[asset_name] = catalog.load(asset_name)

    ins["before_pipeline_run_hook_output"] = dg.AssetIn(
        key="before_pipeline_run_hook_output",
        dagster_type=dg.Nothing,
    )

    outs = {}
    for asset_name in node.outputs:
        metadata, description = None, None
        if asset_name in catalog.list():
            dataset = catalog._get_dataset(asset_name)
            metadata = getattr(dataset, "metadata", None) or {}
            description = metadata.pop("description", "")

        io_manager_key = "io_manager"
        if asset_name in catalog.list() and not isinstance(catalog._get_dataset(asset_name), MemoryDataset):
            io_manager_key = f"{asset_name}_io_manager"

        outs[asset_name] = dg.AssetOut(
            key=asset_name,
            description=description,
            metadata=metadata,
            io_manager_key=io_manager_key,
        )

    # Node parameters are mapped to Dagster configs
    NodeParametersConfig = _create_pydantic_model_from_dict(
        params,
        __base__=dg.Config,
        __config__=ConfigDict(extra="allow", frozen=False),
    )

    # Define a multi_asset from a Kedro node
    @dg.multi_asset(
        name=node.name,
        description=f"Kedro node {node.name} wrapped as a Dagster multi asset.",
        group_name=pipeline_name,
        ins=ins,
        outs=outs,
        required_resource_keys=None,
        op_tags=node.tags,
    )
    def dagster_asset(
        config: NodeParametersConfig,
        **inputs,
    ):
        # Logic to execute the Kedro node

        inputs |= config.model_dump()

        hook_manager.hook.before_node_run(
            node=node,
            catalog=catalog,
            inputs=inputs,
            is_async=False,
            session_id=session_id,
        )

        try:
            outputs = node.run(inputs)

        except Exception as exc:
            hook_manager.hook.on_node_error(
                error=exc,
                node=node,
                catalog=catalog,
                inputs=inputs,
                is_async=False,
                session_id=session_id,
            )
            raise exc

        hook_manager.hook.after_node_run(
            node=node,
            catalog=catalog,
            inputs=inputs,
            outputs=outputs,
            is_async=False,
            session_id=session_id,
        )

        if len(outputs) > 1:
            return tuple(outputs.values())

        elif len(outputs) == 1:
            return list(outputs.values())[0]

    return dagster_asset


def _get_node_pipeline_name(pipelines, node):
    """Return the name of the pipeline that a node belongs to.

    Args:
        pipelines: Dictionary of Kedro pipelines.
        node: The Kedro ``Node`` for which the pipeline name is being retrieved.

    Returns:
        str: Name of the ``Pipeline`` that the ``Node`` belongs to.
    """
    for pipeline_name, pipeline in pipelines.items():
        if pipeline_name != "__default__":
            for pipeline_node in pipeline.nodes:
                if node.name == pipeline_node.name:
                    return pipeline_name


def load_assets_from_kedro_nodes(
    default_pipeline: Pipeline,
    catalog: DataCatalog,
    hook_manager: PluginManager,
    session_id: str,
) -> list[dg.AssetsDefinition]:
    """Load Kedro assets from a pipeline into Dagster.

    Args:
        default_pipeline: The Kedro default ``Pipeline``.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.

    Returns:
        List[AssetsDefinition]: List of Dagster assets.
    """

    LOGGER.info("Building asset list...")
    assets = []
    # Assets that are not generated through dagster are external and
    # registered with AssetSpec
    for external_asset_name in default_pipeline.inputs():
        if not external_asset_name.startswith("params:"):
            dataset = catalog._get_dataset(external_asset_name)
            metadata = getattr(dataset, "metadata", None) or {}
            description = metadata.pop("description", "")
            asset = dg.AssetSpec(
                external_asset_name,
                group_name="external",
                description=description,
                metadata=metadata,
            ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")
            assets.append(asset)

    multi_asset_node_dict = {}
    for node in default_pipeline.nodes:
        if len(node.outputs):
            node_pipeline_name = _get_node_pipeline_name(pipelines, node)

            asset = _define_node_multi_asset(
                node,
                node_pipeline_name,
                catalog,
                hook_manager,
                session_id,
            )
            assets.append(asset)
            multi_asset_node_dict[node.name] = asset

    return assets, multi_asset_node_dict
