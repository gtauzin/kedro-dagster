"""Define Dagster assets from Kedro nodes."""

from collections.abc import Callable

from dagster import (
    AssetIn,
    AssetOut,
    AssetSpec,
    Config,
    Nothing,
    get_dagster_logger,
    multi_asset,
)
from kedro.framework.project import pipelines
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pydantic import ConfigDict

from kedro_dagster.utils import _create_pydantic_model_from_dict, _include_mlflow


def _define_node_multi_asset(
    node: Node,
    pipeline_name: str,
    catalog: DataCatalog,
    hook_manager,
    session_id: str,
) -> Callable:
    """Wrap a kedro Node inside a Dagster multi asset.

    Args:
        node: Kedro node for which a Prefect task is being created.
        pipeline_name: Name of the pipeline that the node belongs to.
        catalog: DataCatalog object that contains the datasets used by the node.
        session_id: ID of the Kedro session that the node will be executed in.

    Returns: Dagster multi assset function that wraps the Kedro node.
    """
    ins, params = {}, {}
    for asset_name in node.inputs:
        if not asset_name.startswith("params:"):
            ins[asset_name] = AssetIn(
                key=asset_name,
                # input_manager_key=f"{asset_name}_io_manager",
            )
        else:
            params[asset_name] = catalog.load(asset_name)

    # If the node has no outputs, we still define it as an assets, so that it appears
    # on the dagster asset lineage graph
    outs = {
        node.name: AssetOut(
            key=node.name,
            dagster_type=Nothing,
            description=f"Untangible asset created for kedro {node.name} node.",
        )
    }
    if len(node.outputs):
        outs = {}
        for asset_name in node.outputs:
            metadata, description = None, None
            if asset_name in catalog.list():
                dataset = catalog._get_dataset(asset_name)
                metadata = dataset.metadata or {}
                description = metadata.pop("description", "")

            io_manager_key = "io_manager"
            if asset_name in catalog.list() and not isinstance(catalog._get_dataset(asset_name), MemoryDataset):
                io_manager_key = f"{asset_name}_io_manager"

            outs[asset_name] = AssetOut(
                key=asset_name,
                description=description,
                metadata=metadata,
                io_manager_key=io_manager_key,
            )

    # Node parameters are mapped to Dagster configs
    NodeParametersConfig = _create_pydantic_model_from_dict(
        params,
        __base__=Config,
        __config__=ConfigDict(extra="allow", frozen=False),
    )

    # Define a multi_asset from a Kedro node
    @multi_asset(
        name=node.name,
        group_name=pipeline_name,
        ins=ins,
        outs=outs,
        required_resource_keys={"mlflow"} if _include_mlflow() else None,
        op_tags=node.tags,
    )
    def dagster_asset(config: NodeParametersConfig, **inputs):
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
        node: Kedro node for which the pipeline name is being retrieved.

    Returns: Name of the pipeline that the node belongs to.
    """
    for pipeline_name, pipeline in pipelines.items():
        if pipeline_name != "__default__":
            for pipeline_node in pipeline.nodes:
                if node.name == pipeline_node.name:
                    return pipeline_name


def load_assets_from_kedro_nodes(default_pipeline: Pipeline, catalog: DataCatalog, hook_manager, session_id: str):
    """Load Kedro assets from a pipeline into Dagster.

    Args:
        catalog: A Kedro DataCatalog.
        session_id: A string representing Kedro session ID.

    Returns:
        List[AssetDefinition]: List of Dagster assets.
    """
    logger = get_dagster_logger()

    logger.info("Building asset list...")
    assets = []
    # Assets that are not generated through dagster are external and
    # registered with AssetSpec
    for external_asset_name in default_pipeline.inputs():
        if not external_asset_name.startswith("params:"):
            dataset = catalog._get_dataset(external_asset_name)
            metadata = dataset.metadata or {}
            description = metadata.pop("description", "")
            asset = AssetSpec(
                external_asset_name,
                group_name="external",
                description=description,
                metadata=metadata,
            ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")
            assets.append(asset)

    for node in default_pipeline.nodes:
        node_pipeline_name = _get_node_pipeline_name(pipelines, node)

        asset = _define_node_multi_asset(
            node,
            node_pipeline_name,
            catalog,
            hook_manager,
            session_id,
        )
        assets.append(asset)

    return assets
