"""Define Dagster assets from Kedro nodes."""

from collections.abc import Callable
from pathlib import Path

from dagster import (
    AssetIn,
    AssetOut,
    AssetSpec,
    Config,
    get_dagster_logger,
    multi_asset,
)
from kedro.framework.hooks.manager import _create_hook_manager
from kedro.framework.project import pipelines
from kedro.framework.startup import bootstrap_project
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline.node import Node
from kedro.runner.runner import _call_node_run
from pydantic import create_model

from .utils import kedro_init



def define_node_multi_asset(
    node: Node,
    pipeline_name: str,
    catalog: DataCatalog,
    session_id: str,
    metadata: dict,
) -> Callable:
    """Wrap a kedro Node inside a Dagster multi asset.

    Args:
        node: Kedro node for which a Prefect task is being created.
        pipeline_name: Name of the pipeline that the node belongs to.
        catalog: DataCatalog object that contains the datasets used by the node.
        session_id: ID of the Kedro session that the node will be executed in.
        memory_asset_names: List of dataset names that are defined in the `catalog`
        as `MemoryDataset`s.
        metadata: Dicto mapping asset names to kedro datasets metadata.

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
            description=f"Untangible asset created for kedro {node.name} node.",
        )
    }
    if len(node.outputs):
        outs = {}
        for asset_name in node.outputs:
            asset_metadata = metadata.get(asset_name) or {}
            asset_description = asset_metadata.pop("description", "")
            io_manager_key = "io_manager"
            if (
                asset_name in catalog.list()
                and not isinstance(catalog._get_dataset(asset_name), MemoryDataset)
            ):
                io_manager_key=f"{asset_name}_io_manager"

            outs[asset_name] = AssetOut(
                key=asset_name,
                description=asset_description, 
                metadata=asset_metadata,
                io_manager_key=io_manager_key,
            )

    # Node parameters are mapped to Dagster configs
    NodeParameters = create_model(
        "MemoryDatasetConfig",
        **{param_name: (type(param), param) for param_name, param in params.items()},
    )

    class NodeParametersConfig(NodeParameters, Config, extra="allow", frozen=False):
        pass

    # Define a multi_asset from a Kedro node
    @multi_asset(
        name=node.name,
        group_name=pipeline_name,
        ins=ins,
        outs=outs,
        op_tags=node.tags,
    )
    def dagster_asset(config: NodeParametersConfig, **inputs):  # TODO: Use context?
        # Logic to execute the Kedro node
        # TODO: Using `_call_node_run` instead of `run_node` as we do not rely on the catalog
        # but on IOManagers. However, in practice, some kedro hooks related to catalog steps
        # are defacto ignored.

        inputs |= config.model_dump()

        outputs = _call_node_run(
            node,
            catalog,
            inputs,
            False,
            _create_hook_manager(),
            session_id,
        )

        if len(outputs) > 1:
            return tuple(outputs.values())
        
        elif len(outputs) == 1:
            return list(outputs.values())[0]

    return dagster_asset


def get_node_pipeline_name(pipelines, node):
    """Return the name of the pipeline that a node belongs to.

    Args:
        pipelines: Dictionary of Kedro pipelines.
        node: Kedro node for which the pipeline name is being retrieved.

    Returns: Name of the pipeline that the node belongs to.
    """
    for pipeline_name, pipeline in pipelines.items():
        for pipeline_node in pipeline.nodes:
            if node.name == pipeline_node.name:
                return pipeline_name


def load_assets_from_kedro_nodes(env: str | None = None):
    """Load Kedro assets from a pipeline into Dagster.

    Args
        env: Kedro environment to load the catalog and parameters from.

    Returns: List of Dagster assets.
    """
    logger = get_dagster_logger()
    pipeline_name = "__default__"
    project_path = Path.cwd()

    project_metadata = bootstrap_project(project_path)
    logger.info("Project name: %s", project_metadata.project_name)

    logger.info("Initializing Kedro...")
    config_loader, catalog, session_id = kedro_init(
        project_path=project_path, env=env
    )
    dataset_config = config_loader.get("catalog")
    pipeline = pipelines.pop(pipeline_name)

    logger.info("Building asset list...")
    metadata = {
        asset_name: asset_config.pop("metadata", None)
        for asset_name, asset_config in dataset_config.items()
    }
    assets = []
    # Assets that are not generated through dagster are external and
    # registered with AssetSpec
    for external_asset_name in pipeline.inputs():
        if not external_asset_name.startswith("params:"):
            asset_metadata = metadata.get(external_asset_name) or {}
            asset_description = asset_metadata.pop("description", "")
            asset = AssetSpec(
                external_asset_name,
                group_name="external",
                description=asset_description,
                metadata=asset_metadata,
            ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")
            assets.append(asset)

    for node in pipeline.nodes:
        node_pipeline_name = get_node_pipeline_name(pipelines, node)

        asset = define_node_multi_asset(
            node,
            node_pipeline_name,
            catalog,
            session_id,
            metadata,
        )
        assets.append(asset)

    return assets
