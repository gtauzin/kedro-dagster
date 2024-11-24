"""Define Dagster assets from Kedro nodes."""

from collections.abc import Callable

from dagster import (
    AssetKey,
    Config,
    In,
    Nothing,
    get_dagster_logger,
    op,
)
from kedro.framework.project import pipelines
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pydantic import ConfigDict

from kedro_dagster.utils import _create_pydantic_model_from_dict, _include_mlflow


def _define_node_op(
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
            ins[asset_name] = In(asset_key=AssetKey(asset_name))
        else:
            params[asset_name] = catalog.load(asset_name)

    ins["before_pipeline_run_hook_result"] = In(
        asset_key=AssetKey("before_pipeline_run_hook_result"),
        dagster_type=Nothing,
    )

    # Node parameters are mapped to Dagster configs
    NodeParametersConfig = _create_pydantic_model_from_dict(
        params,
        __base__=Config,
        __config__=ConfigDict(extra="allow", frozen=False),
    )

    # Define an op from a Kedro node
    # TODO: dagster tags are dicts
    @op(
        name=node.name,
        description=f"Kedro node {node.name} wrapped as a Dagster op.",
        ins=ins,
        required_resource_keys={"mlflow"} if _include_mlflow() else None,
        # tags=node.tags,
    )
    def dagster_op(config: NodeParametersConfig, **inputs):
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

    return dagster_op


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


def load_ops_from_kedro_nodes(default_pipeline: Pipeline, catalog: DataCatalog, hook_manager, session_id: str):
    """Load Kedro assets from a pipeline into Dagster.

    Args:
        catalog: A Kedro DataCatalog.
        session_id: A string representing Kedro session ID.

    Returns:
        List[AssetDefinition]: List of Dagster assets.
    """
    logger = get_dagster_logger()

    logger.info("Building op list...")
    # Assets that are not generated through dagster are external and
    # registered with AssetSpec
    op_node_dict = {}
    for node in default_pipeline.nodes:
        if not len(node.outputs):
            node_pipeline_name = _get_node_pipeline_name(pipelines, node)

            op = _define_node_op(
                node,
                node_pipeline_name,
                catalog,
                hook_manager,
                session_id,
            )
            op_node_dict[node.name] = op

    return op_node_dict
