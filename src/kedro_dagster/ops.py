"""Dagster op definitons from Kedro nodes."""

import dagster as dg
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pluggy import PluginManager
from pydantic import ConfigDict

from kedro_dagster.utils import _create_pydantic_model_from_dict, _include_mlflow


def _define_node_op(
    node: Node,
    catalog: DataCatalog,
    hook_manager: PluginManager,
    session_id: str,
) -> dg.OpDefinition:
    """Wrap a kedro Node inside a Dagster op.

    Args:
        node: The Kedro ``Node`` for which a Dagster multi asset is
        being created.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.

    Returns:
        OpDefinition: Dagster op definition that wraps the Kedro ``Node``.
    """
    ins, params = {}, {}
    for asset_name in node.inputs:
        if not asset_name.startswith("params:"):
            ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))
        else:
            params[asset_name] = catalog.load(asset_name)

    ins["before_pipeline_run_hook_output"] = dg.In(
        asset_key=dg.AssetKey("before_pipeline_run_hook_output"),
        dagster_type=dg.Nothing,
    )

    # Node parameters are mapped to Dagster configs
    NodeParametersConfig = _create_pydantic_model_from_dict(
        params,
        __base__=dg.Config,
        __config__=ConfigDict(extra="allow", frozen=False),
    )

    # Define an op from a Kedro node
    # TODO: dagster tags are dicts
    @dg.op(
        name=node.name,
        description=f"Kedro node {node.name} wrapped as a Dagster op.",
        ins=ins,
        out={"after_pipeline_run_hook_input": dg.Out(dagster_type=dg.Nothing)},
        required_resource_keys={"mlflow"} if _include_mlflow() else None,
        # tags=node.tags,
    )
    def dagster_op(config: NodeParametersConfig, **inputs) -> dg.Nothing:
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

        return None

    return dagster_op


def load_ops_from_kedro_nodes(
    default_pipeline: Pipeline,
    catalog: DataCatalog,
    hook_manager: PluginManager,
    session_id: str,
) -> dict[str, dg.OpDefinition]:
    """Load Kedro ops from a pipeline into Dagster.

    Args:
        default_pipeline: The Kedro default ``Pipeline``.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.

    Returns:
        Dict[str, OpDefinition]: Dictionary of Dagster ops.
    """
    logger = dg.get_dagster_logger()

    logger.info("Building op list...")
    # Assets that are not generated through dagster are external and
    # registered with AssetSpec
    op_node_dict = {}
    for node in default_pipeline.nodes:
        if not len(node.outputs):
            op = _define_node_op(
                node,
                catalog,
                hook_manager,
                session_id,
            )
            op_node_dict[node.name] = op

    return op_node_dict
