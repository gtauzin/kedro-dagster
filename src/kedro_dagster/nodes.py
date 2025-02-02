"""Dagster op definitons from Kedro nodes."""

from logging import getLogger

import dagster as dg
from kedro.framework.project import pipelines
from kedro.pipeline.node import Node
from pydantic import ConfigDict

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _is_asset_name,
)

LOGGER = getLogger(__name__)


class NodeTranslator:
    def _translate_node(self, node: Node):
        ins, params = {}, {}
        for asset_name in node.inputs:
            if _is_asset_name(asset_name):
                ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))
            else:
                params[asset_name] = self._context.catalog.load(asset_name)

        # Node parameters are mapped to Dagster configs
        NodeParametersConfig = _create_pydantic_model_from_dict(
            params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

        # TODO: dagster tags are dicts
        # TODO: Should is_async be False?
        # TODO: Should I define outs?
        @dg.op(
            name=node.name,
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins,
            required_resource_keys={"pipeline_hook"},
            tags={"node_tags": node.tags},
        )
        def node_op(context, config: NodeParametersConfig, **inputs) -> dg.Nothing:
            # Logic to execute the Kedro node

            inputs |= config.model_dump()

            self._context._hook_manager.hook.before_node_run(
                node=node,
                catalog=self._context.catalog,
                inputs=inputs,
                is_async=False,
                session_id=self._session.session_id,
            )

            try:
                outputs = node.run(inputs)

            except Exception as exc:
                self._context._hook_manager.hook.on_node_error(
                    error=exc,
                    node=node,
                    catalog=self._context.catalog,
                    inputs=inputs,
                    is_async=False,
                    session_id=self._session.session_id,
                )
                raise exc

            self._context._hook_manager.hook.after_node_run(
                node=node,
                catalog=self._context.catalog,
                inputs=inputs,
                outputs=outputs,
                is_async=False,
                session_id=self._session.session_id,
            )

            for output_asset_name, output_asset in outputs.items():
                context.resources.pipeline_hook.add_run_results(output_asset_name, output_asset)

            if len(outputs) > 1:
                return tuple(outputs.values())

            elif len(outputs) == 1:
                return list(outputs.values())[0]

        return node_op

    def translate_nodes(self):
        """Translate Kedro nodes into Dagster ops."""
        default_pipeline = pipelines.get("__default__")
        for node in default_pipeline.nodes:
            op = self._translate_node(node)
            self._named_ops[node.name] = op
