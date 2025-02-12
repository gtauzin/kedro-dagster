"""Dagster op definitons from Kedro nodes."""

from logging import getLogger

import dagster as dg
from kedro.framework.project import pipelines
from kedro.io import MemoryDataset
from kedro.pipeline.node import Node
from pydantic import ConfigDict

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_asset_name,
    dagster_format,
    is_mlflow_enabled,
)

LOGGER = getLogger(__name__)


class NodeTranslator:
    def create_op(self, node: Node, retry_policy: dg.RetryPolicy | None = None):
        ins, params = {}, {}
        for dataset_name in node.inputs:
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
                ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))
            else:
                params[asset_name] = self._catalog.load(dataset_name)

        out = {}
        for dataset_name in node.outputs:
            asset_name = dagster_format(dataset_name)
            metadata, description = None, None
            io_manager_key = "io_manager"

            if asset_name in self._catalog.list():
                dataset = self._catalog._get_dataset(asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{asset_name}_io_manager"

            out[asset_name] = dg.Out(
                io_manager_key=io_manager_key,
                metadata=metadata,
                description=description,
            )

        # Node parameters are mapped to Dagster configs
        NodeParametersConfig = _create_pydantic_model_from_dict(
            params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

        op_name = dagster_format(node.name)

        @dg.op(
            name=f"{op_name}_asset",
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins,
            out=out,
            tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
            retry_policy=retry_policy,
        )
        def node_asset_op(context, config: NodeParametersConfig, **inputs):
            # Logic to execute the Kedro node
            context.log.info(f"Running node `{node.name}` in asset.")

            inputs |= config.model_dump()

            outputs = node.run(inputs)

            if len(outputs) == 1:
                return list(outputs.values())[0]
            elif len(outputs) > 1:
                return tuple(outputs.values())

        required_resource_keys = None
        if is_mlflow_enabled():
            required_resource_keys = {"mlflow"}

        @dg.op(
            name=f"{op_name}_graph",
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins | {"before_pipeline_run_hook_output": dg.In(dagster_type=dg.Nothing)},
            out=out | {f"{op_name}_after_pipeline_run_hook_input": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
            tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
            retry_policy=retry_policy,
        )
        def node_graph_op(context, config: NodeParametersConfig, **inputs):
            # Logic to execute the Kedro node
            context.log.info(f"Running node `{node.name}` in graph.")

            inputs |= config.model_dump()

            # TODO: Should is_async be False?
            self._hook_manager.hook.before_node_run(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                is_async=False,
                session_id=self._session_id,
            )

            try:
                outputs = node.run(inputs)

            except Exception as exc:
                self._hook_manager.hook.on_node_error(
                    error=exc,
                    node=node,
                    catalog=self._catalog,
                    inputs=inputs,
                    is_async=False,
                    session_id=self._session_id,
                )
                raise exc

            # TODO: Should is_async be False?
            self._hook_manager.hook.after_node_run(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                outputs=outputs,
                is_async=False,
                session_id=self._session_id,
            )

            for output_asset_name in node.outputs:
                context.log_event(dg.AssetMaterialization(asset_key=output_asset_name))

            if len(outputs) > 0:
                return tuple(outputs.values()) + (None,)

            return None

        return node_asset_op, node_graph_op

    def create_asset(
        self,
        node: Node,
        op: dg.OpDefinition,
        partition_def: dg.PartitionsDefinition | None = None,
        partition_mappings: dict[str, dg.PartitionMapping] | None = None,
        backfill_policy: dg.BackfillPolicy | None = None,
    ):
        """Create a Dagster asset from a Kedro node.

        Args:
            node: The Kedro node to wrap into an asset.

        Returns:
            AssetDefinition: A Dagster asset.
        """
        keys_by_input_name = {}
        for dataset_name in node.inputs:
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
                keys_by_input_name[asset_name] = dg.AssetKey(asset_name)

        keys_by_output_name = {}
        for dataset_name in node.outputs:
            asset_name = dagster_format(dataset_name)
            keys_by_output_name[asset_name] = dg.AssetKey(asset_name)

        node_asset = dg.AssetsDefinition.from_op(
            op,
            keys_by_input_name=keys_by_input_name,
            keys_by_output_name=keys_by_output_name,
            group_name=_get_node_pipeline_name(pipelines, node),
            partitions_def=partition_def,
            partition_mappings=partition_mappings,
            backfill_policy=backfill_policy,
        )

        return node_asset

    def translate_nodes(self):
        """Translate Kedro nodes into Dagster ops."""
        default_pipeline = pipelines.get("__default__")

        # Assets that are not generated through dagster are external and
        # registered with AssetSpec
        for external_dataset_name in default_pipeline.inputs():
            external_asset_name = dagster_format(external_dataset_name)
            if _is_asset_name(external_asset_name):
                dataset = self._catalog._get_dataset(external_asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

                io_manager_key = "io_manager"
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{external_asset_name}_io_manager"

                asset = dg.AssetSpec(
                    external_asset_name,
                    group_name="external",
                    description=description,
                    metadata=metadata,
                ).with_io_manager_key(io_manager_key=io_manager_key)
                self.named_assets_[external_asset_name] = asset

        # Create assets from Kedro nodes that have outputs
        for node in default_pipeline.nodes:
            op_name = dagster_format(node.name)
            asset_op, graph_op = self.create_op(node)
            self._named_ops[f"{op_name}_graph"] = graph_op

            if len(node.outputs):
                asset = self.create_asset(node, asset_op)
                self.named_assets_[op_name] = asset
