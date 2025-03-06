"""Translation of Kedro nodes to Dagster ops and assets."""

from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import DatasetNotFoundError, MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_asset_name,
    dagster_format,
    get_asset_key_from_dataset_name,
    is_mlflow_enabled,
    kedro_format,
)

if TYPE_CHECKING:
    from kedro.io import CatalogProtocol
    from kedro.pipeline.node import Node
    from pluggy import PluginManager


LOGGER = getLogger(__name__)


class NodeTranslator:
    """Translate Kedro nodes into Dagster ops and assets.

    Args:
        catalog (CatalogProtocol): Kedro catalog.
        hook_manager (PluginManager): Kedro hook manager.
        session_id (str): Kedro session ID.
        named_resources (dict[str, ResourceDefinition]): Named dagster resources.

    """

    def __init__(
        self,
        pipelines: dict[str, Pipeline],
        catalog: "CatalogProtocol",
        hook_manager: "PluginManager",
        session_id: str,
        named_resources: dict[str, dg.ResourceDefinition],
        env: str,
    ):
        self._pipelines = pipelines
        self._catalog = catalog
        self._hook_manager = hook_manager
        self._session_id = session_id
        self._named_resources = named_resources
        self._env = env

    def _get_node_parameters_config(self, node: "Node") -> dg.Config:
        """Get the node parameters as a Dagster config.

        Args:
            node (Node): Kedro node.

        Returns:
            Config: A Dagster config representing the node parameters.

        """
        params = {}
        for dataset_name in node.inputs:
            asset_name = dagster_format(dataset_name)
            if not _is_asset_name(asset_name):
                params[asset_name] = self._catalog.load(dataset_name)

        # Node parameters are mapped to Dagster configs
        return _create_pydantic_model_from_dict(
            name="ParametersConfig",
            params=params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

    def _get_out_asset_params(self, dataset_name: str, asset_name: str) -> dict[str, Any]:
        """Get the output asset parameters.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.

        Returns:
            dict[str, Any]: The output asset parameters.

        """
        metadata, description = None, None
        io_manager_key = "io_manager"

        if asset_name in self.asset_names:
            try:
                dataset = self._catalog._get_dataset(dataset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{self._env}__{asset_name}_io_manager"

            except DatasetNotFoundError:
                pass

        return dict(
            io_manager_key=io_manager_key,
            metadata=metadata,
            description=description,
        )

    @property
    def asset_names(self) -> list[str]:
        if not hasattr(self, "_asset_names"):
            asset_names = []
            for dataset_name in sum(self._pipelines.values()).datasets():
                asset_name = dagster_format(dataset_name)
                asset_names.append(asset_name)

            asset_names = list(set(asset_names))
            self._asset_names = asset_names

        return self._asset_names

    def create_op(self, node: "Node", retry_policy: dg.RetryPolicy | None = None) -> dg.OpDefinition:
        """Create a Dagster op from a Kedro node.

        The created op is meant to be used in a Dagster graph.

        Args:
            node (Node): Kedro node.
            retry_policy (RetryPolicy): Dagster retry policy for the op.

        Returns:
            OpDefinition: A Dagster op.

        """
        ins = {}
        for dataset_name in node.inputs:
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
                ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))

        out = {}
        for dataset_name in node.outputs:
            asset_name = dagster_format(dataset_name)
            out_asset_params = self._get_out_asset_params(dataset_name, asset_name)
            out[asset_name] = dg.Out(**out_asset_params)

        NodeParametersConfig = self._get_node_parameters_config(node)
        op_name = dagster_format(node.name)

        required_resource_keys = []
        for dataset_name in node.inputs + node.outputs:
            asset_name = dagster_format(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                required_resource_keys.append(f"{self._env}__{asset_name}_io_manager")

        if is_mlflow_enabled():
            required_resource_keys.append("mlflow")

        @dg.op(
            name=f"{op_name}_graph",
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins | {"before_pipeline_run_hook_output": dg.In(dagster_type=dg.Nothing)},
            out=out | {f"{op_name}_after_pipeline_run_hook_input": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
            tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
            retry_policy=retry_policy,
        )
        def node_graph_op(context: dg.OpExecutionContext, config: NodeParametersConfig, **inputs):  # type: ignore[no-untyped-def, valid-type]
            # Logic to execute the Kedro node with hooks
            context.log.info(f"Running node `{node.name}` in graph.")

            inputs |= config.model_dump()  # type: ignore[attr-defined]
            inputs = {kedro_format(input_asset_name): input_asset for input_asset_name, input_asset in inputs.items()}

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

            self._hook_manager.hook.after_node_run(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                outputs=outputs,
                is_async=False,
                session_id=self._session_id,
            )

            for output_dataset_name in node.outputs:
                output_asset_key = get_asset_key_from_dataset_name(output_dataset_name, self._env)
                context.log_event(dg.AssetMaterialization(asset_key=output_asset_key))

            if len(outputs) > 0:
                return tuple(outputs.values()) + (None,)

            return None

        return node_graph_op

    # TODO: Map partition_mappings per asset in AssetOut
    def create_asset(
        self,
        node: "Node",
        partition_def: dg.PartitionsDefinition | None = None,
        partition_mappings: dict[str, dg.PartitionMapping] | None = None,
        backfill_policy: dg.BackfillPolicy | None = None,
    ) -> dg.AssetsDefinition:
        """Create a Dagster asset from a Kedro node.

        Args:
            node (Node): The Kedro node to wrap into an asset.
            partition_def (PartitionsDefinition | None): Partitions definition for the asset.
            partition_mappings (dict[str, PartitionMapping] | None): Partition mappings for the asset.
            backfill_policy (BackfillPolicy | None): Backfill policy for the asset

        Returns:
            AssetsDefinition: A Dagster asset.
        """

        ins = {}
        for dataset_name in node.inputs:
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                ins[asset_name] = dg.AssetIn(key=asset_key)

        outs = {}
        for dataset_name in node.outputs:
            asset_name = dagster_format(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
            out_asset_params = self._get_out_asset_params(dataset_name, asset_name)
            outs[asset_name] = dg.AssetOut(key=asset_key, **out_asset_params)

        NodeParametersConfig = self._get_node_parameters_config(node)
        name = dagster_format(node.name)

        required_resource_keys = None
        if is_mlflow_enabled():
            required_resource_keys = {"mlflow"}

        @dg.multi_asset(
            name=name,
            description=f"Kedro node {node.name} wrapped as a Dagster multi asset.",
            group_name=_get_node_pipeline_name(self._pipelines, node),
            ins=ins,
            outs=outs,
            required_resource_keys=required_resource_keys,
            op_tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
            partitions_def=partition_def,
            backfill_policy=backfill_policy,
        )
        def dagster_asset(context: dg.AssetExecutionContext, config: NodeParametersConfig, **inputs):  # type: ignore[no-untyped-def, valid-type]
            # Logic to execute the Kedro node
            context.log.info(f"Running node `{node.name}` in asset.")

            inputs |= config.model_dump()  # type: ignore[attr-defined]
            inputs = {kedro_format(input_asset_name): input_asset for input_asset_name, input_asset in inputs.items()}

            outputs = node.run(inputs)

            if len(outputs) == 1:
                return list(outputs.values())[0]
            elif len(outputs) > 1:
                return tuple(outputs.values())

        return dagster_asset

    def to_dagster(self) -> tuple[dict[str, dg.OpDefinition], dict[str, dg.AssetSpec | dg.AssetsDefinition]]:
        """Translate Kedro nodes into Dagster ops.

        Returns:
            dict[str, dg.OpDefinition]: Dictionary of named ops.
            dict[str, dg.AssetSpec | dg.AssetsDefinition]]: Dictionary of named assets.
        """
        default_pipeline = sum(self._pipelines.values(), start=Pipeline([]))

        # Assets that are not generated through dagster are external and
        # registered with AssetSpec
        named_assets = {}
        for external_dataset_name in default_pipeline.inputs():
            external_asset_name = dagster_format(external_dataset_name)
            if _is_asset_name(external_asset_name):
                dataset = self._catalog._get_dataset(external_asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

                io_manager_key = "io_manager"
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{self._env}__{external_asset_name}_io_manager"

                external_asset_key = get_asset_key_from_dataset_name(external_dataset_name, env=self._env)
                external_asset = dg.AssetSpec(
                    key=external_asset_key,
                    group_name="external",
                    description=description,
                    metadata=metadata,
                ).with_io_manager_key(io_manager_key=io_manager_key)
                named_assets[external_asset_name] = external_asset

        # Create assets from Kedro nodes that have outputs
        named_ops = {}
        for node in default_pipeline.nodes:
            op_name = dagster_format(node.name)
            graph_op = self.create_op(node)
            named_ops[f"{op_name}_graph"] = graph_op

            if len(node.outputs):
                asset = self.create_asset(node)
                named_assets[op_name] = asset

        return named_ops, named_assets
