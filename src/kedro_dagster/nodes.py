"""Translation of Kedro nodes to Dagster ops and assets."""

from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import DatasetNotFoundError, MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict

from kedro_dagster.datasets.partitioned_dataset import DagsterPartitionedDataset
from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    get_partition_mapping,
    format_dataset_name,
    format_node_name,
    get_asset_key_from_dataset_name,
    is_mlflow_enabled,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.io import CatalogProtocol
    from kedro.pipeline.node import Node
    from pluggy import PluginManager


LOGGER = getLogger(__name__)


class NodeTranslator:
    """Translate Kedro nodes into Dagster ops and assets.

    Args:
        pipelines (list[Pipeline]): List of Kedro pipelines.
        catalog (CatalogProtocol): Kedro catalog instance.
        hook_manager (PluginManager): Kedro hook manager.
        session_id (str): Kedro session ID.
        named_resources (dict[str, ResourceDefinition]): Named Dagster resources.
        env (str): Kedro environment.
    """

    def __init__(
        self,
        pipelines: list[Pipeline],
        catalog: "CatalogProtocol",
        hook_manager: "PluginManager",
        session_id: str,
        asset_partitions: dict[str, dict[str, dg.PartitionsDefinition | dg.PartitionMapping | None]],
        named_resources: dict[str, dg.ResourceDefinition],
        env: str,
    ):
        self._pipelines = pipelines
        self._catalog = catalog
        self._hook_manager = hook_manager
        self._session_id = session_id
        self._asset_partitions = asset_partitions
        self._named_resources = named_resources
        self._env = env

    def _get_node_partitions_definition(self, node: "Node") -> dg.PartitionsDefinition | None:
        """Get the multi-partitions definition for a node if it has partitioned datasets.

        Args:
            node (Node): Kedro node.

        Returns:
            MultiPartitionsDefinition | None: The multi-partition definition or None.
        """
        partitioned_assets = {}

        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            asset_partition = self._asset_partitions.get(asset_name, None)
            if asset_partition is not None:
                partitions_def = asset_partition["partitions_def"]
                partitioned_assets[asset_name] = partitions_def

        if not partitioned_assets:
            return None

        if len(partitioned_assets) == 1:
            return next(iter(partitioned_assets.values()))

        print("PARTITIONED ASSETS:", partitioned_assets)

        return dg.MultiPartitionsDefinition(partitions_defs=partitioned_assets)

    def _get_node_parameters_config(self, node: "Node") -> dg.Config:
        """Get the node parameters as a Dagster config.

        Args:
            node (Node): Kedro node.

        Returns:
            Config: A Dagster config representing the node parameters.
        """
        params = {}
        for dataset_name in node.inputs:
            if _is_param_name(dataset_name):
                params[dataset_name] = self._catalog.load(dataset_name)

        params["pipeline_input_asset_names"] = []

        # Node parameters are mapped to Dagster configs
        return _create_pydantic_model_from_dict(
            name="ParametersConfig",
            params=params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

    def _get_in_op_params(self, dataset_name: str, asset_name: str) -> dict[str, Any]:
        """Get the input op parameters for a dataset.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.
        Returns:
            dict[str, Any]: The input op parameters.
        """
        in_ops_params = {}

        if asset_name in self.asset_names:
            if asset_name in self._asset_partitions:
                in_ops_params["asset_partitions"] = {asset_name}

        return in_ops_params

    def _get_out_op(self, dataset_name: str, asset_name: str) -> dg.Out:
        """Get the output op for a dataset.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.
        Returns:
            Out: The output op.
        """
        out_ops_params = self._get_out_asset_params(dataset_name, asset_name)

        # if asset_name in self._asset_partitions:
        #     return dg.Out(**out_ops_params) # DynamicOutput

        return dg.DynamicOut(**out_ops_params)

    def _get_in_asset_params(self, dataset_name: str, asset_name: str, output_dataset_names: list[str]) -> dict[str, Any]:
        """Get the input asset parameters for a dataset.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.

        Returns:
            dict[str, Any]: The input asset parameters.
        """
        in_asset_params = {}

        if asset_name in self.asset_names:
            if asset_name in self._asset_partitions:
                partition_mappings = self._asset_partitions[asset_name]["partition_mappings"]
                if partition_mappings is not None:
                    partition_mapping = get_partition_mapping(
                        partition_mappings,
                        asset_name,
                        downstream_dataset_names=output_dataset_names,
                        config_resolver=self._catalog._config_resolver,
                    )

                    if partition_mapping is not None:
                        in_asset_params["partition_mapping"] = partition_mapping

        return in_asset_params

    def _get_out_asset_params(self, dataset_name: str, asset_name: str, return_kinds: bool = False) -> dict[str, Any]:
        """Get the output asset parameters for a dataset.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.
            return_kinds (bool): Whether to return the kinds of the asset. Defaults to False.

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

        out_asset_params = dict(
            io_manager_key=io_manager_key,
            metadata=metadata,
            description=description,
        )

        if return_kinds:
            kinds = {"kedro"}
            if is_mlflow_enabled():
                kinds.add("mlflow")
            out_asset_params["kinds"] = kinds

        return out_asset_params

    # TODO: Put in utils?
    @property
    def asset_names(self) -> list[str]:
        """Return a list of all asset names in the pipelines."""
        if not hasattr(self, "_asset_names"):
            asset_names = []
            for dataset_name in sum(self._pipelines, Pipeline([])).datasets():
                asset_name = format_dataset_name(dataset_name)
                asset_names.append(asset_name)

            asset_names = list(set(asset_names))
            self._asset_names = asset_names

        return self._asset_names

    def create_op(self, node: "Node") -> dg.OpDefinition:
        """Create a Dagster op from a Kedro node for use in a Dagster graph.

        Args:
            node (Node): Kedro node.

        Returns:
            OpDefinition: A Dagster op.
        """
        ins = {}
        for dataset_name in node.inputs:
            if not _is_param_name(dataset_name):
                asset_name = format_dataset_name(dataset_name)
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                in_ops_params = self._get_in_op_params(dataset_name, asset_name)
                ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_key), **in_ops_params)

        out = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            out_ops = self._get_out_op(dataset_name, asset_name)
            out[asset_name] = out_ops

        NodeParametersConfig = self._get_node_parameters_config(node)
        op_name = format_node_name(node.name)

        required_resource_keys = []
        for dataset_name in node.inputs + node.outputs:
            asset_name = format_dataset_name(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                required_resource_keys.append(f"{self._env}__{asset_name}_io_manager")

        if is_mlflow_enabled():
            required_resource_keys.append("mlflow")

        print("OP NAME", op_name)

        @dg.op(
            name=f"{op_name}",
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins | {
                "partition_key": dg.In(
                    dagster_type=dg.String,
                    default_value="__default__",
                    description="Partition key to run this op for; '__default__' if non-partitioned.",
                ),
                "before_pipeline_run_hook_output": dg.In(dagster_type=dg.Nothing)
            },
            out=out | {f"{op_name}_after_pipeline_run_hook_input": dg.DynamicOut(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
            tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
        )
        def node_graph_op(
            context: dg.OpExecutionContext,
            config: NodeParametersConfig,
            partition_key: str | None = None,
            **inputs
        ):  # type: ignore[no-untyped-def, valid-type]
            """Execute the Kedro node as a Dagster op."""
            context.log.info(f"Running node `{node.name}` in graph.")

            print("PARTITION KEY", partition_key)

            context.log.info(f"Using partition key: {partition_key}")

            input_config = config.model_dump()  # type: ignore[attr-defined]
            pipeline_input_asset_names = input_config.pop("pipeline_input_asset_names", [])
            inputs |= input_config

            node_inputs = {}
            for input_asset_name, input_asset in inputs.items():
                input_dataset_name = unformat_asset_name(input_asset_name)

                node_input_val = input_asset
                if not _is_param_name(input_dataset_name):
                    if input_asset_name in pipeline_input_asset_names:
                        node_input_val = input_asset
                    elif partition_key == "__default__":
                        node_input_val = input_asset[0]
                    else:
                        partition_keys = self._asset_partitions[input_asset_name].get_partition_keys()
                        partition_idx = partition_keys.index(partition_key)
                        node_input_val = input_asset[partition_idx]


                if input_asset_name in self._asset_partitions:
                    asset_partition_meta = self._asset_partitions[input_asset_name]
                    partition_keys = asset_partition_meta["partitions_def"].get_partition_keys()

                    # Resolve mapping if downstream asset partitioned
                    if any(out in self._asset_partitions for out in node.outputs):
                        for downstream_output_name in node.outputs:
                            downstream_asset = format_dataset_name(downstream_output_name)
                            mapping = asset_partition_meta["partition_mapping"].get(downstream_asset)

                            if mapping:
                                # Get upstream partitions corresponding to current downstream partition
                                upstream_partitions = mapping.get_upstream_partitions_for_partition_range(
                                    partition_key, partition_key
                                )
                                context.log.info(
                                    f"Mapping upstream {input_asset_name} → downstream {downstream_asset}: "
                                    f"{upstream_partitions}"
                                )
                                node_input_val = [
                                    input_asset[partition_keys.index(up)] for up in upstream_partitions
                                ]
                                break
                    else:
                        # Default: 1-to-1 mapping by same partition key
                        if partition_key in partition_keys:
                            node_input_val = input_asset[partition_keys.index(partition_key)]

                node_inputs[input_dataset_name] = node_input_val


            self._hook_manager.hook.before_node_run(
                node=node,
                catalog=self._catalog,
                inputs=node_inputs,
                is_async=False,  # TODO: Should this be True?
                session_id=self._session_id,
            )

            try:
                node_outputs = node.run(node_inputs)

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
                outputs=node_outputs,
                is_async=False,
                session_id=self._session_id,
            )

            # Emit materializations and wrap everything in DynamicOutput
            outputs = []
            for output_dataset_name in node.outputs:
                output_asset_key = get_asset_key_from_dataset_name(output_dataset_name, self._env)
                output_asset_name = format_dataset_name(output_dataset_name)

                context.log_event(
                    dg.AssetMaterialization(
                        asset_key=output_asset_key,
                        partition=None if partition_key == "__default__" else partition_key,                    )
                )

                value = node_outputs[output_dataset_name]

                # Always wrap in DynamicOutput, even for non-partitioned assets
                outputs.append(
                    dg.DynamicOutput(
                        value=value,
                        mapping_key=str(partition_key),
                        output_name=output_asset_name,
                    )
                )

            # Trailing None DynamicOutput for after-pipeline-run hook
            outputs.append(
                dg.DynamicOutput(
                    value=None,
                    mapping_key=str(partition_key),
                    output_name=f"{op_name}_after_pipeline_run_hook_input",
                )
            )

            # Emit all DynamicOutputs, then a trailing None for after-hook input
            yield from outputs

        return node_graph_op

    def create_asset(self, node: "Node") -> dg.AssetsDefinition:
        """Create a Dagster asset from a Kedro node.

        Args:
            node (Node): The Kedro node to wrap into an asset.

        Returns:
            AssetsDefinition: A Dagster asset.
        """

        ins = {}
        for dataset_name in node.inputs:
            if not _is_param_name(dataset_name):
                asset_name = format_dataset_name(dataset_name)
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                in_asset_params = self._get_in_asset_params(dataset_name, asset_name, output_dataset_names=node.outputs)
                ins[asset_name] = dg.AssetIn(key=asset_key, **in_asset_params)

        outs = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
            out_asset_params = self._get_out_asset_params(dataset_name, asset_name, return_kinds=True)
            outs[asset_name] = dg.AssetOut(key=asset_key, **out_asset_params)

        NodeParametersConfig = self._get_node_parameters_config(node)

        required_resource_keys = None
        if is_mlflow_enabled():
            required_resource_keys = {"mlflow"}

        partitions_def = self._get_node_partitions_definition(node)

        @dg.multi_asset(
            name=f"{format_node_name(node.name)}_asset",
            description=f"Kedro node {node.name} wrapped as a Dagster multi asset.",
            group_name=_get_node_pipeline_name(node),
            ins=ins,
            outs=outs,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys,
            op_tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
        )
        def dagster_asset(context: dg.AssetExecutionContext, config: NodeParametersConfig, **inputs):  # type: ignore[no-untyped-def, valid-type]
            """Execute the Kedro node as a Dagster asset."""
            context.log.info(f"Running node `{node.name}` in asset.")

            inputs |= config.model_dump()  # type: ignore[attr-defined]
            partition_key = inputs.pop("partition_key", None)
            print("CONFIG ASSET PARTITION KEY", partition_key)

            inputs = {
                unformat_asset_name(input_asset_name): input_asset for input_asset_name, input_asset in inputs.items()
            }

            outputs = node.run(inputs)

            if len(outputs) == 1:
                return list(outputs.values())[0]
            elif len(outputs) > 1:
                return tuple(outputs.values())

        return dagster_asset

    def to_dagster(self) -> tuple[dict[str, dg.OpDefinition], dict[str, dg.AssetSpec | dg.AssetsDefinition]]:
        """Translate Kedro nodes into Dagster ops and assets.

        Returns:
            dict[str, dg.OpDefinition]: Dictionary of named ops.
            dict[str, dg.AssetSpec | dg.AssetsDefinition]]: Dictionary of named assets.
        """
        default_pipeline: Pipeline = sum(self._pipelines, start=Pipeline([]))

        print("INPUTS:", default_pipeline.inputs())
        print("OUTPUTS:", default_pipeline.outputs())

        # Assets that are not generated through dagster are external and
        # registered with AssetSpec
        named_assets = {}
        for external_dataset_name in default_pipeline.inputs():
            if not _is_param_name(external_dataset_name):
                external_asset_name = format_dataset_name(external_dataset_name)
                dataset = self._catalog._get_dataset(external_dataset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

                io_manager_key = "io_manager"
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{self._env}__{external_asset_name}_io_manager"

                partitions_def = None
                asset_partition = self._asset_partitions.get(external_asset_name, None)
                if asset_partition is not None:
                    partitions_def = asset_partition["partitions_def"]

                external_asset_key = get_asset_key_from_dataset_name(external_dataset_name, env=self._env)
                external_asset = dg.AssetSpec(
                    key=external_asset_key,
                    partitions_def=partitions_def,
                    group_name="external",
                    description=description,
                    metadata=metadata,
                    kinds={"kedro"},
                ).with_io_manager_key(io_manager_key=io_manager_key)
                named_assets[external_asset_name] = external_asset

        # Create assets from Kedro nodes that have outputs
        named_ops = {}
        for node in default_pipeline.nodes:
            op_name = format_node_name(node.name)
            graph_op = self.create_op(node)
            named_ops[f"{op_name}_graph"] = graph_op

            if len(node.outputs):
                asset = self.create_asset(node)
                named_assets[op_name] = asset

        return named_ops, named_assets
