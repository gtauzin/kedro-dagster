"""Translation of Kedro nodes to Dagster ops and assets."""

from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import DatasetNotFoundError, MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict

from kedro_dagster.datasets import DagsterNothingDataset  # type: ignore

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    format_dataset_name,
    format_node_name,
    get_asset_key_from_dataset_name,
    is_mlflow_enabled,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.io.catalog_config_resolver import CatalogConfigResolver
    from kedro.io import CatalogProtocol
    from kedro.pipeline.node import Node
    from pluggy import PluginManager


LOGGER = getLogger(__name__)


def get_partition_mapping(
    partition_mappings: dict[str, dg.PartitionMapping],
    upstream_asset_name: str,
    downstream_dataset_names: list[str],
    config_resolver: "CatalogConfigResolver",
) -> dg.PartitionMapping | None:
    """Get the appropriate partition mapping for an asset based on its downstream datasets.
    Args:
        partition_mappings (dict[str, PartitionMapping]): A dictionary of partition mappings.
        upstream_asset_name (str): The name of the upstream asset.
        downstream_dataset_names (list[str]): A list of downstream dataset names.
        config_resolver (CatalogConfigResolver): The catalog config resolver to match patterns.
    Returns:
        PartitionMapping | None: The appropriate partition mapping or None if not found.
    """
    mapped_downstream_asset_names = partition_mappings.keys()
    if downstream_dataset_names:
        mapped_downstream_dataset_name = None
        for downstream_dataset_name in downstream_dataset_names:
            downstream_asset_name = format_dataset_name(downstream_dataset_name)
            if downstream_asset_name in mapped_downstream_asset_names:
                mapped_downstream_dataset_name = downstream_dataset_name
                break
            else:
                match_pattern = config_resolver.match_pattern(downstream_dataset_name)
                if match_pattern is not None:
                    mapped_downstream_dataset_name = match_pattern
                    break

        if mapped_downstream_dataset_name is not None:
            if mapped_downstream_dataset_name in partition_mappings:
                return partition_mappings[mapped_downstream_dataset_name]
            else:
                LOGGER.warning(
                    f"Downstream dataset `{mapped_downstream_dataset_name}` of `{upstream_asset_name}` "
                    "is not found in the partition mappings. "
                    "The default partition mapping (i.e., `AllPartitionMapping`) will be used."
                )
        else:
            LOGGER.warning(
                f"None of the downstream datasets `{downstream_dataset_names}` of `{upstream_asset_name}` "
                "is found in the partition mappings. "
                "The default partition mapping (i.e., `AllPartitionMapping`) will be used."
            )

        return None

class NodeTranslator:
    """Translate Kedro nodes into Dagster ops and assets.

    Args:
        pipelines (list[Pipeline]): List of Kedro pipelines.
        catalog (CatalogProtocol): Kedro catalog instance.
        hook_manager (PluginManager): Kedro hook manager.
        session_id (str): Kedro session ID.
        asset_partitions (dict[str, Any]): Asset partition definitions.
        named_resources (dict[str, ResourceDefinition]): Named Dagster resources.
        env (str): Kedro environment.
    """

    def __init__(
        self,
        pipelines: list[Pipeline],
        catalog: "CatalogProtocol",
        hook_manager: "PluginManager",
        session_id: str,
        asset_partitions: dict[str, Any],
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

    def _is_nothing_asset_name(self, dataset_name: str) -> bool:
        """Return True if the catalog entry is a DagsterNothingDataset."""
        try:
            dataset = self._catalog._get_dataset(dataset_name)  # type: ignore[attr-defined]
        except Exception:
            return False
        if DagsterNothingDataset is not None:
            try:
                if isinstance(dataset, DagsterNothingDataset):  # type: ignore[arg-type]
                    return True
            except Exception:
                pass
        return False

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

        params["__partition_key"] = "__default__"

        # Node parameters are mapped to Dagster configs
        return _create_pydantic_model_from_dict(
            name="ParametersConfig",
            params=params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

    def _get_in_asset_params(self, dataset_name: str, asset_name: str, output_dataset_names: list[str]) -> dict[str, Any]:
        """Get the input asset parameters for a dataset.

        Args:
            dataset_name (str): The dataset name.
            asset_name (str): The corresponding asset name.

        Returns:
            dict[str, Any]: The input asset parameters.
        """
        in_asset_params = {}

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
            asset_name = format_dataset_name(dataset_name)
            if self._is_nothing_asset_name(dataset_name):
                ins[asset_name] = dg.In(dagster_type=dg.Nothing)
            elif not _is_param_name(dataset_name):
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_key))

        out = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            if self._is_nothing_asset_name(dataset_name):
                out[asset_name] = dg.Out(dagster_type=dg.Nothing)
            else:
                out_asset_params = self._get_out_asset_params(dataset_name, asset_name)
                out[asset_name] = dg.Out(**out_asset_params)

        NodeParametersConfig = self._get_node_parameters_config(node)
        op_name = format_node_name(node.name)

        required_resource_keys = []
        for dataset_name in node.inputs + node.outputs:
            asset_name = format_dataset_name(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                required_resource_keys.append(f"{self._env}__{asset_name}_io_manager")

        if is_mlflow_enabled():
            required_resource_keys.append("mlflow")

        @dg.op(
            name=f"{op_name}",
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins | {"before_pipeline_run_hook_output": dg.In(dagster_type=dg.Nothing)},
            out=out | {f"{op_name}_after_pipeline_run_hook_input": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
            tags={f"node_tag_{i + 1}": tag for i, tag in enumerate(node.tags)},
        )
        def node_graph_op(context: dg.OpExecutionContext, config: NodeParametersConfig, **inputs):  # type: ignore[no-untyped-def, valid-type]
            """Execute the Kedro node as a Dagster op."""
            context.log.info(f"Running node `{node.name}` in graph.")

            # Merge node parameters into inputs while filtering out reserved keys (e.g., partition key)
            config_values = config.model_dump()  # type: ignore[attr-defined]
            partition_key = config_values.pop("__partition_key", "__default__")

            inputs |= config_values
            inputs = {
                unformat_asset_name(input_asset_name): input_asset for input_asset_name, input_asset in inputs.items()
            }

            for input_dataset_name in node.inputs:
                if self._is_nothing_asset_name(input_dataset_name):
                    inputs[input_dataset_name] = None

            self._hook_manager.hook.before_node_run(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                is_async=False,  # TODO: Should this be True?
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

            # Emit materializations and attach partition metadata when available
            for output_dataset_name in node.outputs:
                if self._is_nothing_asset_name(output_dataset_name):
                    outputs.pop(output_dataset_name, None)
                    continue

                output_asset_key = get_asset_key_from_dataset_name(output_dataset_name, self._env)
                try:
                    if partition_key != "__default__":
                        # Attach output metadata so IO managers can read the target partition
                        context.add_output_metadata(
                            metadata={"partition_key": str(partition_key)},
                            output_name=format_dataset_name(output_dataset_name),
                        )
                        context.log_event(
                            dg.AssetMaterialization(asset_key=output_asset_key, partition=str(partition_key))
                        )
                    else:
                        context.log_event(dg.AssetMaterialization(asset_key=output_asset_key))
                except Exception:  # Best-effort metadata emission; don't fail the run for metadata issues
                    context.log.debug("Failed to emit partition metadata/materialization for %s", output_asset_key)

            if len(outputs) > 0:
                return tuple(outputs.values()) + (None,)

            return None

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
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)

            if self._is_nothing_asset_name(dataset_name):
                ins[asset_name] = dg.AssetIn(key=asset_key, dagster_type=dg.Nothing)
                continue

            if not _is_param_name(dataset_name):
                in_asset_params = self._get_in_asset_params(dataset_name, asset_name, output_dataset_names=node.outputs)
                ins[asset_name] = dg.AssetIn(key=asset_key, **in_asset_params)

        outs = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
            group_name = _get_node_pipeline_name(node)

            if self._is_nothing_asset_name(dataset_name):
                outs[asset_name] = dg.AssetOut(key=asset_key, dagster_type=dg.Nothing, group_name=group_name)
                continue

            out_asset_params = self._get_out_asset_params(dataset_name, asset_name, return_kinds=True)
            outs[asset_name] = dg.AssetOut(key=asset_key, group_name=group_name, **out_asset_params)

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
            inputs = {
                unformat_asset_name(input_asset_name): input_asset for input_asset_name, input_asset in inputs.items()
            }

            for input_dataset_name in node.inputs:
                if self._is_nothing_asset_name(input_dataset_name):
                    inputs[input_dataset_name] = None

            outputs = node.run(inputs)

            for output_dataset_name in node.outputs:
                if self._is_nothing_asset_name(output_dataset_name):
                    outputs[output_dataset_name] = None

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

        # Assets that are not generated through dagster are external and
        # registered with AssetSpec
        named_assets = {}
        for external_dataset_name in default_pipeline.inputs():
            external_asset_name = format_dataset_name(external_dataset_name)
            if not _is_param_name(external_dataset_name):
                dataset = self._catalog._get_dataset(external_dataset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

                io_manager_key = "io_manager"
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{self._env}__{external_asset_name}_io_manager"

                # All pipeline inputs are not necessarily external. A partition that is an input of a node
                # along with a DagsterNothingDataset is most likely part of the pipeline itself and its
                # group name should match that of the node's pipeline.
                # Note that this is a best-effort attempt and may not cover all cases (e.g. same node part
                # of multiple pipelines).
                group_name = "external"
                for pipeline in self._pipelines:
                    for node in pipeline.nodes:
                        if external_dataset_name in node.inputs and any(
                            self._is_nothing_asset_name(ds) for ds in node.inputs
                        ):
                            group_name = _get_node_pipeline_name(node)
                            break

                partitions_def = None
                asset_partition = self._asset_partitions.get(external_asset_name, None)
                if asset_partition is not None:
                    partitions_def = asset_partition["partitions_def"]

                external_asset_key = get_asset_key_from_dataset_name(external_dataset_name, env=self._env)
                external_asset = dg.AssetSpec(
                    key=external_asset_key,
                    group_name=group_name,
                    partitions_def=partitions_def,
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
