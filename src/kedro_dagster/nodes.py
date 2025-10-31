"""Translation of Kedro nodes to Dagster ops and assets.

This module provides the :class:`NodeTranslator` which is responsible for
turning Kedro nodes into Dagster runtime primitives:

- Ops that can be composed in a Dagster graph-based job
- Multi-assets that represent the materialization of a Kedro node's outputs

It also encapsulates partition-awareness and coordinates Kedro hooks around
node execution so that existing Kedro projects behave the same when executed
via Dagster.
"""

from functools import partial
from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict

from kedro_dagster.datasets.nothing_dataset import NOTHING_OUTPUT
from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_param_name,
    _kedro_version,
    format_dataset_name,
    format_node_name,
    get_asset_key_from_dataset_name,
    get_dataset_from_catalog,
    get_partition_mapping,
    is_nothing_asset_name,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.io import CatalogProtocol
    from kedro.pipeline.node import Node
    from pluggy import PluginManager


LOGGER = getLogger(__name__)


class NodeTranslator:
    """Translate Kedro nodes into Dagster ops and assets.

    The translator exposes two main translation methods:

    - :meth:`create_op` wraps a Kedro node function within a Dagster op for use
      inside graph-based jobs (used by :class:`~kedro_dagster.pipelines.PipelineTranslator`).
    - :meth:`create_asset` wraps a Kedro node as a Dagster multi-asset, one
      output per Kedro dataset.

    Partitioned datasets are handled by propagating Dagster partitions through
    the op/asset definitions and by passing partition mappings as needed.

    Args:
        pipelines (list[Pipeline]): Kedro pipelines used to derive assets and groups.
        catalog (CatalogProtocol): Kedro catalog instance for dataset resolution.
        hook_manager (PluginManager): Kedro hook manager to invoke node-related hooks.
        run_id (str): Kedro run ID to forward to hooks. In Kedro < 1.0, this is called `session_id`.
        asset_partitions (dict[str, Any]): Mapping of asset name -> {"partitions_def", "partition_mappings"}.
        named_resources (dict[str, dg.ResourceDefinition]): Pre-created Dagster resources keyed by name.
        env (str): Kedro environment (used for namespacing asset keys/resources).
    """

    def __init__(
        self,
        pipelines: list[Pipeline],
        catalog: "CatalogProtocol",
        hook_manager: "PluginManager",
        run_id: str,
        asset_partitions: dict[str, Any],
        named_resources: dict[str, dg.ResourceDefinition],
        env: str,
    ):
        self._pipelines = pipelines
        self._catalog = catalog
        self._hook_manager = hook_manager
        self._run_id = run_id
        self._asset_partitions = asset_partitions
        self._named_resources = named_resources
        self._env = env

    def _get_node_partitions_definition(self, node: "Node") -> dg.PartitionsDefinition | None:
        """Infer the partitions definition for a node's outputs.

        If a node produces multiple partitioned outputs with different definitions,
        a :class:`dagster.MultiPartitionsDefinition` is returned; for a single
        partitioned output, the definition is returned directly. If none of the
        outputs are partitioned, returns ``None``.

        Args:
            node (Node): Kedro node to inspect.

        Returns:
            dg.PartitionsDefinition | None: Partitions definition (possibly multi) or ``None``.
        """
        partitioned_assets: dict[str, dg.PartitionsDefinition] = {}

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
        """Generate a Dagster Config model mirroring Kedro node parameters.

        Kedro parameters are injected into the op/asset as a Pydantic-based
        Dagster :class:`~dagster.Config` model so that they can be overridden at
        job submission time while retaining validation.

        Args:
            node (Node): Kedro node whose parameters will be loaded from the catalog.

        Returns:
            dg.Config: Config subclass representing the node parameters (possibly empty).
        """
        params: dict[str, Any] = {}
        for dataset_name in node.inputs:
            if _is_param_name(dataset_name):
                params[dataset_name] = self._catalog.load(dataset_name)

        # Node parameters are mapped to Dagster configs
        return _create_pydantic_model_from_dict(
            name="ParametersConfig",
            params=params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

    def _get_in_asset_params(self, dataset_name: str, asset_name: str, out_dataset_names: list[str]) -> dict[str, Any]:
        """Compute :class:`dagster.AssetIn` kwargs for an input dataset.

        In particular, attaches an appropriate ``partition_mapping`` when the
        upstream input and downstream outputs are partitioned and a mapping is
        declared in the catalog via :class:`~kedro_dagster.datasets.DagsterPartitionedDataset`.

        Args:
            dataset_name (str): Kedro dataset name for the input.
            asset_name (str): Dagster-safe asset name for the input.
            out_dataset_names (list[str]): Downstream output dataset names of the consuming node
                (used to select a specific mapping when multiple are defined).

        Returns:
            dict[str, Any]: Keyword arguments to pass to :class:`dagster.AssetIn`.
        """
        in_asset_params: dict[str, Any] = {}

        if asset_name in self._asset_partitions:
            partition_mappings = self._asset_partitions[asset_name]["partition_mappings"]
            if partition_mappings is not None:
                partition_mapping = get_partition_mapping(
                    partition_mappings,
                    asset_name,
                    downstream_dataset_names=out_dataset_names,
                    config_resolver=self._catalog._config_resolver,
                )

                if partition_mapping is not None:
                    in_asset_params["partition_mapping"] = partition_mapping

        return in_asset_params

    def _get_out_asset_params(self, dataset_name: str, asset_name: str, return_kinds: bool = False) -> dict[str, Any]:
        """Compute :class:`dagster.AssetOut` kwargs for an output dataset.

        This inspects the Kedro catalog entry to propagate metadata and to select
        a specific IO manager when the dataset is not in-memory. Optionally, it
        also annotates the asset with ``kinds`` for integration (e.g. MLflow).

        Args:
            dataset_name (str): Kedro dataset name for the output.
            asset_name (str): Dagster-safe asset name for the output.
            return_kinds (bool): Whether to include an explicit ``kinds`` set.

        Returns:
            dict[str, Any]: Keyword arguments to pass to :class:`dagster.AssetOut`.
        """
        metadata, description = None, None
        io_manager_key = "io_manager"

        if asset_name in self.asset_names:
            dataset = get_dataset_from_catalog(self._catalog, dataset_name)
            if dataset is not None:
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")
                if not isinstance(dataset, MemoryDataset):
                    candidate_key = f"{self._env}__{asset_name}_io_manager"
                    if candidate_key in self._named_resources:
                        io_manager_key = candidate_key

        out_asset_params: dict[str, Any] = dict(
            io_manager_key=io_manager_key,
            metadata=metadata,
            description=description,
        )

        if return_kinds:
            kinds = {"kedro"}
            # Annotate MLflow kind only if MLflow resource is available
            if "mlflow" in self._named_resources:
                kinds.add("mlflow")
            out_asset_params["kinds"] = kinds

        return out_asset_params

    @property
    def asset_names(self) -> list[str]:
        """Return a list of all asset names referenced by the provided pipelines.

        Returns:
            list[str]: Unique asset names referenced across all pipelines.
        """
        if not hasattr(self, "_asset_names"):
            asset_names: list[str] = []
            for dataset_name in sum(self._pipelines, Pipeline([])).datasets():
                asset_name = format_dataset_name(dataset_name)
                asset_names.append(asset_name)

            asset_names = list(set(asset_names))
            self._asset_names = asset_names

        return self._asset_names

    def create_op(
        self,
        node: "Node",
        is_in_first_layer: bool = False,
        is_in_last_layer: bool = True,
        partition_keys: dict[str, str] | None = None,
        partition_keys_per_in_asset_names: dict[str, list[str]] | None = None,
    ) -> dg.OpDefinition:
        """Create a Dagster op wrapping a Kedro node for use in a graph job.

        The op wires inputs/outputs to Dagster assets and propagates Kedro hooks.
        When ``partition_keys`` is provided, the op name is suffixed with the
        downstream partition key to ensure uniqueness per cloned invocation.

        Args:
            node (Node): Kedro node to wrap.
            is_in_first_layer (bool): Whether the node is in the first topological layer
                of the pipeline (adds a synthetic input to trigger ``before_pipeline_run``).
            is_in_last_layer (bool): Whether the node is in the last topological layer
                (adds a synthetic output to trigger ``after_pipeline_run``).
            partition_keys (dict[str, str] | None): Optional mapping with keys ``upstream_partition_key`` and
                ``downstream_partition_key`` encoded as "asset_name|partition_key"; used by
                the :class:`~kedro_dagster.pipelines.PipelineTranslator` during static fan-out.
            partition_keys_per_in_asset_names (dict[str, list[str]] | None): For nodes that consume ``Nothing`` assets that
                are repeated per partition, provide a map of input asset name -> list of
                formatted partition keys so multiple Nothing inputs can be declared.

        Returns:
            dg.OpDefinition: Fully constructed Dagster op.
        """
        partition_key = None
        op_name = format_node_name(node.name)
        if partition_keys is not None:
            partition_key = partition_keys["upstream_partition_key"].split("|")[1]
            op_name += f"__{format_node_name(partition_key)}"

        ins: dict[str, dg.In] = {}
        for dataset_name in node.inputs:
            asset_name = format_dataset_name(dataset_name)
            if is_nothing_asset_name(self._catalog, dataset_name):
                if partition_keys_per_in_asset_names is None or asset_name not in partition_keys_per_in_asset_names:
                    ins[asset_name] = dg.In(dagster_type=dg.Nothing)
                else:
                    for in_partition_key in partition_keys_per_in_asset_names[asset_name]:
                        ins[asset_name + f"__{in_partition_key}"] = dg.In(dagster_type=dg.Nothing)
            elif not _is_param_name(dataset_name):
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                ins[asset_name] = dg.In(asset_key=asset_key)

        if is_in_first_layer:
            # Add a dummy input to trigger `before_pipeline_run` hook
            ins["before_pipeline_run_hook_output"] = dg.In(dagster_type=dg.Nothing)

        out: dict[str, dg.Out] = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            if is_nothing_asset_name(self._catalog, dataset_name):
                out[asset_name] = dg.Out(dagster_type=dg.Nothing)
            else:
                out_asset_params = self._get_out_asset_params(dataset_name, asset_name)
                out[asset_name] = dg.Out(**out_asset_params)

        if is_in_last_layer:
            # Add a dummy output to trigger `after_pipeline_run` hook
            out[f"{op_name}_after_pipeline_run_hook_input"] = dg.Out(dagster_type=dg.Nothing)

        NodeParametersConfig = self._get_node_parameters_config(node)

        required_resource_keys: list[str] = []
        for dataset_name in node.inputs + node.outputs:
            asset_name = format_dataset_name(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                required_resource_keys.append(f"{self._env}__{asset_name}_io_manager")

        # Require MLflow resource only if it's provided
        if "mlflow" in self._named_resources:
            required_resource_keys.append("mlflow")

        tags = {f"kedro_tag_{i + 1}": tag for i, tag in enumerate(node.tags)}
        if partition_keys is not None:
            tags |= partition_keys

        @dg.op(
            name=op_name,
            description=f"Kedro node {node.name} wrapped as a Dagster op.",
            ins=ins,
            out=out,
            required_resource_keys=required_resource_keys,
            tags=tags,
        )
        def node_graph_op(context: dg.OpExecutionContext, config: NodeParametersConfig, **inputs):  # type: ignore[no-untyped-def, valid-type]
            """Execute the Kedro node as a Dagster op.

            Args:
                context (OpExecutionContext): Dagster op execution context.
                config (Config): Node parameters config model.
                **inputs: Materialized inputs keyed by formatted asset names and parameters.

            Returns:
                Any | tuple[Any, ...] | None: Node outputs as a single value, tuple, or ``None`` when no outputs.
            """
            context.log.info(f"Running node `{node.name}` in graph.")

            config_values = config.model_dump()  # type: ignore[attr-defined]

            # Merge params into inputs provided by Dagster
            inputs |= config_values
            inputs = {unformat_asset_name(in_asset_name): in_asset for in_asset_name, in_asset in inputs.items()}

            for in_dataset_name in node.inputs:
                if is_nothing_asset_name(self._catalog, in_dataset_name):
                    inputs[in_dataset_name] = None

            before_node_run_params = dict(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                is_async=False,
            )
            # Kedro 1.x hooks renamed session_id to run_id
            if _kedro_version()[0] >= 1:
                before_node_run_params["run_id"] = self._run_id
            else:  # pragma: no cover
                before_node_run_params["session_id"] = self._run_id

            self._hook_manager.hook.before_node_run(**before_node_run_params)

            try:
                outputs = node.run(inputs)

            except Exception as exc:
                if _kedro_version()[0] >= 1:
                    self._hook_manager.hook.on_node_error(
                        error=exc,
                        node=node,
                        catalog=self._catalog,
                        inputs=inputs,
                        is_async=False,
                        run_id=self._run_id,
                    )
                else:
                    self._hook_manager.hook.on_node_error(
                        error=exc,
                        node=node,
                        catalog=self._catalog,
                        inputs=inputs,
                        is_async=False,
                        session_id=self._run_id,
                    )
                raise exc

            after_node_run_params = dict(
                node=node,
                catalog=self._catalog,
                inputs=inputs,
                outputs=outputs,
                is_async=False,
            )
            # Kedro 1.x hooks renamed session_id to run_id
            if _kedro_version()[0] >= 1:
                after_node_run_params["run_id"] = self._run_id
            else:  # pragma: no cover
                after_node_run_params["session_id"] = self._run_id

            self._hook_manager.hook.after_node_run(**after_node_run_params)

            # Emit materializations and attach partition metadata when available
            for out_dataset_name in node.outputs:
                out_asset_key = get_asset_key_from_dataset_name(out_dataset_name, self._env)
                context.log_event(dg.AssetMaterialization(asset_key=out_asset_key, partition=partition_key))

                if (
                    is_nothing_asset_name(self._catalog, out_dataset_name)
                    and outputs[out_dataset_name] == NOTHING_OUTPUT
                ):
                    outputs[out_dataset_name] = None

            if len(outputs) > 0:
                res = tuple(outputs.values())
                if is_in_last_layer:
                    res += (None,)
                elif len(outputs) == 1:
                    return res[0]

                return res

            return None

        return node_graph_op

    def create_asset(self, node: "Node") -> dg.AssetsDefinition:
        """Create a Dagster multi-asset from a Kedro node.

        One asset output is created per Kedro output dataset. Partitioning and
        partition mappings are propagated when available.

        Args:
            node (Node): Kedro node to wrap.

        Returns:
            dg.AssetsDefinition: Multi-asset representing the node outputs.
        """

        ins: dict[str, dg.AssetIn] = {}
        for dataset_name in node.inputs:
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)

            if is_nothing_asset_name(self._catalog, dataset_name):
                ins[asset_name] = dg.AssetIn(key=asset_key, dagster_type=dg.Nothing)
                continue

            if not _is_param_name(dataset_name):
                in_asset_params = self._get_in_asset_params(dataset_name, asset_name, out_dataset_names=node.outputs)
                ins[asset_name] = dg.AssetIn(key=asset_key, **in_asset_params)

        outs: dict[str, dg.AssetOut] = {}
        for dataset_name in node.outputs:
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
            group_name = _get_node_pipeline_name(node)

            if is_nothing_asset_name(self._catalog, dataset_name):
                outs[asset_name] = dg.AssetOut(key=asset_key, dagster_type=dg.Nothing, group_name=group_name)
                continue

            out_asset_params = self._get_out_asset_params(dataset_name, asset_name, return_kinds=True)
            outs[asset_name] = dg.AssetOut(key=asset_key, group_name=group_name, **out_asset_params)

        NodeParametersConfig = self._get_node_parameters_config(node)

        required_resource_keys = None
        # Require MLflow resource only if it's provided
        if "mlflow" in self._named_resources:
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
            """Execute the Kedro node as a Dagster asset.

            Args:
                context (AssetExecutionContext): Dagster asset execution context.
                config (Config): Node parameters config model.
                **inputs: Materialized inputs keyed by formatted asset names and parameters.

            Returns:
                Any | tuple[Any, ...] | None: Node outputs as a single value or a tuple when multiple outputs exist.
            """
            context.log.info(f"Running node `{node.name}` in asset.")

            # Merge params into inputs provided by Dagster
            inputs |= config.model_dump()  # type: ignore[attr-defined]
            inputs = {unformat_asset_name(in_asset_name): in_asset for in_asset_name, in_asset in inputs.items()}

            for in_dataset_name in node.inputs:
                if is_nothing_asset_name(self._catalog, in_dataset_name):
                    inputs[in_dataset_name] = None

            outputs = node.run(inputs)

            for out_dataset_name in node.outputs:
                if (
                    is_nothing_asset_name(self._catalog, out_dataset_name)
                    and outputs[out_dataset_name] == NOTHING_OUTPUT
                ):
                    outputs[out_dataset_name] = None

            if len(outputs) == 1:
                return list(outputs.values())[0]
            elif len(outputs) > 1:
                return tuple(outputs.values())

        return dagster_asset

    def to_dagster(self) -> tuple[dict[str, dg.OpDefinition], dict[str, dg.AssetSpec | dg.AssetsDefinition]]:
        """Translate all Kedro nodes involved into Dagster op factories and assets.

        Returns:
            tuple[dict[str, dg.OpDefinition], dict[str, dg.AssetSpec | dg.AssetsDefinition]]: 2-tuple of
            (op factories, assets), where:
            - op factories map names to callables that produce partition-aware ops when invoked;
            - assets map names to either external :class:`dagster.AssetSpec` (for upstream inputs)
            or concrete :class:`dagster.AssetsDefinition` produced by nodes.
        """

        default_pipeline: Pipeline = sum(self._pipelines, start=Pipeline([]))

        # Assets that are not generated through Dagster are considered external
        # and are registered with AssetSpec so jobs can reference them.
        named_assets: dict[str, dg.AssetSpec | dg.AssetsDefinition] = {}
        for external_dataset_name in default_pipeline.inputs():
            external_asset_name = format_dataset_name(external_dataset_name)
            if not _is_param_name(external_dataset_name):
                dataset = get_dataset_from_catalog(self._catalog, external_dataset_name)
                metadata: dict[str, Any] | None = None
                description = None
                io_manager_key = "io_manager"
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")
                if not isinstance(dataset, MemoryDataset):
                    io_manager_key = f"{self._env}__{external_asset_name}_io_manager"

                # All pipeline inputs are not necessarily external. A partition that is an input of a node
                # along with a DagsterNothingDataset is most likely part of the pipeline itself and its
                # group name should match that of the node's pipeline.
                # Note that this is a best-effort attempt and may not cover all cases (e.g. same node part
                # of multiple pipelines).
                group_name = "external"
                for pipeline in self._pipelines:
                    for pipeline_node in pipeline.nodes:
                        if external_dataset_name in pipeline_node.inputs and any(
                            is_nothing_asset_name(self._catalog, ds) for ds in pipeline_node.inputs
                        ):
                            group_name = _get_node_pipeline_name(pipeline_node)
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
        named_op_factories: dict[str, Any] = {}
        for pipeline_node in default_pipeline.nodes:
            op_name = format_node_name(pipeline_node.name)
            op_factory = partial(self.create_op, node=pipeline_node)
            named_op_factories[f"{op_name}_graph"] = op_factory

            if len(pipeline_node.outputs):
                asset = self.create_asset(pipeline_node)
                named_assets[op_name] = asset

        return named_op_factories, named_assets
