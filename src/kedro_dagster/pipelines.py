"""Translation of Kedro pipelines to Dagster jobs."""

import re
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.utils import (
    _is_param_name,
    format_dataset_name,
    format_node_name,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    get_partition_mapping,
    is_mlflow_enabled,
    is_nothing_asset_name,
    serialize_partition_key,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext

def pprint(name, val):
    print(name)
    if isinstance(val, dict):
        for k, v in val.items():
            print(f"  {k}: {v}")
    else:
        print(f"  {val}")

class PipelineTranslator:
    """Translator for Kedro pipelines to Dagster jobs.

    Args:
        dagster_config (dict[str, Any]): The configuration of the Dagster job.
        context (KedroContext): The Kedro context.
        project_path (str): The path to the Kedro project.
        env (str): The Kedro environment.
        session_id (str): The Kedro session ID.
        named_assets (dict[str, AssetsDefinition]): The named assets.
        named_op_factories (dict[str, OpDefinition]): The named ops.
        named_resources (dict[str, ResourceDefinition]): The named resources.
        named_executors (dict[str, ExecutorDefinition]): The named executors.
    """

    def __init__(
        self,
        dagster_config: dict[str, Any],
        context: "KedroContext",
        project_path: str,
        env: str,
        session_id: str,
        named_assets: dict[str, dg.AssetsDefinition],
        asset_partitions: dict[str, Any],
        named_op_factories: dict[str, dg.OpDefinition],
        named_resources: dict[str, dg.ResourceDefinition],
        named_executors: dict[str, dg.ExecutorDefinition],
        enable_mlflow: bool,
    ):
        self._dagster_config = dagster_config
        self._context = context
        self._project_path = project_path
        self._env = env
        self._session_id = session_id
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._named_assets = named_assets
        self._asset_partitions = asset_partitions
        self._named_op_factories = named_op_factories
        self._named_resources = named_resources
        self._named_executors = named_executors
        self._enable_mlflow = enable_mlflow

    def _enumerate_partition_keys_for_asset(self, asset_name: str) -> list[Any]:
        """Enumerate partition keys for an asset, supports multi and regular partitions.

        Args:
            asset_name: Name of the asset to enumerate partitions for.

        Returns:
            List of partition keys (can be MultiPartitionKey or regular keys), or empty list if unpartitioned.
        """
        partitions_def = None
        if asset_name in self._asset_partitions:
            partitions_def = self._asset_partitions[asset_name].get("partitions_def")

        if not partitions_def:
            return []
        if isinstance(partitions_def, dg.MultiPartitionsDefinition) and hasattr(partitions_def, "get_multi_partition_keys"):
            try:
                return list(partitions_def.get_multi_partition_keys())  # type: ignore[attr-defined]
            except Exception:
                pass

        return list(getattr(partitions_def, "get_partition_keys")())

    def _list_node_execution_keys(self, node) -> list[Any]:
        """List execution partition keys for a node.

        Strategy:
        - If any output is partitioned: use that partition space (validate if multiple outputs share identical key sets).
        - Else if any input is partitioned: use that partition space (validate if multiple inputs share identical key sets).
        - Else: unpartitioned -> [].

        Args:
            node: The Kedro node to analyze

        Returns:
            List of partition keys to execute for this node, or empty list if unpartitioned
        """
        out_key_sets: list[list[Any]] = []
        for ds in node.outputs:
            asset_name = format_dataset_name(ds)
            keys = self._enumerate_partition_keys_for_asset(asset_name)
            if keys:
                out_key_sets.append(keys)

        if out_key_sets:
            base = set(map(serialize_partition_key, out_key_sets[0]))
            for ks in out_key_sets[1:]:
                if set(map(serialize_partition_key, ks)) != base:
                    raise ValueError(
                        f"Node '{node.name}' has multiple partitioned outputs with incompatible keyspaces"
                    )

            return out_key_sets[0]

        in_key_sets: list[list[Any]] = []
        for ds in node.inputs:
            asset_name = format_dataset_name(ds)
            keys = self._enumerate_partition_keys_for_asset(asset_name)
            if keys:
                in_key_sets.append(keys)

        if in_key_sets:
            base = set(map(serialize_partition_key, in_key_sets[0]))
            for ks in in_key_sets[1:]:
                if set(map(serialize_partition_key, ks)) != base:
                    raise ValueError(
                        f"Node '{node.name}' has multiple partitioned inputs with incompatible keyspaces"
                    )
            return in_key_sets[0]

        return []


    def _create_before_pipeline_run_hook(self, job_name: str, pipeline: Pipeline) -> tuple[dg.OpDefinition, dg.OpDefinition]:
        """Create the pipeline hook ops for before and after pipeline run.

        Args:
            job_name (str): The name of the job.
            pipeline (Pipeline): The Kedro pipeline.

        Returns:
            tuple[OpDefinition, OpDefinition]: The before and after pipeline run hook ops.

        """
        required_resource_keys = {"kedro_run"}
        if self._enable_mlflow and is_mlflow_enabled():
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"before_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed before the `{job_name}` pipeline run.",
            out={"before_pipeline_run_hook_output": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
        )
        def before_pipeline_run_hook_op(context: dg.OpExecutionContext):
            kedro_run_resource = context.resources.kedro_run
            kedro_run_resource.after_context_created_hook()

            self._hook_manager.hook.before_pipeline_run(
                run_params=kedro_run_resource.run_params,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        return before_pipeline_run_hook_op

    def _create_after_pipeline_run_hook_op(
        self,
        job_name: str,
        pipeline: Pipeline,
        output_nothing_asset_names: list[str],
        partitioned_output_assets: dict[str, dict[str, Any]],
    ) -> dg.OpDefinition:

        after_pipeline_run_hook_ins = {
            f"{format_node_name(node.name)}_after_pipeline_run_hook_input": dg.In(dagster_type=dg.Nothing)
            for node in pipeline.nodes
        }
        for dataset_name in pipeline.all_outputs():
            asset_name = format_dataset_name(dataset_name)
            asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
            if is_nothing_asset_name(self._catalog, dataset_name):
                if asset_name not in partitioned_output_assets:
                    output_nothing_asset_names.append(asset_name)
                    after_pipeline_run_hook_ins[asset_name] = dg.In(dagster_type=dg.Nothing)

                else:
                    for partition_key in partitioned_output_assets[asset_name].keys():
                        after_pipeline_run_hook_ins[asset_name + f"__{partition_key}"] = dg.In(dagster_type=dg.Nothing)

            elif asset_name in partitioned_output_assets:
                for partition_key in partitioned_output_assets[asset_name].keys():
                    after_pipeline_run_hook_ins[asset_name + f"__{partition_key}"] = dg.In()

            else:
                after_pipeline_run_hook_ins[asset_name] = dg.In(
                    asset_key=dg.AssetKey(asset_key),
                )

        for output_nothing_asset_name in output_nothing_asset_names:
            after_pipeline_run_hook_ins[output_nothing_asset_name] = dg.In(dagster_type=dg.Nothing)

        required_resource_keys = {"kedro_run"}
        if self._enable_mlflow and is_mlflow_enabled():
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook_op(context: dg.OpExecutionContext, **materialized_assets) -> dg.Nothing:  # type: ignore[no-untyped-def]
            kedro_run_resource = context.resources.kedro_run

            run_results = {}
            for dataset_name in pipeline.outputs():
                asset_name = format_dataset_name(dataset_name)

                if is_nothing_asset_name(self._catalog, dataset_name):
                    run_results[dataset_name] = None
                elif asset_name in partitioned_output_assets:
                    # Collect partitioned outputs
                    partitioned_vals = {}
                    for partition_key in partitioned_output_assets[asset_name].keys():
                        handle = materialized_assets.get(asset_name + f"__{partition_key}")
                        if handle is not None:
                            partitioned_vals[partition_key] = handle
                    run_results[dataset_name] = partitioned_vals
                else:
                    run_results[dataset_name] = materialized_assets[asset_name]

            self._hook_manager.hook.after_pipeline_run(
                run_params=kedro_run_resource.run_params,
                run_result=run_results,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        return after_pipeline_run_hook_op

    def translate_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_name: str,
        filter_params: dict[str, Any],
        job_name: str,
        executor_def: dg.ExecutorDefinition | None = None,
        logger_defs: dict[str, dg.LoggerDefinition] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job with partition support.

        This method implements static fan-out for partitioned datasets:
        - Nodes with partitioned outputs/inputs are cloned for each partition key
        - Partition mappings define relationships between upstream and downstream partitions
        - Identity mapping is used by default (same partition key across assets)

        Args:
            pipeline (Pipeline): The Kedro pipeline.
            pipeline_name (str): The name of the Kedro pipeline.
            filter_params (dict[str, Any]): Filter parameters for the pipeline.
            job_name (str): The name of the job.
            executor_def (ExecutorDefinition): The executor definition.
            logger_defs (dict[str, LoggerDefinition] | None): The logger definitions.

        Returns:
            JobDefinition: A Dagster job definition with partition-aware ops.
        """
        before_pipeline_run_hook_op = self._create_before_pipeline_run_hook(job_name, pipeline)

        @dg.graph(
            name=f"{self._env}__{job_name}",
            description=f"Job derived from pipeline associated to `{job_name}` in env `{self._env}`.",
            out=None,
        )
        def pipeline_graph() -> None:
            before_pipeline_run_hook_output = before_pipeline_run_hook_op()

            # Collect initial external assets (broadcastable to partitions)
            materialized_input_assets: dict[str, Any] = {}
            for dataset_name in pipeline.inputs():
                asset_name = format_dataset_name(dataset_name)
                if not _is_param_name(dataset_name):
                    # External assets first
                    if asset_name in self._named_assets:
                        materialized_input_assets[asset_name] = self._named_assets[asset_name]
                    else:
                        asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                        materialized_input_assets[asset_name] = dg.AssetSpec(key=asset_key).with_io_manager_key(
                            f"{self._env}__{asset_name}_io_manager"
                        )

            # Per-asset partitioned outputs cache: asset -> {serialized_partition_key: handle}
            partitioned_output_assets: dict[str, dict[str, Any]] = {}

            # Last seen outputs (for after hook wiring) — for partitioned assets, last clone wins
            materialized_output_assets: dict[str, Any] = {}

            for layer in pipeline.grouped_nodes:
                for node in layer:
                    op_name = format_node_name(node.name) + "_graph"

                    execution_partition_keys = self._list_node_execution_keys(node)

                    # Helper to assemble input kwargs for a given partition key (or None)
                    def _build_inputs_for_partition(partition_key: Any | None, serialized_partition_key: str | None) -> dict[str, Any]:
                        """Build input kwargs for a specific partition execution.

                        Args:
                            partition_key: The partition key object (can be MultiPartitionKey or regular key)
                            serialized_partition_key: Serialized partition key string for lookup

                        Returns:
                            Dictionary mapping input names to their handles/assets
                        """
                        inputs_kwargs: dict[str, Any] = {}
                        for input_dataset_name in node.inputs:
                            input_asset_name = format_dataset_name(input_dataset_name)
                            if _is_param_name(input_dataset_name):
                                continue

                            # Preference order: partitioned_output_assets (from previous nodes) -> initial external
                            # Check if we have a partitioned upstream output from a previous node
                            if input_asset_name in partitioned_output_assets and serialized_partition_key is not None:
                                upstream_partition_mapping = partitioned_output_assets[input_asset_name]
                                # Identity mapping by default: same serial key
                                handle = upstream_partition_mapping.get(serialized_partition_key)
                                if handle is None:
                                    # Try mapping from upstream to this downstream partition key
                                    # Infer downstream asset from current node's outputs
                                    downstream_asset_candidates = [format_dataset_name(ds) for ds in node.outputs]
                                    mapping_fn = None
                                    for downstream_candidate in downstream_asset_candidates:
                                        mapping_fn = get_partition_mapping(input_asset_name, downstream_candidate)
                                        if mapping_fn is not None:
                                            break
                                    if mapping_fn is not None and partition_key is not None:
                                        # Find upstream partitions mapping to this downstream key
                                        # This reverses the mapping: given downstream key, find upstream keys
                                        upstream_partition_keys = self._enumerate_partition_keys_for_asset(input_asset_name)
                                        matched_serials: list[str] = []
                                        for upstream_partition_key in upstream_partition_keys:
                                            mapped_down = mapping_fn(upstream_partition_key) or []
                                            if any(
                                                serialize_partition_key(downstream_partition_key) == serialized_partition_key
                                                for downstream_partition_key in mapped_down
                                            ):
                                                matched_serials.append(serialize_partition_key(upstream_partition_key))
                                        if len(matched_serials) > 1:
                                            raise ValueError(
                                                f"Implicit fan-in not supported for input '{input_asset_name}' of node '{node.name}'."
                                            )
                                        if matched_serials:
                                            handle = upstream_partition_mapping.get(matched_serials[0])
                                if handle is not None:
                                    inputs_kwargs[input_asset_name] = handle
                                    continue
                            # Fallback to broadcast external/unpartitioned
                            if input_asset_name in materialized_input_assets:
                                inputs_kwargs[input_asset_name] = materialized_input_assets[input_asset_name]

                        return inputs_kwargs

                    if not execution_partition_keys:
                        # Unpartitioned single invocation
                        inputs_kwargs = _build_inputs_for_partition(None, None)
                        base_op = self._named_op_factories[op_name]()

                        partition_keys_per_input_asset_names = {}
                        for asset_name, asset_partitions_val in partitioned_output_assets.items():
                            dataset_name = unformat_asset_name(asset_name)
                            if asset_name in base_op.ins and is_nothing_asset_name(self._catalog, dataset_name):
                                partition_keys_per_input_asset_names[asset_name] = list(asset_partitions_val.keys())
                                for partition_key, asset_partition_val in asset_partitions_val.items():
                                    inputs_kwargs[asset_name + f"__{partition_key}"] = asset_partition_val

                        op = self._named_op_factories[op_name](partition_keys_per_input_asset_names=partition_keys_per_input_asset_names)
                        res = op(
                            before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                            **inputs_kwargs,
                        )

                        # Capture outputs
                        if len(node.outputs) == 0:
                            materialized_output_assets_op = {res.output_name: res}
                        else:
                            materialized_output_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res  # type: ignore[assignment]
                            }
                        # Update caches
                        for out_ds in node.outputs:
                            out_asset_name = format_dataset_name(out_ds)
                            handle = materialized_output_assets_op.get(out_asset_name)
                            if handle is not None:
                                materialized_input_assets[out_asset_name] = handle
                                materialized_output_assets[out_asset_name] = handle
                        # Also keep the hook Nothing output (overwrites last)
                        for name, handle in materialized_output_assets_op.items():
                            if name.endswith("_after_pipeline_run_hook_input"):
                                materialized_output_assets[name] = handle
                        continue

                    # Partitioned: clone per partition key
                    for partition_key in execution_partition_keys:
                        partition_keys = serialize_partition_key(partition_key)
                        op = self._named_op_factories[op_name](partition_key=partition_key)

                        inputs_kwargs = _build_inputs_for_partition(partition_key, partition_keys)
                        res = op(
                            before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                            **inputs_kwargs,
                        )
                        if len(node.outputs) == 0:
                            materialized_output_assets_op = {res.output_name: res}
                        else:
                            materialized_output_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res  # type: ignore[assignment]
                            }

                        # Update partitioned cache for each produced asset
                        for out_ds in node.outputs:
                            out_asset_name = format_dataset_name(out_ds)
                            handle = materialized_output_assets_op.get(out_asset_name)

                            if handle is not None:
                                partitioned_output_assets.setdefault(out_asset_name, {})[partition_keys] = handle
                                # For after hook, last clone wins
                                materialized_output_assets[out_asset_name] = handle
                        # Also keep the hook Nothing output for this clone (last clone wins)
                        for name, handle in materialized_output_assets_op.items():
                            if name.endswith("_after_pipeline_run_hook_input"):
                                materialized_output_assets[name] = handle

            output_nothing_asset_names = []
            for output_asset_name in materialized_output_assets.keys():
                if output_asset_name.endswith("_after_pipeline_run_hook_input"):
                    output_nothing_asset_names.append(output_asset_name)

            for output_asset_name, output_partitioned_asset in partitioned_output_assets.items():
                for partition_key, partitioned_asset_val in output_partitioned_asset.items():
                    materialized_output_assets[output_asset_name + f"__{partition_key}"] = partitioned_asset_val

                materialized_output_assets.pop(output_asset_name)

            after_pipeline_run_hook_op = self._create_after_pipeline_run_hook_op(
                job_name, pipeline, output_nothing_asset_names, partitioned_output_assets
            )
            after_pipeline_run_hook_op(**materialized_output_assets)

            pprint("OP INPUTSS", materialized_output_assets)
            pprint("OP INS", after_pipeline_run_hook_op.ins)

        # Overrides the kedro_run resource with the one created for the job
        kedro_run_translator = KedroRunTranslator(
            context=self._context,
            project_path=self._project_path,
            env=self._env,
            session_id=self._session_id,
        )
        kedro_run_resource = kedro_run_translator.to_dagster(
            pipeline_name=pipeline_name,
            filter_params=filter_params,
        )
        resource_defs = {"kedro_run": kedro_run_resource}

        for dataset_name in pipeline.all_inputs() | pipeline.all_outputs():
            asset_name = format_dataset_name(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                resource_defs[f"{self._env}__{asset_name}_io_manager"] = self._named_resources[
                    f"{self._env}__{asset_name}_io_manager"
                ]

        if self._enable_mlflow and is_mlflow_enabled():
            resource_defs |= {"mlflow": self._named_resources["mlflow"]}

        job = pipeline_graph.to_job(
            name=f"{self._env}__{job_name}",
            resource_defs=resource_defs,
            executor_def=executor_def,
            logger_defs=logger_defs,
        )

        return job

    def to_dagster(self) -> dict[str, dg.JobDefinition]:
        """Translate the Kedro pipelines into Dagster jobs.

        Returns:
            dict[str, JobDefinition]: The translated Dagster jobs.
        """
        named_jobs = {}
        for job_name, job_config in self._dagster_config.jobs.items():  # type: ignore[attr-defined]
            pipeline_config = job_config.pipeline.model_dump()

            pipeline_name = pipeline_config.get("pipeline_name", "__default__")
            filter_params = get_filter_params_dict(pipeline_config)
            pipeline = pipelines.get(pipeline_name).filter(**filter_params)

            executor_config = job_config.executor
            if executor_config in self._named_executors:
                executor_def = self._named_executors[executor_config]
            else:
                raise ValueError(f"Executor `{executor_config}` not found.")

            job = self.translate_pipeline(
                pipeline=pipeline,
                pipeline_name=pipeline_name,
                filter_params=filter_params,
                job_name=job_name,
                executor_def=executor_def,
            )

            named_jobs[job_name] = job

        return named_jobs
