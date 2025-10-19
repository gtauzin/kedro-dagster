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
    format_partition_key,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext


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

    def _enumerate_partition_keys(self, partitions_def: dg.PartitionsDefinition) -> list[str]:
        """Enumerate partition keys for an asset, does not support multi partitions.

        Args:
            partitions_def: PartitionsDefinition of the asset to enumerate partitions for.

        Returns:
            List of partition keys.
        """
        if not partitions_def:
            return []

        if isinstance(partitions_def, dg.MultiPartitionsDefinition):
            raise NotImplementedError("MultiPartitionsDefinition is not supported.")

        return list(partitions_def.get_partition_keys())

    def _get_node_partition_keys(self, node) -> list[Any]:
        """Get partition keys for a node.

        Args:
            node: The Kedro node to analyze

        """
        # Check partitioning consistency among output datasets
        # Output datasets can be either all partitioned or all non-partitioned (excluding nothing datasets)
        out_asset_names = [format_dataset_name(dataset_name) for dataset_name in node.outputs]
        partitioned_out_asset_names = [
            asset_name for asset_name in out_asset_names if asset_name in self._asset_partitions
        ]
        non_partitioned_out_asset_names = [
            asset_name for asset_name in out_asset_names
            if asset_name not in self._asset_partitions and not is_nothing_asset_name(self._catalog, unformat_asset_name(asset_name))
        ]

        if partitioned_out_asset_names and non_partitioned_out_asset_names:
            partitioned_out_dataset_names = [unformat_asset_name(asset_name) for asset_name in partitioned_out_asset_names]
            non_partitioned_out_dataset_names = [unformat_asset_name(asset_name) for asset_name in non_partitioned_out_asset_names]
            raise ValueError(
                f"Node '{node.name}' has mixed partitioned and non-partitioned non-nothing outputs: "
                f"partitioned={partitioned_out_dataset_names}, non-partitioned, non-nothing={non_partitioned_out_dataset_names}. "
                "All outputs must be either partitioned or nothing datasets if any output is partitioned."
            )

        downstream_per_upstream_partition_key: dict[str, str] = {}
        for out_dataset_name in node.outputs:
            out_asset_name = format_dataset_name(out_dataset_name)
            if out_asset_name in self._asset_partitions:
                out_partitions_def = self._asset_partitions[out_asset_name].get("partitions_def")

            for in_dataset_name in node.inputs:
                in_asset_name = format_dataset_name(in_dataset_name)
                if in_asset_name in self._asset_partitions and out_asset_name in self._asset_partitions:
                    in_partitions_def = self._asset_partitions[in_asset_name].get("partitions_def")
                    partition_mappings = self._asset_partitions[in_asset_name].get("partition_mappings", None)

                    in_partition_keys = self._enumerate_partition_keys(in_partitions_def)
                    partition_mapping = get_partition_mapping(
                        partition_mappings,
                        upstream_asset_name=in_dataset_name,
                        downstream_dataset_names=[out_dataset_name],
                        config_resolver= self._catalog.config_resolver,
                    )

                    if partition_mapping is None:
                        # Identity mapping
                        partition_mapping = dg.IdentityPartitionMapping()

                    for in_partition_key in in_partition_keys:
                        mapped_downstream_key = partition_mapping.get_downstream_partitions_for_partitions(
                            upstream_partitions_subset=in_partitions_def.empty_subset().with_partition_keys(
                                [in_partition_key]
                            ),
                            upstream_partitions_def=in_partitions_def,
                            downstream_partitions_def=out_partitions_def,
                        )[0]
                        # TODO: Support 1 to many
                        mapped_downstream_key = list(mapped_downstream_key)[0]
                        downstream_per_upstream_partition_key[f"{in_asset_name}|{in_partition_key}"] = f"{out_asset_name}|{mapped_downstream_key}"

        return downstream_per_upstream_partition_key


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
        after_pipeline_run_asset_names: list[str],
    ) -> dg.OpDefinition:
        after_pipeline_run_hook_ins: dict[str, dg.In] = {}
        for asset_name in after_pipeline_run_asset_names:
            after_pipeline_run_hook_ins[asset_name] = dg.In(dagster_type=dg.Nothing)

        required_resource_keys = {"kedro_run"}
        if self._enable_mlflow and is_mlflow_enabled():
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook_op(context: dg.OpExecutionContext) -> dg.Nothing:  # type: ignore[no-untyped-def]
            kedro_run_resource = context.resources.kedro_run
            run_params = kedro_run_resource.run_params

            self._hook_manager.hook.after_pipeline_run(
                run_params=run_params,
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
            materialized_in_assets: dict[str, Any] = {}
            for dataset_name in pipeline.inputs():
                asset_name = format_dataset_name(dataset_name)
                if not _is_param_name(dataset_name):
                    # External assets first
                    if asset_name in self._named_assets:
                        materialized_in_assets[asset_name] = self._named_assets[asset_name]
                    else:
                        asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                        materialized_in_assets[asset_name] = dg.AssetSpec(key=asset_key).with_io_manager_key(
                            f"{self._env}__{asset_name}_io_manager"
                        )

            partitioned_out_assets: dict[str, dict[str, Any]] = {}
            after_pipeline_run_hook_inputs: dict[str, Any] = {}

            n_layers = len(pipeline.grouped_nodes)
            for i_layer, layer in enumerate(pipeline.grouped_nodes):
                is_node_in_first_layer = (i_layer == 0)
                is_node_in_last_layer = (i_layer == n_layers - 1)

                for node in layer:
                    downstream_per_upstream_partition_key = self._get_node_partition_keys(node)

                    op_name = format_node_name(node.name) + "_graph"

                    inputs_kwargs: dict[str, Any] = {}
                    for in_dataset_name in node.inputs:
                        in_asset_name = format_dataset_name(in_dataset_name)
                        if _is_param_name(in_dataset_name):
                            continue

                        if in_asset_name in materialized_in_assets:
                            inputs_kwargs[in_asset_name] = materialized_in_assets[in_asset_name]

                    if is_node_in_first_layer:
                        inputs_kwargs["before_pipeline_run_hook_output"] = before_pipeline_run_hook_output

                    if not downstream_per_upstream_partition_key:
                        # Unpartitioned single invocation
                        base_op = self._named_op_factories[op_name](
                            is_in_first_layer=is_node_in_first_layer,
                            is_in_last_layer=is_node_in_last_layer,
                        )

                        partition_keys_per_in_asset_names = {}
                        for asset_name, asset_partitions_val in partitioned_out_assets.items():
                            dataset_name = unformat_asset_name(asset_name)
                            if asset_name in base_op.ins and is_nothing_asset_name(self._catalog, dataset_name):
                                partition_keys_per_in_asset_names[asset_name] = [
                                    format_partition_key(partition_key)
                                    for partition_key in asset_partitions_val.keys()
                                ]
                                for partition_key, asset_partition_val in asset_partitions_val.items():
                                    formatted_partition_key = format_partition_key(partition_key)
                                    inputs_kwargs[asset_name + f"__{formatted_partition_key}"] = asset_partition_val

                        op = self._named_op_factories[op_name](
                            is_in_first_layer=is_node_in_first_layer,
                            is_in_last_layer=is_node_in_last_layer,
                            partition_keys_per_in_asset_names=partition_keys_per_in_asset_names,
                        )
                        res = op(**inputs_kwargs)

                        # Capture outputs
                        if hasattr(res, "output_name"):
                            # Single output
                            materialized_out_assets_op = {res.output_name: res}
                        else:
                            materialized_out_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res
                            }

                        for out_dataset in node.outputs:
                            out_asset_name = format_dataset_name(out_dataset)
                            out_asset = materialized_out_assets_op.get(out_asset_name)

                            if out_asset is not None:
                                materialized_in_assets[out_asset_name] = out_asset

                        for out_asset_name, out_asset in materialized_out_assets_op.items():
                            if out_asset_name.endswith("_after_pipeline_run_hook_input"):
                                after_pipeline_run_hook_inputs[out_asset_name] = out_asset
                        continue

                    # Partitioned: clone per partition key
                    # TODO: support 1 to many partition mappings
                    for in_asset_partition_key, out_asset_partition_key in downstream_per_upstream_partition_key.items():
                        op_partition_keys = {
                            "upstream_partition_key": in_asset_partition_key,
                            "downstream_partition_key": out_asset_partition_key,
                        }
                        op = self._named_op_factories[op_name](
                            is_in_first_layer=is_node_in_first_layer,
                            is_in_last_layer=is_node_in_last_layer,
                            partition_keys=op_partition_keys,
                        )

                        res = op(**inputs_kwargs)

                        # Capture outputs
                        if hasattr(res, "output_name"):
                            # Single output
                            materialized_out_assets_op = {res.output_name: res}
                        else:
                            materialized_out_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res  # type: ignore[assignment]
                            }

                        for out_asset_name, out_asset in materialized_out_assets_op.items():
                            if not out_asset_name.endswith("_after_pipeline_run_hook_input"):
                                out_partition_key = out_asset_partition_key.split("|")[1]
                                formatted_out_partition_key = format_partition_key(out_partition_key)

                                partitioned_out_assets.setdefault(out_asset_name, {})[out_partition_key] = out_asset

                            elif is_node_in_last_layer:
                                after_pipeline_run_hook_in_name = (
                                    out_asset_name.split("_after_pipeline_run_hook_input")[0] + f"__{formatted_out_partition_key}" +"_after_pipeline_run_hook_input"
                                )
                                after_pipeline_run_hook_inputs[after_pipeline_run_hook_in_name] = out_asset


            after_pipeline_run_hook_op = self._create_after_pipeline_run_hook_op(
                job_name, pipeline, after_pipeline_run_hook_inputs.keys()
            )
            after_pipeline_run_hook_op(**after_pipeline_run_hook_inputs)

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
