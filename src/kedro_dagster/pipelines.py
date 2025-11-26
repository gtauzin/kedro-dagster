"""Translation of Kedro pipelines to Dagster jobs.

This module contains :class:`PipelineTranslator`, which takes Kedro pipelines
and produces fully wired Dagster jobs. It supports static fan-out of
partitioned nodes, Kedro hook invocation via dedicated hook ops, and resource
overrides per job.
"""

import warnings
from logging import getLogger
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.pipeline import Pipeline

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.utils import (
    _is_param_name,
    format_dataset_name,
    format_node_name,
    format_partition_key,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    get_partition_mapping,
    is_nothing_asset_name,
    unformat_asset_name,
)

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext
    from kedro.pipeline.node import Node

    from kedro_dagster.config.kedro_dagster import KedroDagsterConfig

LOGGER = getLogger(__name__)


class PipelineTranslator:
    """Translator for Kedro pipelines to Dagster jobs.

    Args:
        dagster_config (KedroDagsterConfig): Parsed configuration of the Dagster repository.
        context (KedroContext): Active Kedro context (provides catalog and hooks).
        project_path (str): Path to the Kedro project.
        env (str): Kedro environment used for namespacing.
        run_id (str): Kedro run ID. In Kedro < 1.0, this is called `session_id`.
        named_assets (dict[str, dg.AssetsDefinition]): Mapping of asset name -> asset.
        asset_partitions (dict[str, Any]): Mapping of asset name -> partition definitions/mappings.
        named_op_factories (dict[str, dg.OpDefinition]): Mapping of graph-op name -> op factory.
        named_resources (dict[str, dg.ResourceDefinition]): Mapping of resource name -> resource def.
        named_executors (dict[str, dg.ExecutorDefinition]): Mapping of executor name -> executor def.
        named_loggers (dict[str, dg.LoggerDefinition]): Mapping of logger name -> logger def.
        enable_mlflow (bool): Whether MLflow integration is enabled.
    """

    def __init__(
        self,
        dagster_config: "KedroDagsterConfig",
        context: "KedroContext",
        project_path: str,
        env: str,
        run_id: str,
        named_assets: dict[str, dg.AssetsDefinition],
        asset_partitions: dict[str, Any],
        named_op_factories: dict[str, dg.OpDefinition],
        named_resources: dict[str, dg.ResourceDefinition],
        named_executors: dict[str, dg.ExecutorDefinition],
        named_loggers: dict[str, dg.LoggerDefinition],
        enable_mlflow: bool,
    ):
        self._dagster_config = dagster_config
        self._context = context
        self._project_path = project_path
        self._env = env
        self._run_id = run_id
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._named_assets = named_assets
        self._asset_partitions = asset_partitions
        self._named_op_factories = named_op_factories
        self._named_resources = named_resources
        self._named_executors = named_executors
        self._named_loggers = named_loggers
        self._enable_mlflow = enable_mlflow

    def _enumerate_partition_keys(self, partitions_def: dg.PartitionsDefinition | None) -> list[str]:
        """Enumerate partition keys for an asset.

        Note: This method assumes the partition definition has already been validated
        by `DagsterPartitionedDataset`. Only `StaticPartitionsDefinition` should reach here.

        Args:
            partitions_def (PartitionsDefinition | None): Partitions definition of the asset to enumerate.

        Returns:
            list[str]: Partition keys.
        """
        if not partitions_def:
            return []

        return list(partitions_def.get_partition_keys())

    def _get_node_partition_keys(self, node: "Node") -> dict[str, str]:
        """Compute downstream partition key per upstream partition for a node.

        Returns a mapping of "upstream_asset|partition_key" to "downstream_asset|partition_key".
        If the node is unpartitioned, returns an empty mapping.

        Args:
            node: Kedro node to analyze.

        """
        # Check partitioning consistency among output datasets
        # Output datasets can be either all partitioned or all non-partitioned (excluding nothing datasets)
        out_asset_names = [format_dataset_name(dataset_name) for dataset_name in node.outputs]
        partitioned_out_asset_names = [
            asset_name for asset_name in out_asset_names if asset_name in self._asset_partitions
        ]
        non_partitioned_out_asset_names = [
            asset_name
            for asset_name in out_asset_names
            if asset_name not in self._asset_partitions
            and not is_nothing_asset_name(self._catalog, unformat_asset_name(asset_name))
        ]

        if partitioned_out_asset_names and non_partitioned_out_asset_names:
            partitioned_out_dataset_names = [
                unformat_asset_name(asset_name) for asset_name in partitioned_out_asset_names
            ]
            non_partitioned_out_dataset_names = [
                unformat_asset_name(asset_name) for asset_name in non_partitioned_out_asset_names
            ]
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
                        partition_mappings=partition_mappings,
                        upstream_asset_name=in_dataset_name,
                        downstream_dataset_names=[out_dataset_name],
                        config_resolver=self._catalog._config_resolver,
                    )

                    if partition_mapping is None:
                        # Identity mapping: same key propagated downstream
                        partition_mapping = dg.IdentityPartitionMapping()

                    for in_partition_key in in_partition_keys:
                        mapped_downstream_key = partition_mapping.get_downstream_partitions_for_partitions(
                            upstream_partitions_subset=in_partitions_def.empty_subset().with_partition_keys([
                                in_partition_key
                            ]),
                            upstream_partitions_def=in_partitions_def,
                            downstream_partitions_def=out_partitions_def,
                        )[0]
                        mapped_downstream_key = list(mapped_downstream_key)[0]
                        downstream_per_upstream_partition_key[f"{in_asset_name}|{in_partition_key}"] = (
                            f"{out_asset_name}|{mapped_downstream_key}"
                        )

        return downstream_per_upstream_partition_key

    def _create_before_pipeline_run_hook(self, job_name: str, pipeline: Pipeline) -> dg.OpDefinition:
        """Create the pipeline hook op executed before the pipeline run.

        Args:
            job_name (str): Job name.
            pipeline (Pipeline): Kedro pipeline.

        Returns:
            dg.OpDefinition: Op that triggers before-pipeline-run hooks.

        """
        required_resource_keys = {"kedro_run"}
        # Only require mlflow if provided in named resources
        if "mlflow" in self._named_resources:
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"before_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed before the `{job_name}` pipeline run.",
            out={"before_pipeline_run_hook_output": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
        )
        def before_pipeline_run_hook_op(context: dg.OpExecutionContext) -> dg.Nothing:
            kedro_run_resource = context.resources.kedro_run
            kedro_run_resource.after_context_created_hook()
            kedro_run_resource.after_catalog_created_hook()

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
        """Create the pipeline hook op executed after the pipeline run.

        Args:
            job_name (str): Job name.
            pipeline (Pipeline): Kedro pipeline.
            after_pipeline_run_asset_names (list[str]): Names of Nothing inputs to fan-in.

        Returns:
            dg.OpDefinition: Op that triggers after-pipeline-run hooks.
        """
        after_pipeline_run_hook_ins: dict[str, dg.In] = {}
        for asset_name in after_pipeline_run_asset_names:
            after_pipeline_run_hook_ins[asset_name] = dg.In(dagster_type=dg.Nothing)

        required_resource_keys = {"kedro_run"}
        # Only require mlflow if provided in named resources
        if "mlflow" in self._named_resources:
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook_op(context: dg.OpExecutionContext) -> dg.Nothing:
            kedro_run_resource = context.resources.kedro_run
            run_params = kedro_run_resource.run_params

            # NOTE: We set run_results=None because, in the Dagster context, we do not have access
            # to the Kedro run results dictionary (mapping dataset names to DatasetSaveError objects).
            # This means that hooks relying on run_results for error reporting or post-processing
            # will not receive this information. This is a known limitation of the Dagster integration.
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    r"Argument(s) 'run_result' which are declared in the hookspec cannot be found in this hook call",
                    UserWarning,
                    # NOTE: This filter depends on pluggy internals ('pluggy._hooks').
                    # If pluggy changes its internal structure, this filter may need to be updated.
                    "pluggy._hooks",
                )
                self._hook_manager.hook.after_pipeline_run(
                    run_results=None,
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
        loggers_config: dict[str, Any] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job with partition support.

        This method implements static fan-out for partitioned datasets:
        - Nodes with partitioned outputs/inputs are cloned for each partition key
        - Partition mappings define relationships between upstream and downstream partitions
        - Identity mapping is used by default (same partition key across assets)

        Args:
            pipeline (Pipeline): Kedro pipeline.
            pipeline_name (str): Name of the Kedro pipeline.
            filter_params (dict[str, Any]): Filter parameters for the pipeline.
            job_name (str): Name of the job.
            executor_def (ExecutorDefinition | None): Executor definition.
            logger_defs (dict[str, LoggerDefinition] | None): Logger definitions.
            loggers_config (dict[str, Any] | None): Logger configurations.

        Returns:
            dg.JobDefinition: Dagster job definition with partition-aware ops.
        """
        before_pipeline_run_hook_op = self._create_before_pipeline_run_hook(job_name, pipeline)

        @dg.graph(
            name=f"{self._env}__{job_name}",
            description=f"Job derived from pipeline associated to `{job_name}` in env `{self._env}`.",
            out=None,
        )
        def pipeline_graph() -> None:
            before_pipeline_run_hook_output = before_pipeline_run_hook_op()

            # Collect input assets
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
            # Iterate in topological order over layers of the pipeline
            for i_layer, layer in enumerate(pipeline.grouped_nodes):
                is_node_in_first_layer = i_layer == 0
                is_node_in_last_layer = i_layer == n_layers - 1

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
                                    format_partition_key(partition_key) for partition_key in asset_partitions_val.keys()
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
                            materialized_out_assets_op = {out_handle.output_name: out_handle for out_handle in res}

                        for out_dataset in node.outputs:
                            out_asset_name = format_dataset_name(out_dataset)
                            out_asset = materialized_out_assets_op.get(out_asset_name)

                            if out_asset is not None:
                                materialized_in_assets[out_asset_name] = out_asset

                        for out_asset_name, out_asset in materialized_out_assets_op.items():
                            if out_asset_name.endswith("_after_pipeline_run_hook_input"):
                                after_pipeline_run_hook_inputs[out_asset_name] = out_asset
                        continue

                    # Partitioned: fan out node execution per partition key
                    for (
                        in_asset_partition_key,
                        out_asset_partition_key,
                    ) in downstream_per_upstream_partition_key.items():
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
                            materialized_out_assets_op = {out_handle.output_name: out_handle for out_handle in res}

                        for out_asset_name, out_asset in materialized_out_assets_op.items():
                            out_partition_key = out_asset_partition_key.split("|")[1]
                            formatted_out_partition_key = format_partition_key(out_partition_key)

                            if not out_asset_name.endswith("_after_pipeline_run_hook_input"):
                                partitioned_out_assets.setdefault(out_asset_name, {})[out_partition_key] = out_asset
                            elif is_node_in_last_layer:
                                after_pipeline_run_hook_in_name = (
                                    out_asset_name.split("_after_pipeline_run_hook_input")[0]
                                    + f"__{formatted_out_partition_key}"
                                    + "_after_pipeline_run_hook_input"
                                )
                                after_pipeline_run_hook_inputs[after_pipeline_run_hook_in_name] = out_asset

            after_pipeline_run_hook_op = self._create_after_pipeline_run_hook_op(
                job_name, pipeline, list(after_pipeline_run_hook_inputs.keys())
            )
            after_pipeline_run_hook_op(**after_pipeline_run_hook_inputs)

        kedro_run_translator = KedroRunTranslator(
            context=self._context,
            project_path=self._project_path,
            env=self._env,
            run_id=self._run_id,
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

        # Expose mlflow resource only if provided
        if "mlflow" in self._named_resources:
            resource_defs |= {"mlflow": self._named_resources["mlflow"]}

        job = pipeline_graph.to_job(
            name=f"{self._env}__{job_name}",
            resource_defs=resource_defs,
            executor_def=executor_def,
            logger_defs=logger_defs,
            config=loggers_config,
        )

        return job

    def to_dagster(self) -> dict[str, dg.JobDefinition]:
        """Translate the Kedro pipelines into Dagster jobs.

        Returns:
            dict[str, dg.JobDefinition]: Translated Dagster jobs.
        """
        LOGGER.info("Translating Kedro pipelines to Dagster jobs...")
        # Lazy import to avoid circular dependency
        from kedro.framework.project import pipelines

        named_jobs: dict[str, dg.JobDefinition] = {}
        if self._dagster_config.jobs is None:
            LOGGER.debug("No jobs defined in configuration")
            return named_jobs

        LOGGER.debug(f"Processing {len(self._dagster_config.jobs)} job(s)")
        for job_name, job_config in self._dagster_config.jobs.items():
            LOGGER.debug(f"Translating job '{job_name}'...")
            pipeline_config = job_config.pipeline

            pipeline_name = pipeline_config.pipeline_name
            filter_params = get_filter_params_dict(pipeline_config.model_dump())
            pipeline = pipelines.get(pipeline_name).filter(**filter_params)

            # Handle executor configuration (string reference or inline config)
            executor_def = None
            executor_config = job_config.executor
            if executor_config is not None:
                if isinstance(executor_config, str):
                    # String reference to named executor
                    if executor_config in self._named_executors:
                        executor_def = self._named_executors[executor_config]
                    else:
                        msg = f"Executor '{executor_config}' not found. Available executors: {list(self._named_executors.keys())}"
                        LOGGER.error(msg)
                        raise ValueError(msg)
                else:
                    # Inline executor configuration - look for job-specific executor
                    job_executor_name = f"{job_name}__executor"
                    if job_executor_name in self._named_executors:
                        executor_def = self._named_executors[job_executor_name]
                    else:
                        msg = f"Job-specific executor '{job_executor_name}' not found. Available executors: {list(self._named_executors.keys())}"
                        LOGGER.error(msg)
                        raise ValueError(msg)

            # Handle logger configurations (string references and/or inline configs)
            logger_defs, logger_configs = {}, {}
            if job_config.loggers:
                LOGGER.debug(f"Processing {len(job_config.loggers)} loggers for job '{job_name}'...")
                for idx, logger_config in enumerate(job_config.loggers):
                    if isinstance(logger_config, str):
                        # String reference to named logger
                        if logger_config in self._named_loggers:
                            logger_defs[logger_config] = self._named_loggers[logger_config]
                        else:
                            msg = f"Logger '{logger_config}' not found. Available loggers: {list(self._named_loggers.keys())}"
                            LOGGER.error(msg)
                            raise ValueError(msg)

                        # If logger_config exists in _named_loggers, then loggers must exist - we assert for mypy
                        assert self._dagster_config.loggers is not None
                        logger_configs[logger_config] = self._dagster_config.loggers[logger_config]

                    else:
                        # Inline logger configuration - look for job-specific logger
                        job_logger_name = f"{job_name}__logger_{idx}"
                        if job_logger_name in self._named_loggers:
                            logger_defs[job_logger_name] = self._named_loggers[job_logger_name]
                        else:
                            msg = f"Job-specific logger '{job_logger_name}' for inline logger configuration not found."
                            LOGGER.error(msg)
                            raise ValueError(msg)

                        logger_configs[job_logger_name] = logger_config

            loggers_config = {}
            if logger_configs:
                loggers_config = {
                    "loggers": {
                        name: {"config": logger_config.model_dump()} for name, logger_config in logger_configs.items()
                    }
                }

            job = self.translate_pipeline(
                pipeline=pipeline,
                pipeline_name=pipeline_name,
                filter_params=filter_params,
                job_name=job_name,
                executor_def=executor_def,
                logger_defs=logger_defs,
                loggers_config=loggers_config,
            )

            named_jobs[job_name] = job
            LOGGER.debug(f"Successfully translated job '{job_name}'")

        LOGGER.debug(f"Translated {len(named_jobs)} Dagster job(s)")
        return named_jobs
