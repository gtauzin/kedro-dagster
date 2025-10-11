"""Translation of Kedro pipelines to Dagster jobs."""

from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.datasets.partitioned_dataset import DagsterPartitionedDataset
from kedro_dagster.utils import (
    _is_param_name,
    format_dataset_name,
    format_node_name,
    unformat_asset_name,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    is_mlflow_enabled,
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
        named_ops (dict[str, OpDefinition]): The named ops.
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
        asset_partitions: dict[str, dict[str, dg.PartitionsDefinition | dg.PartitionMapping | None]],
        named_ops: dict[str, dg.OpDefinition],
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
        self._named_ops = named_ops
        self._named_resources = named_resources
        self._named_executors = named_executors
        self._enable_mlflow = enable_mlflow

    def _get_partitioned_config(self, pipeline: Pipeline) -> dict[str, Any]:
        """Get the partitioned config for the pipeline.

        Args:
            pipeline (Pipeline): The Kedro pipeline.
        Returns:
            dict[str, Any]: The partitioned config.
        """
        partitions_definitions: dict[str, dg.PartitionsDefinition] = {}
        for dataset_name in pipeline.all_inputs() | pipeline.all_outputs():
            asset_name = format_dataset_name(dataset_name)
            if not _is_param_name(dataset_name) and asset_name not in self._named_assets:
                if asset_name in self._asset_partitions:
                    partitions_def = self._asset_partitions[asset_name]["partitions_def"]
                    partitions_definitions[asset_name] = partitions_def

        if not partitions_definitions:
            return None

        if len(partitions_definitions) == 1:
            is_partitions_def_multiple = False
            partitions_def = list(partitions_definitions.values())[0]
        else:
            is_partitions_def_multiple = True
            partitions_def = dg.MultiPartitionsDefinition(partitions_definitions)

        def tags_for_partition_key_fn(partition_key: str) -> dict[str, str]:
            partitioned_config: dict[str, Any] = {"resources": {}, "ops": {}}

            for asset_name in partitions_definitions.keys():
                if is_partitions_def_multiple:
                    partition_key = partition_key[asset_name]

                if asset_name in self._asset_partitions:
                    partition_mapping = self._asset_partitions[asset_name]["partition_mapping"]
                    io_manager_key = f"{self._env}__{asset_name}_io_manager"
                    partitioned_config["resources"][io_manager_key] = {
                        "config": {"partition_key": partition_key}
                    }

            for node in pipeline.nodes:
                op_name = format_node_name(node.name)
                partitioned_config["ops"][op_name] = {
                    "config": {"partition_key": partition_key}
                }

            return partitioned_config

        partitioned_config = dg.PartitionedConfig(
            partitions_def=partitions_def,
            tags_for_partition_key_fn=tags_for_partition_key_fn,
            run_config_for_partition_key_fn=tags_for_partition_key_fn,
        )

        return partitioned_config

    def _create_pipeline_hook_ops(self, job_name: str, pipeline: Pipeline) -> tuple[dg.OpDefinition, dg.OpDefinition]:
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
        def before_pipeline_run_hook(context: dg.OpExecutionContext) -> dg.Nothing:
            kedro_run_resource = context.resources.kedro_run
            kedro_run_resource.after_context_created_hook()

            self._hook_manager.hook.before_pipeline_run(
                run_params=kedro_run_resource.run_params,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        after_pipeline_run_hook_ins = {
            f"{format_node_name(node.name)}_after_pipeline_run_hook_input": dg.In(dagster_type=dg.Nothing)
            for node in pipeline.nodes
        }
        for dataset_name in pipeline.all_outputs():
            if not _is_param_name(dataset_name):
                asset_name = format_dataset_name(dataset_name)
                after_pipeline_run_hook_ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook(context: dg.OpExecutionContext, **materialized_assets) -> dg.Nothing:  # type: ignore[no-untyped-def]
            kedro_run_resource = context.resources.kedro_run

            run_results = {}
            for dataset_name in pipeline.outputs():
                asset_name = format_dataset_name(dataset_name)
                run_results[dataset_name] = materialized_assets[asset_name]

            self._hook_manager.hook.after_pipeline_run(
                run_params=kedro_run_resource.run_params,
                run_result=run_results,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        return before_pipeline_run_hook, after_pipeline_run_hook

    def translate_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_name: str,
        filter_params: dict[str, Any],
        job_name: str,
        executor_def: dg.ExecutorDefinition | None = None,
        logger_defs: dict[str, dg.LoggerDefinition] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job.

        Args:
            pipeline (Pipeline): The Kedro pipeline.
            pipeline_name (str): The name of the Kedro pipeline.
            filter_params (dict[str, Any]): Filter parameters for the pipeline.
            job_name (str): The name of the job.
            executor_def (ExecutorDefinition): The executor definition.
            logger_defs (dict[str, LoggerDefinition] | None): The logger definitions.

        Returns:
            JobDefinition: A Dagster job definition.
        """
        (
            before_pipeline_run_hook,
            after_pipeline_run_hook,
        ) = self._create_pipeline_hook_ops(job_name, pipeline)

        @dg.graph(
            name=f"{self._env}__{job_name}",
            description=f"Job derived from pipeline associated to `{job_name}` in env `{self._env}`.",
            out=None,
        )
        def pipeline_graph() -> None:
            before_pipeline_run_hook_output = before_pipeline_run_hook()

            # Fil up materialized_assets with pipeline input assets
            materialized_input_assets = {}
            for dataset_name in pipeline.inputs():
                if not _is_param_name(dataset_name):
                    asset_name = format_dataset_name(dataset_name)

                    # First, we account for external assets
                    if asset_name in self._named_assets:
                        materialized_input_assets[asset_name] = self._named_assets[asset_name]
                    else:
                        asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                        materialized_input_assets[asset_name] = dg.AssetSpec(
                            key=asset_key,
                        ).with_io_manager_key(f"{self._env}__{asset_name}_io_manager")

            materialized_output_assets: dict[str, Any] = {}
            for layer in pipeline.grouped_nodes:
                for node in layer:
                    op_name = format_node_name(node.name) + "_graph"
                    op = self._named_ops[op_name]

                    materialized_input_assets_op = {}
                    for input_dataset_name in node.inputs:
                        input_asset_name = format_dataset_name(input_dataset_name)
                        if input_asset_name in materialized_input_assets:
                            materialized_input_assets_op[input_asset_name] = materialized_input_assets[input_asset_name]
                    
                    materialized_outputs = op(
                        before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                        **materialized_input_assets_op,
                    )

                    if len(node.outputs) == 0:
                        materialized_output_assets_op = {materialized_outputs.output_name: materialized_outputs}
                    else:
                        materialized_output_assets_op = {
                            materialized_output.output_name: materialized_output
                            for materialized_output in materialized_outputs
                        }
                    materialized_input_assets |= materialized_output_assets_op
                    materialized_output_assets |= materialized_output_assets_op

            after_pipeline_run_hook(**materialized_output_assets)

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

        partitioned_config = self._get_partitioned_config(pipeline)

        # TODO: Description and tags?
        job = pipeline_graph.to_job(
            name=f"{self._env}__{job_name}",
            resource_defs=resource_defs,
            executor_def=executor_def,
            logger_defs=logger_defs,
            config=partitioned_config
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
