"""Dagster job definitons from Kedro pipelines."""

from typing import Any

import dagster as dg
from kedro.framework.context import KedroContext
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.utils import _is_asset_name, dagster_format, get_asset_key_from_dataset_name, is_mlflow_enabled


class PipelineTranslator:
    """ """

    def __init__(
        self,
        dagster_config: dict[str, Any],
        context: KedroContext,
        project_path: str,
        env: str,
        session_id: str,
        named_assets: dict[str, dg.AssetsDefinition],
        named_ops: dict[str, dg.OpDefinition],
        named_resources: dict[str, dg.ResourceDefinition],
        named_executors: dict[str, dg.ExecutorDefinition],
    ):
        self._dagster_config = dagster_config
        self._context = context
        self._project_path = project_path
        self._env = env
        self._session_id = session_id
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._named_assets = named_assets
        self._named_ops = named_ops
        self._named_resources = named_resources
        self._named_executors = named_executors

    @staticmethod
    def _get_filter_params_dict(pipeline_config: dict[str, Any]):
        filter_params = dict(
            tags=pipeline_config.get("tags"),
            from_nodes=pipeline_config.get("from_nodes"),
            to_nodes=pipeline_config.get("to_nodes"),
            node_names=pipeline_config.get("node_names"),
            from_inputs=pipeline_config.get("from_inputs"),
            to_outputs=pipeline_config.get("to_outputs"),
            node_namespace=pipeline_config.get("node_namespace"),
        )

        return filter_params

    def _create_pipeline_hook_ops(self, job_name: str, pipeline: Pipeline):
        required_resource_keys = {"kedro_run"}
        if is_mlflow_enabled():
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
            f"{dagster_format(node.name)}_after_pipeline_run_hook_input": dg.In(dagster_type=dg.Nothing)
            for node in pipeline.nodes
        }
        for dataset_name in pipeline.all_outputs():
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
                after_pipeline_run_hook_ins[asset_name] = dg.In(asset_key=dg.AssetKey(asset_name))

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook(context: dg.OpExecutionContext, **materialized_assets) -> dg.Nothing:
            kedro_run_resource = context.resources.kedro_run

            run_results = {}
            for dataset_name in pipeline.outputs():
                asset_name = dagster_format(dataset_name)
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
        pipeline_config: dict[str, Any],
        job_name: str,
        executor_def: dg.ExecutorDefinition | None = None,
        partitions_def: dg.PartitionsDefinition | None = None,
        op_retry_policy: dg.RetryPolicy | None = None,
        logger_defs: dict[str, dg.LoggerDefinition] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job.

        Args:
            pipeline_config: The configuration of the pipeline.
            job_name: The name of the job.

        Returns:
            JobDefinition: A Dagster job definition.

        """
        pipeline_name = pipeline_config.get("pipeline_name")
        filter_params = self._get_filter_params_dict(pipeline_config)
        pipeline = pipelines.get(pipeline_name).filter(**filter_params)

        (
            before_pipeline_run_hook,
            after_pipeline_run_hook,
        ) = self._create_pipeline_hook_ops(job_name, pipeline)

        @dg.graph(
            name=job_name,
            description=f"Job derived from pipeline associated to `{job_name}`.",
            out=None,
        )
        def pipeline_graph():
            before_pipeline_run_hook_output = before_pipeline_run_hook()

            # Fil up materialized_assets with pipeline input assets
            materialized_input_assets = {}
            for dataset_name in pipeline.inputs():
                asset_name = dagster_format(dataset_name)
                if _is_asset_name(asset_name):
                    # First, we account for external assets
                    if asset_name in self._named_assets:
                        materialized_input_assets[asset_name] = self._named_assets[asset_name]
                    else:
                        asset_key = get_asset_key_from_dataset_name(dataset_name)
                        materialized_input_assets[asset_name] = dg.AssetSpec(
                            key=asset_key,
                        ).with_io_manager_key(f"{asset_name}_io_manager")

            materialized_output_assets = {}
            for layer in pipeline.grouped_nodes:
                for node in layer:
                    op_name = dagster_format(node.name) + "_graph"
                    op = self._named_ops[op_name]

                    materialized_input_assets_op = {}
                    for input_dataset_name in node.inputs:
                        input_asset_name = dagster_format(input_dataset_name)
                        if input_asset_name in materialized_input_assets:
                            materialized_input_assets_op[input_asset_name] = materialized_input_assets[input_asset_name]

                    materialized_outputs = op(
                        before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                        **materialized_input_assets_op,
                    )

                    if len(node.outputs) == 0:
                        materialized_output_assets_op = {materialized_outputs.output_name: materialized_outputs}
                    elif len(node.outputs) > 0:
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
        kedro_run_resource = kedro_run_translator._create_kedro_run_resource(
            pipeline_name=pipeline_name,
            filter_params=filter_params,
            load_versions=pipeline_config.get("load_versions"),
            extra_params=pipeline_config.get("extra_params"),
        )
        resource_defs = {"kedro_run": kedro_run_resource}

        for dataset_name in pipeline.all_inputs() | pipeline.all_outputs():
            asset_name = dagster_format(dataset_name)
            if f"{asset_name}_io_manager" in self._named_resources:
                resource_defs[f"{asset_name}_io_manager"] = self._named_resources[f"{asset_name}_io_manager"]

        if is_mlflow_enabled():
            resource_defs |= {"mlflow": self._named_resources["mlflow"]}

        job = pipeline_graph.to_job(
            name=job_name,
            resource_defs=resource_defs,
            executor_def=executor_def,
            partitions_def=partitions_def,
            op_retry_policy=op_retry_policy,
            logger_defs=logger_defs,
        )

        return job

    def translate_pipelines(self):
        named_jobs = {}
        for job_name, job_config in self._dagster_config.jobs.items():
            pipeline_config = job_config.pipeline.model_dump()

            executor_config = job_config.executor
            if isinstance(executor_config, str):
                if executor_config in self._named_executors:
                    executor_def = self._named_executors[executor_config]
                else:
                    raise ValueError(f"Executor `{executor_config}` not found.")

            job = self.translate_pipeline(
                pipeline_config=pipeline_config,
                job_name=job_name,
                executor_def=executor_def,
            )

            named_jobs[job_name] = job

        return named_jobs
