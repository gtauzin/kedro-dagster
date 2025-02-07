"""Dagster job definitons from Kedro pipelines."""

from typing import Any

import dagster as dg
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.utils import FilterParamsModel, RunParamsModel, _is_asset_name


class PipelineTranslator:
    @staticmethod
    def _get_filter_params(pipeline_config: dict[str, Any] | None = None) -> FilterParamsModel:
        # TODO: Remove all defaults in get as the config takes care of default values
        filter_params = {}
        if pipeline_config is not None:
            filter_params = dict(
                tags=pipeline_config.get("tags", None),
                from_nodes=pipeline_config.get("from_nodes", None),
                to_nodes=pipeline_config.get("to_nodes", None),
                node_names=pipeline_config.get("node_names", None),
                from_inputs=pipeline_config.get("from_inputs", None),
                to_outputs=pipeline_config.get("to_outputs", None),
                node_namespace=pipeline_config.get("node_namespace", None),
            )

        filter_params = FilterParamsModel(**filter_params)
        return filter_params

    def _get_run_params(self, pipeline_config: dict[str, Any] | None = None) -> RunParamsModel:
        run_params = dict(
            session_id=self._session_id,
            project_path=str(self._project_path),
            env=self.env,
            kedro_version=kedro_version,
        )
        if pipeline_config is not None:
            run_params |= dict(
                pipeline_name=pipeline_config.get("pipeline_name", None),
                load_versions=pipeline_config.get("load_versions", None),
                extra_params=pipeline_config.get("extra_params", None),
                runner=pipeline_config.get("runner", None),
            )

        run_params = self._get_filter_params(pipeline_config).dict() | run_params
        run_params = RunParamsModel(**run_params)
        return run_params

    def translate_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_config: dict[str, Any],
        job_name: str,
        executor_def: dg.ExecutorDefinition | None = None,
        partitions_def: dg.PartitionsDefinition | None = None,
        op_retry_policy: dg.RetryPolicy | None = None,
        logger_defs: dict[str, dg.LoggerDefinition] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job.

        Args:
            pipeline: The Kedro pipeline to translate.
            pipeline_config: The configuration of the pipeline.
            job_name: The name of the job.

        Returns:
            JobDefinition: A Dagster job definition.

        """

        run_params = self._get_run_params(pipeline_config).model_dump()

        # TODO: Set up run_params as a config resources that provides a way to access
        # pipeline?
        @dg.op(
            name=f"before_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed before the `{job_name}` pipeline run.",
            out={"before_pipeline_run_hook_output": dg.Out(dagster_type=dg.Nothing)},
        )
        def before_pipeline_run_hook() -> dg.Nothing:
            self._context._hook_manager.hook.before_pipeline_run(
                run_params=run_params,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins={
                f"{node.name}_after_pipeline_run_hook_input": dg.In(dagster_type=dg.Nothing) for node in pipeline.nodes
            }
            | {
                asset_name: dg.In(asset_key=dg.AssetKey(asset_name))
                for asset_name in pipeline.all_outputs()
                if not asset_name.startswith("params:")
            },
        )
        def after_pipeline_run_hook(**materialized_assets) -> dg.Nothing:
            run_results = {asset_name: materialized_assets[asset_name] for asset_name in pipeline.outputs()}

            self._context._hook_manager.hook.after_pipeline_run(
                run_params=run_params,
                run_result=run_results,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        @dg.graph(
            name=job_name,
            description=f"Job derived from pipeline associated to `{job_name}`.",
            out=None,
        )
        def pipeline_graph():
            before_pipeline_run_hook_output = before_pipeline_run_hook()

            # Fil up materialized_assets with pipeline input assets
            materialized_assets = {}
            for asset_name in pipeline.inputs():
                if _is_asset_name(asset_name):
                    # First, we account for external assets
                    if asset_name in self.named_assets_:
                        materialized_assets[asset_name] = self.named_assets_[asset_name]
                    else:
                        materialized_assets[asset_name] = dg.AssetSpec(
                            asset_name,
                        ).with_io_manager_key(f"{asset_name}_io_manager")

            for layer in pipeline.grouped_nodes:
                for node in layer:
                    op = self._named_ops[f"{node.name}_graph"]

                    materialized_input_assets = {
                        input_name: materialized_assets[input_name]
                        for input_name in node.inputs
                        if input_name in materialized_assets
                    }

                    materialized_outputs = op(
                        before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                        **materialized_input_assets,
                    )

                    if len(node.outputs) == 0:
                        materialized_output_assets = {materialized_outputs.output_name: materialized_outputs}
                    elif len(node.outputs) > 0:
                        materialized_output_assets = {
                            materialized_output.output_name: materialized_output
                            for materialized_output in materialized_outputs
                        }

                    materialized_assets |= materialized_output_assets

            after_pipeline_run_hook(
                **materialized_assets,
            )

        resource_defs = {
            f"{asset_name}_io_manager": self.named_resources_[f"{asset_name}_io_manager"]
            for asset_name in pipeline.all_inputs() | pipeline.all_outputs()
            if f"{asset_name}_io_manager" in self.named_resources_
        }

        # Overrides the pipeline_hook resource with the one created for the job
        # pipeline_hook_resource = self._create_pipeline_hook_resource(run_params=run_params)
        # resource_defs |= {"pipeline_hook": pipeline_hook_resource}

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
        for job_name, job_config in self._dagster_config.jobs.items():
            pipeline_config = job_config.pipeline.model_dump()

            pipeline_name = pipeline_config.get("pipeline_name")
            filter_params = self._get_filter_params(pipeline_config)
            pipeline = pipelines.get(pipeline_name).filter(**filter_params.dict())

            executor_config = job_config.executor
            if isinstance(executor_config, str):
                if executor_config in self.named_executors_:
                    executor_def = self.named_executors_[executor_config]
                else:
                    raise ValueError(f"Executor `{executor_config}` not found.")

            job = self.translate_pipeline(
                pipeline=pipeline,
                pipeline_config=pipeline_config,
                job_name=job_name,
                executor_def=executor_def,
            )

            self.named_jobs_[job_name] = job
