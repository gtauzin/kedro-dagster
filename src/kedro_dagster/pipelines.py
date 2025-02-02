"""Dagster job definitons from Kedro pipelines."""

from typing import Any

import dagster as dg
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.utils import FilterParamsModel, RunParamsModel


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

    def _translate_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_config: dict[str, Any],
        job_name: str,
        job_config: dict[str, Any],
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job.

        Args:
            pipeline: The Kedro pipeline to translate.
            pipeline_config: The configuration of the pipeline.
            job_name: The name of the job.

        Returns:
            JobDefinition: A Dagster job definition.

        """

        @dg.graph(
            name=job_name,
            description=f"Graph derived from pipeline associated to the `{job_name}` job.",
            out=None,
        )
        def pipeline_graph():
            materialized_assets = {}

            # TODO: Use named_assets instead of pasting this code
            for external_asset_name in pipeline.inputs():
                if not external_asset_name.startswith("params:"):
                    dataset = self._context.catalog._get_dataset(external_asset_name)
                    metadata = getattr(dataset, "metadata", None) or {}
                    description = metadata.pop("description", "")
                    materialized_assets[external_asset_name] = dg.AssetSpec(
                        external_asset_name,
                        description=description,
                        metadata=metadata,
                    ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")

            for layer in pipeline.grouped_nodes:
                for node in layer:
                    if node.name in self.named_assets_:
                        op = self.named_assets_[node.name]
                    else:
                        op = self._named_ops[node.name]

                    node_inputs = node.inputs

                    materialized_input_assets = {
                        input_name: materialized_assets[input_name]
                        for input_name in node_inputs
                        if input_name in materialized_assets
                    }

                    materialized_outputs = op(**materialized_input_assets)

                    if len(node.outputs) <= 1:
                        materialized_output_assets = {materialized_outputs.output_name: materialized_outputs}
                    elif len(node.outputs) > 1:
                        materialized_output_assets = {
                            materialized_output.output_name: materialized_output
                            for materialized_output in materialized_outputs
                        }

                    materialized_assets |= materialized_output_assets

        pipeline_hook_resource = self._create_pipeline_hook_resource(
            run_params=self._get_run_params(pipeline_config)
        )

        executor_config = job_config.executor
        if isinstance(executor_config, str):
            if executor_config in self.named_executors_:
                executor = self.named_executors_[executor_config]
            else:
                raise ValueError(f"Executor `{executor_config}` not found.")
        # Overrides the pipeline_hook resource with the one created for the job
        resource_defs = self.named_resources_ | {"pipeline_hook": pipeline_hook_resource}
        job = pipeline_graph.to_job(
            name=job_name,
            resource_defs=resource_defs,
            executor_def=executor,
        )

        return job

    def translate_pipelines(self):
        for job_name, job_config in self._dagster_config.jobs.items():
            pipeline_config = job_config.pipeline.model_dump()

            pipeline_name = pipeline_config.get("pipeline_name")
            filter_params = self._get_filter_params(pipeline_config)
            pipeline = pipelines.get(pipeline_name).filter(**filter_params.dict())

            job = self._translate_pipeline(
                pipeline=pipeline,
                pipeline_config=pipeline_config,
                job_name=job_name,
                job_config=job_config,
            )

            self.named_jobs_[job_name] = job
