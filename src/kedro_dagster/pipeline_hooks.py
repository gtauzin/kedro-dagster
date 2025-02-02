"""Translation of Kedro pipeline hooks."""

from typing import Any

import dagster as dg
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline
from pydantic import PrivateAttr

from kedro_dagster.utils import FilterParamsModel, RunParamsModel


class PipelineHookTranslator:
    """Translator for Kedro pipeline hooks."""

    def _create_pipeline_hook_resource(self, run_params: dict[str, Any]) -> dg.IOManagerDefinition:
        class PipelineHookResource(RunParamsModel, dg.ConfigurableResource):
            """Resource for kedro  `before_pipeline_run` and `after_pipeline_run` hooks."""

            _run_results: dict = PrivateAttr()
            _pipeline: Pipeline = PrivateAttr()

            def filter_params_dict(self) -> dict:
                filter_params = FilterParamsModel.__fields__.keys()
                return {key: val for key, val in self.dict().items() if key in filter_params}

            def get_pipeline(self, filter_params):
                pipeline = pipelines.get("__default__")
                if self.pipeline_name is not None:
                    pipeline = pipelines.get(self.pipeline_name)

                pipeline = pipeline.filter(**filter_params)

                return pipeline

            def add_run_results(self, asset_name, asset):
                self._run_results[asset_name] = asset

            def setup_for_execution(self, context: dg.InitResourceContext):
                self._run_results = {}

                # In the case where we start a run without using the predifined kedro-dagster jobs
                # e.g. by materializing selected assets from the Assets tab of Dagster UI, we want
                # the pipeline to correspond to the actual nodes ran by dagster
                node_names = None
                if all(filter_param is None for filter_param in self.filter_params_dict().values()):
                    node_names = context.dagster_run.step_keys_to_execute

                filter_params = self.filter_params_dict()

                if node_names is not None:
                    filter_params |= {"node_names": node_names}

                self._pipeline = self.get_pipeline(filter_params)

                self._context._hook_manager.hook.before_pipeline_run(
                    run_params=self.dict() | filter_params,
                    pipeline=self._pipeline,
                    catalog=self._context.catalog,
                )
                context.log.info("Pipelinke hook resource setup executed `before_pipeline_run` hook.")

                context.log.info(self.filter_params_dict())

            def teardown_after_execution(self, context: dg.InitResourceContext):
                # Make sure `after_pipeline_run` is not called in case of an error
                # Here we assume there is no error if all job outputs have
                # been computed
                output_asset_names = [asset_key.path for asset_key in context.dagster_run.asset_selection]
                if set(output_asset_names) == set(self._run_results.keys()):
                    self._context._hook_manager.hook.after_pipeline_run(
                        run_params=self.dict(),
                        run_result=self._run_results,
                        pipeline=self._pipeline,
                        catalog=self._context.catalog,
                    )
                    context.log.info("Pipelinke hook resource teardown executed `after_pipeline_run` hook.")

                else:
                    context.log.info("Pipelinke hook resource teardown did not execute `after_pipeline_run` hook.")

        return PipelineHookResource(**run_params)

    def translate_pipeline_hook(self, run_params: dict[str, Any]) -> dict[str, dg.IOManagerDefinition]:
        """Translate Kedro pipeline hooks to Dagster resource and sensor."""

        self.named_resources_["pipeline_hook"] = self._create_pipeline_hook_resource(run_params)

        @dg.run_failure_sensor(
            name="on_pipeline_error_sensor",
            description="Sensor for kedro `on_pipeline_error` hook.",
            monitored_jobs=None,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def on_pipeline_error_sensor(context: dg.RunFailureSensorContext):
            if "pipeline_hook" in context.resource_defs:
                pipeline_hook_resource = context.resource_defs["pipeline_hook"]
                pipeline = pipeline_hook_resource._pipeline
                run_params = pipeline_hook_resource.dict()

                error_class_name = context.failure_event.event_specific_data.error.cls_name
                error_message = context.failure_event.event_specific_data.error.message

                context.log.error(error_message)

                self._context._hook_manager.hook.on_pipeline_error(
                    error=error_class_name(error_message),
                    run_params=run_params,
                    pipeline=pipeline,
                    catalog=self._context.catalog,
                )
                context.log.info("Pipeline hook sensor executed `on_pipeline_error` hook`.")

        self.named_sensors_["on_pipeline_error_sensor"] = on_pipeline_error_sensor
