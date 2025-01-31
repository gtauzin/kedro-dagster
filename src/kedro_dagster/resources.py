"""Dagster io manager definitons from Kedro catalog."""

from logging import getLogger
from pathlib import PurePosixPath
from typing import Any

import dagster as dg
from kedro.framework.project import pipelines
from kedro.io import KedroDataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from pluggy import PluginManager
from pydantic import ConfigDict, PrivateAttr

from kedro_dagster.utils import _create_pydantic_model_from_dict

LOGGER = getLogger(__name__)


def load_io_managers_from_kedro_datasets(
    default_pipeline: Pipeline,
    catalog: KedroDataCatalog,
    hook_manager: PluginManager,
) -> dict[str, dg.IOManagerDefinition]:
    """
    Get the IO managers from Kedro datasets.

    Args:
        default_pipeline: The Kedro default ``Pipeline``.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.

    Returns:
        Dict[str, IOManagerDefinition]: A dictionary of DagsterIO managers.

    """

    node_dict = {node.name: node for node in default_pipeline.nodes}

    LOGGER.info("Creating IO managers...")
    io_managers = {}
    for dataset_name in catalog.list():
        if not dataset_name.startswith("params:") and dataset_name != "parameters":
            dataset = catalog._get_dataset(dataset_name)

            if isinstance(dataset, MemoryDataset):
                continue

            def get_io_manager_definition(dataset, dataset_name):
                # TODO: Figure out why this ConfigDict does not allow to see the config of the io managers in dagit
                dataset_config = {
                    key: val if not isinstance(val, PurePosixPath) else str(val)
                    for key, val in dataset._describe().items()
                    if key not in ["version"]
                    and val
                    is not None  # TODO: Why are those condition necessary? We could want to edit them on launchpad
                }  # | {"dataset": dataset}

                DatasetModel = _create_pydantic_model_from_dict(
                    dataset_config,
                    __base__=dg.Config,
                    __config__=ConfigDict(arbitrary_types_allowed=True),
                )

                class ConfiguredDatasetIOManager(DatasetModel, dg.ConfigurableIOManager):
                    f"""IO Manager for kedro dataset `{dataset_name}`."""

                    def handle_output(self, context: dg.OutputContext, obj):
                        op_name = context.op_def.name
                        node = node_dict[op_name]
                        hook_manager.hook.before_dataset_saved(
                            dataset_name=dataset_name,
                            data=obj,
                            node=node,
                        )

                        dataset.save(obj)

                        hook_manager.hook.after_dataset_saved(
                            dataset_name=dataset_name,
                            data=obj,
                            node=node,
                        )

                    def load_input(self, context: dg.InputContext):
                        op_name = context.op_def.name
                        node = node_dict[op_name]
                        hook_manager.hook.before_dataset_loaded(
                            dataset_name=dataset_name,
                            node=node,
                        )

                        data = dataset.load()

                        hook_manager.hook.after_dataset_loaded(
                            dataset_name=dataset_name,
                            data=data,
                            node=node,
                        )

                        return data

                return ConfiguredDatasetIOManager(**dataset_config)

            io_managers[f"{dataset_name}_io_manager"] = get_io_manager_definition(dataset, dataset_name)

    return io_managers


def load_pipeline_hook_translation(
    run_params,
    catalog: KedroDataCatalog,
    hook_manager: PluginManager,
) -> dict[str, dg.IOManagerDefinition]:
    class FilterParamsModel(dg.Config):
        node_names: list[str] | None = None
        from_nodes: list[str] | None = None
        to_nodes: list[str] | None = None
        from_inputs: list[str] | None = None
        to_outputs: list[str] | None = None
        node_namespace: str | None = None

    class RunParamsModel(FilterParamsModel):
        session_id: str
        project_path: str | None = None
        env: str | None = None
        kedro_version: str | None = None
        pipeline_name: str | None = None
        tags: list[str] | None = None
        load_versions: dict[str, str] | None = None
        extra_params: dict[str, Any] | None = None
        runner: str | None = None

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

            hook_manager.hook.before_pipeline_run(
                run_params=self.dict() | filter_params,
                pipeline=self._pipeline,
                catalog=catalog,
            )
            context.log.info("Pipelinke hook resource setup executed `before_pipeline_run` hook.")

            context.log.info(self.filter_params_dict())

        def teardown_after_execution(self, context: dg.InitResourceContext):
            # Make sure `after_pipeline_run` is not called in case of an error
            # Here we assume there is no error if all job outputs have
            # been computed
            output_asset_names = [asset_key.path for asset_key in context.dagster_run.asset_selection]
            if set(output_asset_names) == set(self._run_results.keys()):
                hook_manager.hook.after_pipeline_run(
                    run_params=self.dict(),
                    run_result=self._run_results,
                    pipeline=self._pipeline,
                    catalog=catalog,
                )
                context.log.info("Pipelinke hook resource teardown executed `after_pipeline_run` hook.")

            else:
                context.log.info("Pipelinke hook resource teardown did not execute `after_pipeline_run` hook.")

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

            hook_manager.hook.on_pipeline_error(
                error=error_class_name(error_message),
                run_params=run_params,
                pipeline=pipeline,
                catalog=catalog,
            )
            context.log.info("Pipeline hook sensor executed `on_pipeline_error` hook`.")

    return PipelineHookResource(**run_params), on_pipeline_error_sensor
