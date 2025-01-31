"""Dagster io manager definitons from Kedro catalog."""

from logging import getLogger
from pathlib import PurePosixPath

import dagster as dg
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from pluggy import PluginManager
from pydantic import ConfigDict, PrivateAttr

from kedro_dagster.utils import _create_pydantic_model_from_dict

LOGGER = getLogger(__name__)


def load_io_managers_from_kedro_datasets(
    default_pipeline: Pipeline,
    catalog: DataCatalog,
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


def get_pipeline_hook_resource(
    pipeline,
    catalog,
    hook_manager,
):
    class PipelineHookResource(dg.ConfigurableResource):
        run_params: dict
        _run_results: dict = PrivateAttr()

        def add_run_results(self, asset_name, asset):
            self._run_results[asset_name] = asset

        def setup_for_execution(self, context: dg.InitResourceContext):
            self._run_results = {}
            context.log.info("Resource setup complete.")
            hook_manager.hook.before_pipeline_run(
                run_params=self.run_params,
                pipeline=pipeline,
                catalog=catalog,
            )

        def teardown_after_execution(self, context: dg.InitResourceContext):
            status = context.dagster_run.status
            if status == dg.DagsterRunStatus.SUCCESS:
                hook_manager.hook.after_pipeline_run(
                    run_params=self.run_params,
                    run_result=self._run_results,
                    pipeline=pipeline,
                    catalog=catalog,
                )

                context.log.info("Pipeline run succeeded.")

            else:
                hook_manager.hook.on_pipeline_error(
                    error=context.op_exception,
                    run_params=self.run_params,
                    pipeline=pipeline,
                    catalog=catalog,
                )
                context.log.info("Pipeline run failed.")

            context.log.info("Resource teardown complete.")
