"""Dagster io manager definitons from Kedro catalog."""

from pathlib import PurePosixPath
from pluggy import PluginManager

from dagster import Config, ConfigurableIOManager, InputContext, IOManagerDefinition, OutputContext, ResourceDefinition, get_dagster_logger
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict

from kedro_dagster.config import KedroMlflowConfig
from kedro_dagster.utils import _create_pydantic_model_from_dict


def get_mlflow_resource_from_config(mlflow_config: KedroMlflowConfig) -> ResourceDefinition:
    from dagster_mlflow import mlflow_tracking

    # TODO: Define custom mlflow resource
    mlflow_resource = mlflow_tracking.configured({
        "experiment_name": mlflow_config.tracking.experiment.name,
        "mlflow_tracking_uri": mlflow_config.server.mlflow_tracking_uri,
        "parent_run_id": None,
        "env": {
            # "MLFLOW_S3_ENDPOINT_URL": "my_s3_endpoint",
            # "AWS_ACCESS_KEY_ID": "my_aws_key_id",
            # "AWS_SECRET_ACCESS_KEY": "my_secret",
        },
        "env_to_tag": [],
        "extra_tags": {},
    })

    return {"mlflow": mlflow_resource}


def load_io_managers_from_kedro_datasets(
    default_pipeline: Pipeline, 
    catalog: DataCatalog, 
    hook_manager: PluginManager,
) -> dict[str, IOManagerDefinition]:
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

    logger = get_dagster_logger()

    node_dict = {node.name: node for node in default_pipeline.nodes}

    logger.info("Creating IO managers...")
    io_managers = {}
    for dataset_name in catalog.list():
        if not dataset_name.startswith("params:") and dataset_name != "parameters":
            dataset = catalog._get_dataset(dataset_name)

            if isinstance(dataset, MemoryDataset):
                continue

            def get_io_manager_definition(dataset, dataset_name):
                # TODO: Figure out why thisConfigDict does not allow to see the config of the io managers in dagit
                dataset_config = {
                    key: val if not isinstance(val, PurePosixPath) else str(val)
                    for key, val in dataset._describe().items()
                    if key not in ["version"]
                }  # | {"dataset": dataset}

                DatasetModel = _create_pydantic_model_from_dict(
                    dataset_config,
                    __base__=Config,
                    __config__=ConfigDict(arbitrary_types_allowed=True),
                )

                class ConfiguredDatasetIOManager(DatasetModel, ConfigurableIOManager):
                    f"""IO Manager for kedro dataset `{dataset_name}`."""

                    def handle_output(self, context: OutputContext, obj):
                        op_name = context.op_def.name
                        if not op_name.endswith("after_pipeline_run_hook"):
                            node = node_dict[op_name]
                            hook_manager.hook.before_dataset_saved(
                                dataset_name=dataset_name,
                                data=obj,
                                node=node,
                            )

                        dataset.save(obj)

                        if not op_name.endswith("after_pipeline_run_hook"):
                            hook_manager.hook.after_dataset_saved(
                                dataset_name=dataset_name,
                                data=obj,
                                node=node,
                            )

                    def load_input(self, context: InputContext):
                        op_name = context.op_def.name
                        if not op_name.endswith("after_pipeline_run_hook"):
                            node = node_dict[op_name]
                            hook_manager.hook.before_dataset_loaded(
                                dataset_name=dataset_name,
                                node=node,
                            )

                        data = dataset.load()

                        if not op_name.endswith("after_pipeline_run_hook"):
                            hook_manager.hook.after_dataset_loaded(
                                dataset_name=dataset_name,
                                data=data,
                                node=node,
                            )

                        return data

                return ConfiguredDatasetIOManager(**dataset_config)

            io_managers[f"{dataset_name}_io_manager"] = get_io_manager_definition(dataset, dataset_name)

    return io_managers
