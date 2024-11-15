from pathlib import PurePosixPath

from dagster import InputContext, IOManager, IOManagerDefinition, OutputContext, get_dagster_logger
from kedro.io import DataCatalog, MemoryDataset
from pydantic import ConfigDict, create_model


def load_io_managers_from_kedro_datasets(catalog: DataCatalog):
    """
    Get the IO managers for an environment.

    Args:
        env (str): The environment name.

    Returns:
        io_managers (dict): A dictionary of IO managers.

    """

    logger = get_dagster_logger()

    logger.info("Creating IO managers...")
    io_managers = {}
    for dataset_name in catalog.list():
        if not dataset_name.startswith("params:") and dataset_name != "parameters":
            dataset = catalog._get_dataset(dataset_name)

            if isinstance(dataset, MemoryDataset):
                continue

            # TODO: Figure out why this does not allow to see the config of the io managers in dagit
            dataset_config = {
                key: val if not isinstance(val, PurePosixPath) else str(val) for key, val in dataset._describe().items()
            }
            DatasetParameters = create_model(
                "DatasetParameters",
                __config__=ConfigDict(arbitrary_types_allowed=True),
                dataset=(type(dataset), dataset),
                **{key: (type(val), val) for key, val in dataset_config.items()},
            )

            class DatasetIOManager(IOManager, DatasetParameters):
                f"""IO Manager for kedro dataset `{dataset_name}`."""
                __name__ = f"{dataset_name}_io_manager"

                def __call__(self):
                    return self

                def handle_output(self, context: OutputContext, obj):
                    self.dataset.save(obj)

                def load_input(self, context: InputContext):
                    return self.dataset.load()

            io_managers[f"{dataset_name}_io_manager"] = IOManagerDefinition(
                DatasetIOManager(dataset=dataset, **dataset_config),
                description=f"IO Manager for kedro dataset `{dataset_name}`.",
            )

    return io_managers
