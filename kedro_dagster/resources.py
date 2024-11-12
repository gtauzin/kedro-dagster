from pathlib import Path
from dagster import get_dagster_logger, IOManager, InputContext, OutputContext

from kedro.framework.startup import bootstrap_project

from .utils import kedro_init

def load_io_managers_from_kedro_datasets(env: str | None = None):
    """
    Get the IO managers for an environment.

    Args:
        env (str): The environment name.

    Returns:
        io_managers (dict): A dictionary of IO managers.
    
    """

    logger = get_dagster_logger()
    project_path = Path.cwd()

    project_metadata = bootstrap_project(project_path)
    logger.info("Project name: %s", project_metadata.project_name)

    logger.info("Initializing Kedro...")
    _, catalog, _ = kedro_init(
        project_path=project_path, env=env
    )

    io_managers = {}
    for dataset_name in catalog.list():
        if not dataset_name.startswith("params:") and dataset_name != "parameters":
            # TODO: Make use of https://docs.dagster.io/_apidocs/io-managers#dagster.io_manager
            # so that we can have a description
            class DatasetIOManager(IOManager):
                f"""IO Manager for kedro dataset `{dataset_name}`."""
                def __init__(self, dataset):
                    self.dataset = dataset

                def handle_output(self, context: OutputContext, obj):
                    self.dataset.save(obj)

                def load_input(self, context: InputContext):
                    return self.dataset.load()

            io_managers[f"{dataset_name}_io_manager"] = DatasetIOManager(catalog._get_dataset(dataset_name))

    return io_managers