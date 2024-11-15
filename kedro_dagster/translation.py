"""Translation function."""

from pathlib import Path

from dagster import get_dagster_logger
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project

from kedro_dagster.assets import load_assets_from_kedro_nodes
from kedro_dagster.config import get_dagster_config
from kedro_dagster.jobs import load_jobs_from_kedro_config
from kedro_dagster.resources import load_io_managers_from_kedro_datasets


def translate_kedro(
    env: str | None = None,
):
    """Translate Kedro project into Dagster assets and resources.

    Args:
        env: A string representing the Kedro environment to use.
            If None, the default environment is used.

    Returns:
        Tuple[List[AssetDefinition], Dict[str, ResourceDefinition]]: A tuple containing a list of
            Dagster assets and a dictionary of Dagster resources.
    """
    logger = get_dagster_logger()

    logger.info("Initializing Kedro...")
    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()

    logger.info("Bootstrapping project")
    project_metadata = bootstrap_project(project_path)
    logger.info("Project name: %s", project_metadata.project_name)

    # bootstrap project within task / flow scope
    session = KedroSession.create(
        project_path=project_path,
        env=env,
    )
    session_id = session.session_id

    logger.info(
        "Session created with ID %s",
    )

    logger.info("Loading context...")
    context = session.load_context()
    config_loader = context.config_loader
    catalog = context.catalog
    catalog_config = config_loader.get("catalog")
    dagster_config = get_dagster_config(context)

    kedro_assets = load_assets_from_kedro_nodes(catalog, catalog_config, session_id)
    kedro_io_managers = load_io_managers_from_kedro_datasets(catalog)
    kedro_jobs = load_jobs_from_kedro_config(dagster_config)

    logger.info("Kedro project translated into Dagster.")

    return kedro_assets, kedro_io_managers, kedro_jobs
