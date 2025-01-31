"""Translation function from Kedro to Dagtser."""

import warnings
from logging import getLogger
from pathlib import Path

import dagster as dg
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project

from kedro_dagster.assets import load_assets_from_kedro_nodes
from kedro_dagster.config import get_dagster_config
from kedro_dagster.executors import load_executors_from_kedro_config
from kedro_dagster.jobs import load_jobs_from_kedro_config
from kedro_dagster.loggers import get_kedro_loggers
from kedro_dagster.ops import load_ops_from_kedro_nodes
from kedro_dagster.resources import load_io_managers_from_kedro_datasets
from kedro_dagster.schedules import load_schedules_from_kedro_config

LOGGER = getLogger(__name__)

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)


def translate_kedro(
    env: str | None = None,
) -> tuple[
    list[dg.AssetsDefinition],
    dict[str, dg.ResourceDefinition],
    dict[str, dg.JobDefinition],
    dict[str, dg.LoggerDefinition],
]:
    """Translate Kedro project into Dagster.

    Args:
        env: A string representing the Kedro environment to use.
            If None, the default environment is used.

    Returns:
        Tuple[
            List[AssetsDefinition],
            Dict[str, ResourceDefinition],
            Dict[str, JobDefinition],
            Dict[str, LoggerDefinition]
        ]: A tuple containing a list of Dagster assets, a dictionary
        of Dagster resources, a dictionary of Dagster jobs, and a
        dictionary of Dagster loggers.

    """
    LOGGER.info("Initializing Kedro...")
    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()

    LOGGER.info("Bootstrapping project")
    project_metadata = bootstrap_project(project_path)
    LOGGER.info("Project name: %s", project_metadata.project_name)

    # bootstrap project within task / flow scope
    session = KedroSession.create(
        project_path=project_path,
        env=env,
    )
    session_id = session.session_id

    LOGGER.info(
        "Session created with ID %s",
    )

    LOGGER.info("Loading context...")
    context = session.load_context()
    catalog = context.catalog
    dagster_config = get_dagster_config(context)
    hook_manager = context._hook_manager
    default_pipeline = pipelines.get("__default__")

    assets, multi_asset_node_dict = load_assets_from_kedro_nodes(default_pipeline, catalog, hook_manager, session_id)
    op_node_dict = load_ops_from_kedro_nodes(default_pipeline, catalog, hook_manager, session_id)
    executors = load_executors_from_kedro_config(dagster_config)
    job_dict = load_jobs_from_kedro_config(
        dagster_config,
        multi_asset_node_dict,
        op_node_dict,
        executors,
        catalog,
        hook_manager,
        session_id,
        project_path,
        env,
    )
    jobs = list(job_dict.values())
    schedules = load_schedules_from_kedro_config(dagster_config, job_dict)
    loggers = get_kedro_loggers(project_metadata.package_name)
    io_managers = load_io_managers_from_kedro_datasets(default_pipeline, catalog, hook_manager)
    resources = io_managers

    LOGGER.info("Kedro project translated into Dagster.")

    return assets, resources, jobs, schedules, loggers, executors
