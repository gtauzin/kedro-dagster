"""Translation function from Kedro to Dagtser."""

from pathlib import Path

from dagster import AssetsDefinition, JobDefinition, LoggerDefinition, ResourceDefinition, get_dagster_logger
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project

from kedro_dagster.assets import load_assets_from_kedro_nodes
from kedro_dagster.config import get_dagster_config, get_mlflow_config
from kedro_dagster.jobs import load_jobs_from_kedro_config
from kedro_dagster.loggers import get_kedro_loggers
from kedro_dagster.ops import load_ops_from_kedro_nodes
from kedro_dagster.resources import get_mlflow_resource_from_config, load_io_managers_from_kedro_datasets
from kedro_dagster.utils import _include_mlflow


def translate_kedro(
    env: str | None = None,
) -> tuple[
    list[AssetsDefinition],
    dict[str, ResourceDefinition],
    dict[str, JobDefinition],
    dict[str, LoggerDefinition],
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
    catalog = context.catalog
    dagster_config = get_dagster_config(context)
    hook_manager = context._hook_manager
    default_pipeline = pipelines.get("__default__")

    kedro_resources = {}
    if _include_mlflow():
        mlflow_config = get_mlflow_config(context)
        kedro_resources = {"mlflow": get_mlflow_resource_from_config(mlflow_config)}

    kedro_assets, multi_asset_node_dict, asset_input_dict = load_assets_from_kedro_nodes(
        default_pipeline, catalog, hook_manager, session_id
    )
    op_node_dict = load_ops_from_kedro_nodes(default_pipeline, catalog, hook_manager, session_id)
    kedro_jobs, before_pipeline_run_hook = load_jobs_from_kedro_config(
        dagster_config,
        asset_input_dict,
        multi_asset_node_dict,
        op_node_dict,
        catalog,
        hook_manager,
        session_id,
        project_path,
        env,
    )
    kedro_assets.append(before_pipeline_run_hook)
    kedro_loggers = get_kedro_loggers(project_metadata.package_name)
    kedro_io_managers = load_io_managers_from_kedro_datasets(default_pipeline, catalog, hook_manager)
    kedro_resources |= kedro_io_managers

    logger.info("Kedro project translated into Dagster.")

    return kedro_assets, kedro_resources, kedro_jobs, kedro_loggers
