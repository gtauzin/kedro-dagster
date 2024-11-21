""" """

from typing import Any

from dagster import AssetSelection, define_asset_job
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.config import KedroDagsterConfig
from kedro_dagster.utils import _include_mlflow


def get_job_from_pipeline(
    pipeline: Pipeline,
    job_name: str,
    job_config: dict,
) -> list[str]:
    asset_list = []
    for node in pipeline.nodes:
        output_assets = [node.name]
        if len(node.outputs):
            output_assets = node.outputs

        asset_list.extend(output_assets)

    asset_selection = AssetSelection.assets(*asset_list)

    hooks = None
    if _include_mlflow():
        from dagster_mlflow import end_mlflow_on_run_finished

        hooks = {end_mlflow_on_run_finished}

    job = define_asset_job(
        job_name,
        selection=asset_selection,
        config=job_config.get("config", None),
        description=job_config.get("description", None),
        tags=job_config.get("tags", None),
        run_tags=job_config.get("run_tags", None),
        metadata=job_config.get("metadata", None),
        partitions_def=job_config.get("partitions", None),
        executor_def=job_config.get("executor", None),
        hooks=hooks,
        op_retry_policy=job_config.get("op_retry_policy", None),
    )
    return job


def load_jobs_from_kedro_config(
    dagster_config: KedroDagsterConfig,
) -> list[dict[str, Any]]:
    """Loads job definitions from a Kedro pipeline.

    Args:
        dagster_config :

    Returns:
        A list of dagster job definitions.

    """
    jobs = []
    for job_name, job_config in dagster_config.jobs.items():
        job = get_job_from_pipeline(
            pipeline=pipelines.get(job_config.pipeline.pipeline_name),
            job_name=job_name,
            job_config=job_config.pipeline.dict(),
        )

        jobs.append(job)

    return jobs
