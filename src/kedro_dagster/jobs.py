""" """

from typing import Any

from dagster import AssetSelection, graph, op
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline
from kedro.io import CatalogProtocol

from kedro_dagster.config import KedroDagsterConfig
from kedro_dagster.utils import _include_mlflow




def get_job_from_pipeline(
    multi_asset_node_dict: dict[str, Any],
    pipeline: Pipeline,
    catalog: CatalogProtocol,
    job_name: str,
    job_config: dict,
    hook_manager,
    run_params: dict[str, Any],

):
    """Create a Dagster job from a Kedro pipeline.

    Args:
        multi_asset_node_dict: A dictionary of node names mapped to their correspondingmulti-asset.
        pipeline: A Kedro pipeline.
        catalog: A Kedro catalog.
        job_name: The name of the job.
        job_config: The configuration of the job.
        hook_manager: A Kedro hook manager.
        run_params: The parameters of the run.

    Returns:
        A Dagster job.
    """
    
    @op
    def before_pipeline_run_hook():
        hook_manager.hook.before_pipeline_run(
            run_params=run_params, 
            pipeline=pipeline, 
            catalog=catalog,
        )

    @op
    def after_pipeline_run_hook():
        hook_manager.hook.after_pipeline_run(
            run_params=run_params,
            run_result=run_results,
            pipeline=pipeline,
            catalog=catalog,
        )

    @op
    def on_pipeline_error_hook(error):
            hook_manager.hook.on_pipeline_error(
            error=error,
            run_params=run_params,
            pipeline=pipeline,
            catalog=catalog,
        )

    @graph(
        name=job_name,
        description=f"Graph derived from pipeline associated to the `{job_name}` job.",
        ins=None,
        out=None,
        tags=None,
        config=None,
    )
    def pipeline_graph():
        before_pipeline_run_hook()

        try:
            for node in pipeline.nodes:
                if node.name in multi_asset_node_dict:
                    multi_asset_op = multi_asset_node_dict[node.name]
                    multi_asset_op()
                
        except Exception as exec:
            on_pipeline_error_hook(exec)
            
        after_pipeline_run_hook()

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

    job = pipeline_graph.to_job(
        name=None, 
        description=None, 
        resource_defs=None, 
        config=job_config.get("config", None),
        tags=job_config.get("tags", None),
        metadata=job_config.get("metadata", None),
        logger_defs=job_config.get("logger_defs", None),
        executor_def=job_config.get("executor_def", None),
        op_retry_policy=job_config.get("op_retry_policy", None),
        partitions_def=job_config.get("partitions_def", None),
        asset_layer=job_config.get("asset_layer", None),
        input_values=job_config.get("input_values", None),
        run_tags=job_config.get("run_tags", None),
        hooks=hooks, 
        _asset_selection_data=None,
        op_selection=None, 
    )

    return job


def load_jobs_from_kedro_config(
    dagster_config: KedroDagsterConfig,
    multi_asset_node_dict: dict,
    catalog: CatalogProtocol,
    hook_manager,
    session_id: str,
    project_path: str,
    env: str,
) -> list[dict[str, Any]]:
    """Loads job definitions from a Kedro pipeline.

    Args:
        dagster_config :

    Returns:
        A list of dagster job definitions.

    """
    jobs = []
    for job_name, job_config in dagster_config.jobs.items():
        pipeline_name = job_config.pipeline.pipeline_name

        filter_params = dict(
            tags=job_config.tags,
            from_nodes=job_config.from_nodes,
            to_nodes=job_config.to_nodes,
            node_names=job_config.node_names,
            from_inputs=job_config.from_inputs,
            to_outputs=job_config.to_outputs,
            namespace=job_config.pipeline.namespace,
        )

        run_params = filter_params | dict(
            session_id=session_id,
            project_path=project_path,
            env=env,
            kedro_version=kedro_version,
            pipeline_name=pipeline_name,
            load_versions=job_config.load_versions,
            extra_params=job_config.extra_params,
            runner=job_config.pipeline.runner,
        )
    

        pipeline = pipelines.get(pipeline_name).filter(
            **filter_params
        )

        job = get_job_from_pipeline(
            multi_asset_node_dict=multi_asset_node_dict,
            pipeline=pipeline,
            catalog=catalog,
            hook_manager=hook_manager,
            job_name=job_name,
            job_config=job_config.pipeline.dict(),
            run_params=run_params,
        )

        jobs.append(job)

    return jobs

