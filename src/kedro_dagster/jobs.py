""" """

from typing import Any

from dagster import AssetIn, AssetKey, AssetOut, AssetSpec, AssetSelection, In, Nothing, multi_asset, graph, op
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline
from kedro.io import CatalogProtocol

from kedro_dagster.config import KedroDagsterConfig
from kedro_dagster.utils import _include_mlflow


def get_job_from_pipeline(
    before_pipeline_run_hook,
    asset_input_dict: dict[str, AssetSpec],
    multi_asset_node_dict: dict[str, Any],
    op_node_dict: dict[str, Any],
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

    @op(
        name=f"{job_name}_after_pipeline_run_hook",
        description=f"Hook to be executed after the `{job_name}` pipeline run.",
        ins={asset_name: In(asset_key=AssetKey(asset_name)) for asset_name in pipeline.outputs()},
        # out=None,
        # tags=None,
        # config_schema=None,
    )
    def after_pipeline_run_hook(**run_results):
        hook_manager.hook.after_pipeline_run(
            run_params=run_params,
            run_result=run_results,
            pipeline=pipeline,
            catalog=catalog,
        )

    @op(
        name=f"{job_name}_on_pipeline_error_hook",
        description=f"Hook to be executed when the `{job_name}` pipeline run fails.",
        # ins={"error": Exception},
        # out=None,
        # tags=None,
        # config_schema=None,
    )
    def on_pipeline_error_hook(error):
            hook_manager.hook.on_pipeline_error(
            error=error,
            run_params=run_params,
            pipeline=pipeline,
            catalog=catalog,
        )

    ins = {}
    for asset_name in pipeline.inputs():
        if not asset_name.startswith("params:"):
            ins[asset_name] = AssetIn(
                key=asset_name,
                input_manager_key=f"{asset_name}_io_manager",
            )

    @graph(
        name=job_name,
        description=f"Graph derived from pipeline associated to the `{job_name}` job.",
        # ins=ins,
        out=None,
        tags=None,
        config=None,
    )
    def pipeline_graph():
        materialized_before_pipeline_run_hook_outputs = before_pipeline_run_hook()

        materialized_assets = {
            materialized_output.output_name: materialized_output
            for materialized_output in materialized_before_pipeline_run_hook_outputs
        }

        # TODO: Not sure graph supports try/except
        try:
            for layer in pipeline.grouped_nodes:
                for node in layer:
                    if node.name in multi_asset_node_dict:
                        if len(node.outputs):
                            op = multi_asset_node_dict[node.name]
                        else:
                            op = op_node_dict[node.name]

                        node_inputs = node.inputs
                        node_inputs.append(f"{node.name}_before_pipeline_run_hook")

                        materialized_input_assets = {
                            input_name: asset_input_dict[input_name]
                             for input_name in node_inputs
                             if input_name in asset_input_dict
                        }
                        
                        materialized_input_assets |= {
                            input_name: materialized_assets[input_name]
                            for input_name in node_inputs
                            if input_name in materialized_assets
                        }

                        materialized_outputs = op(**materialized_input_assets)

                        if len(node.outputs) == 1:
                            materialized_output_assets = {
                                materialized_outputs.output_name: materialized_outputs
                            }
                        elif len(node.outputs) > 1:
                            materialized_output_assets = {
                                materialized_output.output_name: materialized_output
                                for materialized_output in materialized_outputs
                            }

                        materialized_assets |= materialized_output_assets

        except Exception as exec:
            on_pipeline_error_hook(exec)
        
        run_results = {
            asset_name: materialized_assets[asset_name]
             for asset_name in pipeline.outputs()
        }

        after_pipeline_run_hook(**run_results)

    hooks = None
    if _include_mlflow():
        from dagster_mlflow import end_mlflow_on_run_finished

        hooks = {end_mlflow_on_run_finished}

    job = pipeline_graph.to_job(
        name=job_name, 
        description=f"", 
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
    asset_input_dict: dict,
    multi_asset_node_dict: dict,
    op_node_dict: dict,
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
    @multi_asset(
        name="before_pipeline_run_hook",
        group_name="hooks",
        description=f"Hook to be executed before a pipeline run.",
        outs={
            f"{node.name}_before_pipeline_run_hook": AssetOut(
                key=f"{node.name}_before_pipeline_run_hook",
                description=f"Untangible asset corresponding to `{node.name}` for the `before_pipeline_run` hook.",
                dagster_type=Nothing,
                is_required=False,
            ) 
            for node in pipelines["__default__"].nodes},
    )
    def before_pipeline_run_hook():
        hook_manager.hook.before_pipeline_run(
            run_params=run_params, 
            pipeline=pipeline, 
            catalog=catalog,
        )

        return tuple([None]* len(pipelines["__default__"].nodes))

    jobs = []
    for job_name, job_config in dagster_config.jobs.items():
        pipeline_config = job_config.pipeline.model_dump()
        pipeline_name = pipeline_config.get("pipeline_name")

        # TODO: Remove all defaults in get as the config takes care of default values 
        filter_params = dict(
            tags=pipeline_config.get("tags", None),
            from_nodes=pipeline_config.get("from_nodes", None),
            to_nodes=pipeline_config.get("to_nodes", None),
            node_names=pipeline_config.get("node_names", None),
            from_inputs=pipeline_config.get("from_inputs", None),
            to_outputs=pipeline_config.get("to_outputs", None),
            node_namespace=pipeline_config.get("node_namespace", None),
        )

        run_params = filter_params | dict(
            session_id=session_id,
            project_path=project_path,
            env=env,
            kedro_version=kedro_version,
            pipeline_name=pipeline_name,
            load_versions=pipeline_config.get("load_versions", None),
            extra_params=pipeline_config.get("extra_params", None),
            runner=pipeline_config.get("runner", None),
        )
    
        pipeline = pipelines.get(pipeline_name).filter(
            **filter_params
        )

        job = get_job_from_pipeline(
            before_pipeline_run_hook=before_pipeline_run_hook,
            asset_input_dict=asset_input_dict,
            multi_asset_node_dict=multi_asset_node_dict,
            op_node_dict=op_node_dict,
            pipeline=pipeline,
            catalog=catalog,
            hook_manager=hook_manager,
            job_name=job_name,
            job_config=job_config.pipeline.model_dump(),
            run_params=run_params,
        )

        jobs.append(job)

    return jobs, before_pipeline_run_hook

