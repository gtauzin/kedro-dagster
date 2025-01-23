"""Dagster job definitons from Kedro pipelines."""

from typing import Any

from dagster import (
    AssetKey,
    AssetSpec,
    ExecutorDefinition,
    HookContext,
    In,
    JobDefinition,
    Nothing,
    failure_hook,
    get_dagster_logger,
    graph,
    op,
)
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline

from kedro_dagster.config import KedroDagsterConfig
from kedro_dagster.utils import _include_mlflow


def get_job_from_pipeline(
    asset_input_dict: dict[str, AssetSpec],
    multi_asset_node_dict: dict[str, Any],
    op_node_dict: dict[str, Any],
    pipeline: Pipeline,
    catalog: DataCatalog,
    job_name: str,
    hook_manager,
    run_params: dict[str, Any],
    executor: ExecutorDefinition,
) -> JobDefinition:
    """Create a Dagster job from a Kedro ``Pipeline``.

    Args:
        multi_asset_node_dict: A dictionary of node names mapped to their
        corresponding multi-asset defintion.
        pipeline: The Kedro ``Pipeline`` to wrap into a job.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        job_name: The name of the job.
        hook_manager: The ``PluginManager`` to activate hooks.
        run_params: The parameters of the run.
        executor: .

    Returns:
        JobDefinition: A Dagster job.
    """

    @op(
        name=f"{job_name}_before_pipeline_run_hook",
        description="Hook to be executed before a pipeline run.",
    )
    def before_pipeline_run_hook() -> Nothing:
        hook_manager.hook.before_pipeline_run(
            run_params=run_params,
            pipeline=pipeline,
            catalog=catalog,
        )

    @op(
        name=f"{job_name}_after_pipeline_run_hook",
        description=f"Hook to be executed after the `{job_name}` pipeline run.",
        ins={asset_name: In(asset_key=AssetKey(asset_name)) for asset_name in pipeline.outputs()},
    )
    def after_pipeline_run_hook(**run_results):
        hook_manager.hook.after_pipeline_run(
            run_params=run_params,
            run_result=run_results,
            pipeline=pipeline,
            catalog=catalog,
        )

    required_resource_keys = None
    if _include_mlflow():
        required_resource_keys = {"mlflow"}

    @failure_hook(
        name=f"{job_name}_on_pipeline_error_hook",
        required_resource_keys=required_resource_keys,
    )
    def on_pipeline_error_hook(context: HookContext):
        hook_manager.hook.on_pipeline_error(
            error=context.op_exception,
            run_params=run_params,
            pipeline=pipeline,
            catalog=catalog,
        )

    @graph(
        name=job_name,
        description=f"Graph derived from pipeline associated to the `{job_name}` job.",
        # ins=ins,
        out=None,
    )
    def pipeline_graph():
        before_pipeline_run_hook_result = before_pipeline_run_hook()

        materialized_assets = {
            "before_pipeline_run_hook_result": before_pipeline_run_hook_result,
        }

        for layer in pipeline.grouped_nodes:
            for node in layer:
                if node.name in multi_asset_node_dict:
                    if len(node.outputs):
                        op = multi_asset_node_dict[node.name]
                    else:
                        op = op_node_dict[node.name]

                    node_inputs = node.inputs
                    node_inputs.append("before_pipeline_run_hook_result")

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

                    get_dagster_logger().info(materialized_input_assets.keys())

                    materialized_outputs = op(**materialized_input_assets)

                    if len(node.outputs) == 1:
                        materialized_output_assets = {materialized_outputs.output_name: materialized_outputs}
                    elif len(node.outputs) > 1:
                        materialized_output_assets = {
                            materialized_output.output_name: materialized_output
                            for materialized_output in materialized_outputs
                        }

                    materialized_assets |= materialized_output_assets

        run_results = {asset_name: materialized_assets[asset_name] for asset_name in pipeline.outputs()}

        after_pipeline_run_hook(**run_results)

    job = JobDefinition(
        name=job_name,
        graph_def=pipeline_graph,
        executor_def=executor,
        hook_defs={on_pipeline_error_hook},
    )

    return job


def load_jobs_from_kedro_config(
    dagster_config: KedroDagsterConfig,
    asset_input_dict: dict,
    multi_asset_node_dict: dict,
    op_node_dict: dict,
    executors: dict[str, ExecutorDefinition],
    catalog: DataCatalog,
    hook_manager,
    session_id: str,
    project_path: str,
    env: str,
) -> list[JobDefinition]:
    """Loads job definitions from a Kedro pipeline.

    Args:
        dagster_config : The Dagster configuration.
        asset_input_dict: A dictionary of asset inputs.
        multi_asset_node_dict: A dictionary of multi-asset nodes.
        op_node_dict: A dictionary of operation nodes.
        executors: A dictionary of executors.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.
        project_path: The path to the Kedro project.
        env: A string representing the Kedro environment to use.

    Returns:
        List[JobDefintion]: A list of dagster job definitions.

    """

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

        pipeline = pipelines.get(pipeline_name).filter(**filter_params)

        executor_config = job_config.executor
        if isinstance(executor_config, str):
            if executor_config in executors:
                executor = executors[executor_config]
            else:
                raise ValueError("")

        job = get_job_from_pipeline(
            asset_input_dict=asset_input_dict,
            multi_asset_node_dict=multi_asset_node_dict,
            op_node_dict=op_node_dict,
            pipeline=pipeline,
            catalog=catalog,
            hook_manager=hook_manager,
            job_name=job_name,
            run_params=run_params,
            executor=executor,
        )

        jobs.append(job)

    from dagster import AssetOut, multi_asset

    @multi_asset(
        name="before_pipeline_run_hook",
        group_name="hooks",
        description="Hook to be executed before a pipeline run.",
        outs={
            "before_pipeline_run_hook_result": AssetOut(
                key="before_pipeline_run_hook_result",
                description="Untangible asset for the `before_pipeline_run` hook.",
                dagster_type=Nothing,
                is_required=False,
            )
        },
    )
    def before_pipeline_run_hook():
        hook_manager.hook.before_pipeline_run(
            run_params=run_params,
            pipeline=pipeline,
            catalog=catalog,
        )

    return jobs, before_pipeline_run_hook
