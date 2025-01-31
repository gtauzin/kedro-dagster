"""Dagster job definitons from Kedro pipelines."""

from typing import Any

import dagster as dg
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.io import KedroDataCatalog
from kedro.pipeline import Pipeline

from kedro_dagster.config import KedroDagsterConfig


def get_job_from_pipeline(
    multi_asset_node_dict: dict[str, Any],
    op_node_dict: dict[str, Any],
    pipeline: Pipeline,
    catalog: KedroDataCatalog,
    job_name: str,
    hook_manager,
    run_params: dict[str, Any],
    executor: dg.ExecutorDefinition,
    io_managers,
) -> dg.JobDefinition:
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

    from kedro_dagster.resources import load_pipeline_hook_translation

    pipeline_hook_resource, _ = load_pipeline_hook_translation(
        run_params=run_params,
        # pipeline=pipeline,
        catalog=catalog,
        hook_manager=hook_manager,
    )

    @dg.graph(
        name=job_name,
        description=f"Graph derived from pipeline associated to the `{job_name}` job.",
        out=None,
    )
    def pipeline_graph():
        materialized_assets = {}
        for external_asset_name in pipeline.inputs():
            if not external_asset_name.startswith("params:"):
                dataset = catalog._get_dataset(external_asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")
                materialized_assets[external_asset_name] = dg.AssetSpec(
                    external_asset_name,
                    description=description,
                    metadata=metadata,
                ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")

        for layer in pipeline.grouped_nodes:
            for node in layer:
                if len(node.outputs):
                    op = multi_asset_node_dict[node.name]
                else:
                    op = op_node_dict[node.name]

                node_inputs = node.inputs

                materialized_input_assets = {
                    input_name: materialized_assets[input_name]
                    for input_name in node_inputs
                    if input_name in materialized_assets
                }

                materialized_outputs = op(**materialized_input_assets)

                if len(node.outputs) <= 1:
                    materialized_output_assets = {materialized_outputs.output_name: materialized_outputs}
                elif len(node.outputs) > 1:
                    materialized_output_assets = {
                        materialized_output.output_name: materialized_output
                        for materialized_output in materialized_outputs
                    }

                materialized_assets |= materialized_output_assets

    job = pipeline_graph.to_job(
        name=job_name,
        resource_defs=io_managers | {"pipeline_hook": pipeline_hook_resource},
        executor_def=executor,
    )

    return job


def load_jobs_from_kedro_config(
    dagster_config: KedroDagsterConfig,
    multi_asset_node_dict: dict,
    op_node_dict: dict,
    executors: dict[str, dg.ExecutorDefinition],
    catalog: KedroDataCatalog,
    hook_manager,
    session_id: str,
    project_path: str,
    env: str,
    io_managers,
) -> list[dg.JobDefinition]:
    """Loads job definitions from a Kedro pipeline.

    Args:
        dagster_config : The Dagster configuration.
        multi_asset_node_dict: A dictionary of multi-asset nodes.
        op_node_dict: A dictionary of operation nodes.
        executors: A dictionary of executors.
        catalog: An implemented instance of ``CatalogProtocol``
        from which to fetch data.
        hook_manager: The ``PluginManager`` to activate hooks.
        session_id: A string representing Kedro session ID.
        project_path: The path to the Kedro project.
        env: A string representing the Kedro environment to use.
        io_managers

    Returns:
        List[JobDefintion]: A list of dagster job definitions.

    """

    job_dict = {}
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
            project_path=str(project_path),
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
            multi_asset_node_dict=multi_asset_node_dict,
            op_node_dict=op_node_dict,
            pipeline=pipeline,
            catalog=catalog,
            hook_manager=hook_manager,
            job_name=job_name,
            run_params=run_params,
            executor=executor,
            io_managers=io_managers,
        )

        job_dict[job_name] = job

    return job_dict
