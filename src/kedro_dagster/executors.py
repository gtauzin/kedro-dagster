"""Dagster executor definitons from Kedro pipelines."""

from dagster import ExecutorDefinition

from kedro_dagster.config import KedroDagsterConfig
from kedro_dagster.config.execution import InProcessExecutorOptions, K8sJobExecutorOptions, MultiprocessExecutorOptions


# TODO: Map more executors
# TODO: Allow for custom executors?
def load_executors_from_kedro_config(
    dagster_config: KedroDagsterConfig,
) -> dict[str, ExecutorDefinition]:
    """Loads executor definitions from the config file.

    Args:
        dagster_config : The Dagster configuration.

    Returns:
        Dict[Str, ExecutorDefinition]: A dict of dagster executor definitions.

    """

    executors = {}
    for executor_name, executor_config in dagster_config.executors.items():
        if isinstance(executor_config, InProcessExecutorOptions):
            from dagster import in_process_executor

            executor = in_process_executor

        elif isinstance(executor_config, MultiprocessExecutorOptions):
            from dagster import multiprocess_executor

            executor = multiprocess_executor

        elif isinstance(executor_config, K8sJobExecutorOptions):
            from dagster_k8s import k8s_job_executor

            executor = k8s_job_executor

        executors[executor_name] = executor.configured(executor_config.model_dump())

    return executors
