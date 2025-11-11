"""Dagster definitions."""

import dagster as dg

from kedro_dagster import KedroProjectTranslator

translator = KedroProjectTranslator(env="local")
dagster_code_location = translator.to_dagster()

resources = dagster_code_location.named_resources
# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
resources |= {
    "io_manager": dg.fs_io_manager,
}

# Define the default executor for Dagster jobs
default_executor = dg.multiprocess_executor.configured(dict(max_concurrent=2))

# Define default loggers for Dagster jobs
default_loggers = {
    "console": dg.colored_console_logger,
}
# They could also come from the Kedro-Dagster config
# default_loggers = {
#   "default": dagster_code_location.named_loggers["default_logger"]
# }

defs = dg.Definitions(
    assets=list(dagster_code_location.named_assets.values()),
    resources=resources,
    jobs=list(dagster_code_location.named_jobs.values()),
    schedules=list(dagster_code_location.named_schedules.values()),
    sensors=list(dagster_code_location.named_sensors.values()),
    loggers=default_loggers,
    executor=default_executor,
)
