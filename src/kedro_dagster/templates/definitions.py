"""Dagster definitions."""

import os

import dagster as dg

from kedro_dagster import KedroProjectTranslator

KEDRO_ENV = os.getenv("KEDRO_ENV", "dev")

translator = KedroProjectTranslator(env=KEDRO_ENV)
dagster_code_location = translator.to_dagster()

resources = dagster_code_location.named_resources
# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
resources |= {
    "io_manager": dg.fs_io_manager,
}

defs = dg.Definitions(
    assets=list(dagster_code_location.named_assets.values()),
    resources=resources,
    jobs=list(dagster_code_location.named_jobs.values()),
    schedules=list(dagster_code_location.named_schedules.values()),
    sensors=list(dagster_code_location.named_sensors.values()),
    loggers=dagster_code_location.named_loggers,
    executor=dg.multiprocess_executor.configured(dict(max_concurrent=2)),
)
