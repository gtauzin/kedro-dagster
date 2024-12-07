"""Dagster definitions."""

from dagster import Definitions, fs_io_manager
from kedro_dagster import (
    translate_kedro,
)

kedro_assets, kedro_resources, kedro_jobs, kedro_loggers = translate_kedro()


# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
kedro_resources |= {
    "io_manager": fs_io_manager,
}

defs = Definitions(
    assets=kedro_assets,
    resources=kedro_resources,
    jobs=kedro_jobs,
    loggers=kedro_loggers,
)
