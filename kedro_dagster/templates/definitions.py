"""Dagster definitions."""

from dagster import Definitions, fs_io_manager

from kedro_dagster import (
    translate_kedro,
)

kedro_assets, kedro_io_managers, kedro_jobs = translate_kedro()

# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
kedro_io_managers |= {
    "io_manager": fs_io_manager,
}

defs = Definitions(
    assets=kedro_assets,
    resources=kedro_io_managers,
    jobs=kedro_jobs,
)
