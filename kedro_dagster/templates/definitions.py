"""Dagster definitions."""

from dagster import Definitions, InMemoryIOManager

from kedro_dagster import (
    load_assets_from_kedro_nodes,
    load_io_managers_from_kedro_datasets,
)

kedro_assets = load_assets_from_kedro_nodes()
kedro_io_managers = load_io_managers_from_kedro_datasets()

# The "io_manager" key handles how Kedro MemoryDatasets are handled by Dagster
kedro_io_managers |= {
    "io_manager": InMemoryIOManager()
}

defs = Definitions(
    assets=kedro_assets,
    resources=kedro_io_managers
)
