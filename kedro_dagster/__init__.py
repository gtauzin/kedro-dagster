"""Kedro plugin for running a project with Dagster."""

__version__ = "0.0.1"

from .assets import load_assets_from_kedro_nodes
from .resources import load_io_managers_from_kedro_datasets