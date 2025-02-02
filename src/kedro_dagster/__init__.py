"""Kedro plugin for running a project with Dagster."""

__version__ = "0.0.1"

from .translator import KedroDagsterTranslator

__all__ = [
    "KedroDagsterTranslator",
]
