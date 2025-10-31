"""Translate Kedro catalog datasets into Dagster IO managers.

This module inspects the Kedro catalog to create Dagster IO managers capable of
loading/saving datasets, while invoking Kedro dataset hooks. It also extracts
partitioning information for partitioned datasets.
"""

from logging import getLogger
from os import PathLike
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict, create_model

from kedro_dagster.datasets.partitioned_dataset import DagsterPartitionedDataset
from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _is_param_name,
    format_dataset_name,
    format_node_name,
    get_dataset_from_catalog,
    is_nothing_asset_name,
)

if TYPE_CHECKING:
    from kedro.io import AbstractDataset, CatalogProtocol
    from pluggy import PluginManager

LOGGER = getLogger(__name__)


class CatalogTranslator:
    """Translate Kedro datasets into Dagster IO managers.

    Args:
        catalog (CatalogProtocol): Kedro catalog.
        pipelines (list[Pipeline]): Kedro pipelines to consider when building IO managers.
        hook_manager (PluginManager): Kedro hook manager used to invoke dataset hooks.
        env (str): Kedro environment (used to namespace resource keys).
    """

    def __init__(
        self,
        catalog: "CatalogProtocol",
        pipelines: list["Pipeline"],
        hook_manager: "PluginManager",
        env: str,
    ):
        self._catalog = catalog
        self._pipelines = pipelines
        self._hook_manager = hook_manager
        self._env = env

    def _create_dataset_config(self, dataset: "AbstractDataset") -> Any:
        """Create and return a Pydantic model class capturing dataset configuration.

        The returned class extends Dagster's `Config` and contains fields for:
        - dataset: the dataset class short name (e.g., CSVDataset)
        - fields from `dataset._describe()` (excluding "version"), converting
          `PurePosixPath` values to strings for serialization.
        """
        params: dict[str, Any] = {"dataset": dataset.__class__.__name__}
        for param, value in dataset._describe().items():
            if param == "version":
                continue
            # Convert any path-like values to strings (preserve original separators).
            if isinstance(value, PurePosixPath):
                params[param] = str(value)
            elif isinstance(value, PathLike):
                params[param] = str(value)
            else:
                params[param] = value

        DatasetModel = _create_pydantic_model_from_dict(
            name="DatasetModel",
            params=params,
            __base__=dg.Config,
            __config__=ConfigDict(arbitrary_types_allowed=True),
        )
        return DatasetModel

    def _translate_dataset(
        self, dataset: "AbstractDataset", dataset_name: str
    ) -> tuple[dg.IOManagerDefinition, Any, Any]:
        """Create a configurable IO manager class for a single Kedro dataset.

        Args:
            dataset (AbstractDataset): Kedro dataset to wrap into an IO manager.
            dataset_name (str): Name of the dataset in the Kedro catalog.

        Returns:
            tuple[IOManagerDefinition, Any, Any]: 3-tuple of (IO manager instance,
            partitions_def, partition_mappings).
        """

        asset_name = format_dataset_name(dataset_name)

        partitions_def, partition_mappings = None, None
        if isinstance(dataset, DagsterPartitionedDataset):
            partitions_def = dataset._get_partitions_definition()
            partition_mappings = dataset._get_partition_mappings()

        DatasetModel = self._create_dataset_config(dataset)

        hook_manager = self._hook_manager
        named_nodes = {format_node_name(node.name): node for node in sum(self._pipelines, start=Pipeline([])).nodes}

        class ConfigurableDatasetIOManager(DatasetModel, dg.ConfigurableIOManager):  # type: ignore[valid-type]
            def handle_output(self, context: dg.OutputContext, obj) -> None:  # type: ignore[no-untyped-def]
                node_name = context.op_def.name
                is_node_op = node_name in named_nodes

                if is_node_op:
                    context.log.info("Executing `before_dataset_saved` Kedro hook.")
                    node = named_nodes[node_name]
                    hook_manager.hook.before_dataset_saved(
                        dataset_name=dataset_name,
                        data=obj,
                        node=node,
                    )

                partition = None
                # Partition key passed via op tags for fanned-out ops
                if "downstream_partition_key" in context.op_def.tags:
                    downstream_partition_key = context.op_def.tags["downstream_partition_key"]
                    if asset_name == downstream_partition_key.split("|")[0]:
                        partition = downstream_partition_key.split("|")[1]
                # Partition key passed via context for asset jobs
                # Prefer Dagster's asset partition when available, otherwise fall back to plain partition_key
                elif getattr(context, "has_asset_partitions", False):
                    partition = context.asset_partition_key
                elif getattr(context, "has_partition_key", False):
                    partition = context.partition_key

                if partition is not None and isinstance(obj, dict) and set(obj.keys()) != {partition}:
                    raise ValueError(
                        f"Expected data for downstream partition to be a dict with key '{partition}' but got: {obj}"
                    )

                dataset.save(obj)

                if is_node_op:
                    context.log.info("Executing `after_dataset_saved` Kedro hook.")
                    hook_manager.hook.after_dataset_saved(
                        dataset_name=dataset_name,
                        data=obj,
                        node=node,
                    )

            def load_input(self, context: dg.InputContext) -> Any:
                node_name = context.op_def.name
                is_node_op = node_name in named_nodes

                if is_node_op:
                    context.log.info("Executing `before_dataset_loaded` Kedro hook.")
                    node = named_nodes[node_name]
                    hook_manager.hook.before_dataset_loaded(
                        dataset_name=dataset_name,
                        node=node,
                    )

                data = dataset.load()

                partition = None
                # Partition key passed via op tags for fanned-out ops
                if "upstream_partition_key" in context.op_def.tags:
                    upstream_partition_key = context.op_def.tags["upstream_partition_key"]
                    if asset_name == upstream_partition_key.split("|")[0]:
                        partition = upstream_partition_key.split("|")[1]
                # Partition key passed via context for asset jobs
                # Prefer Dagster's asset partition when available, otherwise fall back to plain partition_key
                elif getattr(context, "has_asset_partitions", False):
                    partition = context.asset_partition_key
                elif getattr(context, "has_partition_key", False):
                    partition = context.partition_key

                if partition is not None and isinstance(data, dict):
                    data = {partition: data.get(partition)}

                if is_node_op:
                    context.log.info("Executing `after_dataset_loaded` Kedro hook.")
                    node = named_nodes[node_name]
                    hook_manager.hook.after_dataset_loaded(
                        dataset_name=dataset_name,
                        data=data,
                        node=node,
                    )

                return data

        # Build a named IO manager class for this particular dataset type
        dataset_type_short = dataset.__class__.__name__
        ConfigurableDatasetIOManagerClass = create_model(dataset_type_short, __base__=ConfigurableDatasetIOManager)
        ConfigurableDatasetIOManagerClass.__doc__ = f"IO Manager for Kedro dataset `{dataset_name}`."

        # Instantiate without args; defaults are embedded in the DatasetModel
        io_manager_instance = ConfigurableDatasetIOManagerClass()

        # Normalize absolute filepaths to native separators only at IO manager instance level
        # to satisfy round-trip tests on Windows while preserving raw config values.
        # try:
        filepath = getattr(io_manager_instance, "filepath", None)
        if isinstance(filepath, str):
            casted_filepath = Path(filepath)
            if casted_filepath.is_absolute():
                normalized_filepath = str(casted_filepath)
                if normalized_filepath != filepath:
                    try:
                        io_manager_instance.filepath = normalized_filepath
                    except Exception:
                        object.__setattr__(io_manager_instance, "filepath", normalized_filepath)
        # except Exception:
        #     pass

        return io_manager_instance, partitions_def, partition_mappings

    def to_dagster(self) -> tuple[dict[str, dg.IOManagerDefinition], dict[str, dict[str, Any]]]:
        """Generate IO managers and partitions for all Kedro datasets referenced by pipelines."""
        named_io_managers: dict[str, dg.IOManagerDefinition] = {}
        asset_partitions: dict[str, dict[str, Any]] = {}

        for dataset_name in sum(self._pipelines, start=Pipeline([])).datasets():
            if _is_param_name(dataset_name) or is_nothing_asset_name(self._catalog, dataset_name):
                continue

            asset_name = format_dataset_name(dataset_name)
            dataset = get_dataset_from_catalog(self._catalog, dataset_name)
            if dataset is None:
                LOGGER.debug(
                    f"Dataset `{dataset_name}` not in catalog. It will be handled by default IO manager `io_manager`."
                )
                continue

            if isinstance(dataset, MemoryDataset):
                continue

            io_manager, partitions_def, partition_mappings = self._translate_dataset(dataset, dataset_name)
            named_io_managers[f"{self._env}__{asset_name}_io_manager"] = io_manager

            if partitions_def is not None:
                asset_partitions[asset_name] = {
                    "partitions_def": partitions_def,
                    "partition_mappings": partition_mappings,
                }

        return named_io_managers, asset_partitions
