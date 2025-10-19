"""Translation of Kedro catalog's datasets into Dagster IO managers.
"""
from logging import getLogger
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Tuple

import dagster as dg
from kedro.io import DatasetNotFoundError, MemoryDataset
from kedro.pipeline import Pipeline
from pydantic import ConfigDict, create_model

from kedro_dagster.datasets.partitioned_dataset import DagsterPartitionedDataset
from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _is_param_name,
    format_dataset_name,
    format_node_name,
)

if TYPE_CHECKING:
    from kedro.io import AbstractDataset, CatalogProtocol
    from pluggy import PluginManager

LOGGER = getLogger(__name__)


class CatalogTranslator:
    """Translate Kedro datasets into Dagster IO managers."""

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

    def _translate_dataset(self, dataset: "AbstractDataset", dataset_name: str) -> Tuple[dg.IOManagerDefinition, Any, Any]:
        """Create a Dagster IO manager from a Kedro dataset.

        Returns:
            (IOManagerDefinition, partitions_def, partition_mappings)
        """
        asset_name = format_dataset_name(dataset_name)

        partitions_def, partition_mappings = None, None
        if isinstance(dataset, DagsterPartitionedDataset):
            partitions_def = dataset._get_partitions_definition()
            partition_mappings = dataset._get_partition_mappings()

        dataset_params = {"dataset": dataset.__class__.__name__}
        for param, value in dataset._describe().items():
            valid_value = value
            if isinstance(value, PurePosixPath):
                valid_value = str(value)
            if param == "version":
                continue
            dataset_params[param] = valid_value

        DatasetModel = _create_pydantic_model_from_dict(
            name="DatasetModel",
            params=dataset_params,
            __base__=dg.Config,
            __config__=ConfigDict(arbitrary_types_allowed=True),
        )

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
                if "downstream_partition_key" in context.op_def.tags:
                    downstream_partition_key = context.op_def.tags["downstream_partition_key"]
                    if asset_name == downstream_partition_key.split("|")[0]:
                        partition = downstream_partition_key.split("|")[1]
                elif context.has_asset_partitions:
                    partition = context.asset_partition_key

                if partition is not None:
                    obj = {partition: obj}

                dataset.save(obj)

                if is_node_op:
                    context.log.info("Executing `after_dataset_saved` Kedro hook.")
                    hook_manager.hook.after_dataset_saved(
                        dataset_name=dataset_name,
                        data=obj,
                        node=node,
                    )

            def load_input(self, context: dg.InputContext) -> Any:  # type: ignore[no-untyped-def]
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
                if "upstream_partition_key" in context.op_def.tags:
                    upstream_partition_key = context.op_def.tags["upstream_partition_key"]
                    if asset_name == upstream_partition_key.split("|")[0]:
                        partition = upstream_partition_key.split("|")[1]
                elif context.has_asset_partitions:
                    partition = context.asset_partition_key

                if partition is not None and isinstance(data, dict):
                    val = data.get(partition)
                    if callable(val):
                        data = val()
                    else:
                        data = val

                if is_node_op:
                    context.log.info("Executing `after_dataset_loaded` Kedro hook.")
                    node = named_nodes[node_name]
                    hook_manager.hook.after_dataset_loaded(
                        dataset_name=dataset_name,
                        data=data,
                        node=node,
                    )

                return data

        ConfigurableDatasetIOManager = create_model(dataset_params["dataset"], __base__=ConfigurableDatasetIOManager)
        ConfigurableDatasetIOManager.__doc__ = f"""IO Manager for Kedro dataset `{dataset_name}`."""

        return ConfigurableDatasetIOManager(**dataset_params), partitions_def, partition_mappings

    def to_dagster(self) -> tuple[dict[str, dg.IOManagerDefinition], dict[str, dict[str, Any]]]:
        """Generate IO managers and partition metadata for all Kedro datasets involved in the pipelines."""
        named_io_managers: dict[str, dg.IOManagerDefinition] = {}
        asset_partitions: dict[str, dict[str, Any]] = {}

        for dataset_name in sum(self._pipelines, start=Pipeline([])).datasets():
            if _is_param_name(dataset_name):
                continue

            asset_name = format_dataset_name(dataset_name)
            try:
                dataset = self._catalog._get_dataset(dataset_name)
            except DatasetNotFoundError:
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
