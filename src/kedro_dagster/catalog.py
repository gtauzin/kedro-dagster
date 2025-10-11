"""Translation of Kedro catalog's datasets into Dagster IO manager."""

from logging import getLogger
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.io import DatasetNotFoundError, MemoryDataset
from kedro.pipeline import Pipeline
from kedro_dagster.datasets.partitioned_dataset import DagsterPartitionedDataset
from pydantic import ConfigDict, create_model

from kedro_dagster.utils import _create_pydantic_model_from_dict, _is_param_name, format_dataset_name, format_node_name

if TYPE_CHECKING:
    from kedro.io import AbstractDataset, CatalogProtocol
    from pluggy import PluginManager

LOGGER = getLogger(__name__)


class CatalogTranslator:
    """Translate Kedro datasets into Dagster IO managers.

    Args:
        catalog (CatalogProtocol): The Kedro catalog.
        pipelines (list[Pipeline]): List of Kedro pipelines to translate.
        hook_manager (PluginManager): The hook manager to call Kedro hooks.
        env (str): The Kedro environment.

    """

    def __init__(
        self, catalog: "CatalogProtocol", pipelines: list["Pipeline"], hook_manager: "PluginManager", env: str
    ):
        self._catalog = catalog
        self._pipelines = pipelines
        self._hook_manager = hook_manager
        self._env = env

    def _translate_dataset(self, dataset: "AbstractDataset", dataset_name: str) -> dg.IOManagerDefinition:
        """Create a Dagster IO manager from a Kedro dataset.

        Args:
            dataset (AbstractDataset): The Kedro dataset to wrap into an IO manager.
            dataset_name (str): The name of the dataset.

        Returns:
            IOManagerDefinition: A Dagster IO manager.
        """
        partitions_def, partition_mappings = None, None
        if isinstance(dataset, DagsterPartitionedDataset):
            partitions_def = dataset._get_partitions_definition()
            partition_mappings = dataset._get_partition_mappings()

        dataset_params = {"dataset": dataset.__class__.__name__}
        for param, value in dataset._describe().items():
            valid_value = value
            if isinstance(value, PurePosixPath):
                valid_value = str(value)

            # Version is not serializable
            if param == "version":
                continue

            dataset_params[param] = valid_value

        if dataset_params["dataset"].endswith("DagsterPartitionedDataset"):
            # We need to pass the partitions and depends_on as dict
                dataset_params["partition_key"] = None

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
                # When defining the op, we have named them either with
                # a trailing "_graph"
                node_name = context.op_def.name
                # Hooks called only if op is not an asset
                is_node_multi_asset = node_name in named_nodes
                if is_node_multi_asset:
                    context.log.info("Executing `before_dataset_saved` Kedro hook.")

                    node = named_nodes[node_name]
                    hook_manager.hook.before_dataset_saved(
                        dataset_name=dataset_name,
                        data=obj,
                        node=node,
                    )

                partition = None
                if context.has_asset_partitions:
                    partition = context.asset_partition_key
                elif "partition_key" in context.resource_config:
                    partition = context.resource_config["partition_key"]

                if partition is not None:
                    obj = {partition: obj}

                print("SAVING DATASET", dataset_name)
                print("CONTEXT HAS ASSET PARTITIONS", context.has_asset_partitions)
                print("CONTEXT HAS PARTITION KEY", context.has_partition_key)
                try:
                    print("CONTEXT PARTITION KEY", context.partition_key)
                except:
                    pass

                try:
                    print("CONTEXT ASSET PARTITION KEY", context.asset_partition_key)
                except:
                    pass

                try:
                    print("CONTEXT ASSET PARTITION KEYS", context.asset_partition_keys)
                except:
                    pass

                print("RESOURCE CONFIG", context.resource_config)

                dataset.save(obj)

                if is_node_multi_asset:
                    context.log.info("Executing `after_dataset_saved` Kedro hook.")

                    hook_manager.hook.after_dataset_saved(
                        dataset_name=dataset_name,
                        data=obj,
                        node=node,
                    )

            def load_input(self, context: dg.InputContext) -> Any:
                node_name = context.op_def.name
                # When defining the op, we have named them either with
                # a trailing "_graph"
                # Hooks called only if op is not an asset
                is_node_multi_asset = node_name in named_nodes
                if is_node_multi_asset:
                    context.log.info("Executing `before_dataset_loaded` Kedro hook.")

                    node = named_nodes[node_name]
                    hook_manager.hook.before_dataset_loaded(
                        dataset_name=dataset_name,
                        node=node,
                    )

                # TODO: Make sure it works with multi partitions
                print("LOADING DATASET", dataset_name)
                print("CONTEXT HAS ASSET PARTITIONS", context.has_asset_partitions)
                print("CONTEXT HAS PARTITION KEY", context.has_partition_key)
                try:

                    print("CONTEXT PARTITION KEY", context.partition_key)
                except:
                    pass

                try:
                    print("CONTEXT ASSET PARTITION KEYS", context.asset_partition_key)
                except:
                    pass

                try:
                    print("CONTEXT ASSET PARTITION KEYS", context.asset_partition_keys)
                except:
                    pass

                try:
                    print("RESOURCE CONFIG", context.resource_config)
                except:
                    pass

                data = dataset.load()

                partition = None
                if context.has_asset_partitions:
                    partition = context.asset_partition_key
                elif "partition_key" in context.resource_config:
                    partition = context.resource_config["partition_key"]

                if partition is not None:
                    data = data[partition]()

                if is_node_multi_asset:
                    context.log.info("Executing `after_dataset_loaded` Kedro hook.")

                    hook_manager.hook.after_dataset_loaded(
                        dataset_name=dataset_name,
                        data=data,
                        node=node,
                    )

                return data

        # Trick to modify the name of the IO Manager in Dagster UI
        ConfigurableDatasetIOManager = create_model(dataset_params["dataset"], __base__=ConfigurableDatasetIOManager)

        # Modify the description of the IO Manager in Dagster UI
        ConfigurableDatasetIOManager.__doc__ = f"""IO Manager for Kedro dataset `{dataset_name}`."""

        return ConfigurableDatasetIOManager(**dataset_params), partitions_def, partition_mappings

    def to_dagster(self) -> dict[str, dg.IOManagerDefinition]:
        """Get the IO managers from Kedro datasets.

        Returns:
            Dict[str, IOManagerDefinition]: A dictionary of DagsterIO managers.
        """
        named_io_managers, asset_partitions = {}, {}
        for dataset_name in sum(self._pipelines, start=Pipeline([])).datasets():
            if not _is_param_name(dataset_name):
                asset_name = format_dataset_name(dataset_name)

                try:
                    dataset = self._catalog._get_dataset(dataset_name)

                except DatasetNotFoundError:
                    LOGGER.debug(
                        f"Dataset `{dataset_name}` not in catalog. It will be "
                        "handled by default IO manager `io_manager`."
                    )
                    continue

                if isinstance(dataset, MemoryDataset):
                    continue

                io_manager, partitions_def, partition_mappings = self._translate_dataset(
                    dataset,
                    dataset_name,
                )
                named_io_managers[f"{self._env}__{asset_name}_io_manager"] = io_manager
                if partitions_def is not None:
                    asset_partitions[asset_name] = {
                        "partitions_def": partitions_def,
                        "partition_mappings": partition_mappings,
                    }

        return named_io_managers, asset_partitions
