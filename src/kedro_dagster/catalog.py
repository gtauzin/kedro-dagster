"""Translation of Kedro catalog's datasets into Dagster IO manager."""

from logging import getLogger
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

import dagster as dg
from kedro.framework.project import pipelines
from kedro.io import DatasetNotFoundError, MemoryDataset
from pydantic import ConfigDict, create_model

from kedro_dagster.utils import _create_pydantic_model_from_dict, _is_asset_name, dagster_format

if TYPE_CHECKING:
    from kedro.io import CatalogProtocol
    from pluggy import PluginManager


LOGGER = getLogger(__name__)


class CatalogTranslator:
    """Translate Kedro datasets into Dagster IO managers.

    Args:
        catalog: The Kedro catalog.
        hook_manager: The hook manager to call Kedro hooks.

    """

    def __init__(self, catalog: "CatalogProtocol", hook_manager: "PluginManager", env: str):
        self._catalog = catalog
        self._hook_manager = hook_manager
        self._env = env

    def _translate_dataset(self, dataset: Any, dataset_name: str) -> dg.IOManagerDefinition:
        """Create a Dagster IO manager from a Kedro dataset.

        Args:
            dataset: The Kedro dataset to wrap into an IO manager.
            dataset_name: The name of the dataset.

        Returns:
            IOManagerDefinition: A Dagster IO manager.
        """
        dataset_params = {"dataset": dataset.__class__.__name__}
        for param, value in dataset._describe().items():
            valid_value = value
            if isinstance(value, PurePosixPath):
                valid_value = str(value)

            # Version is not serializable
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
        named_nodes = {dagster_format(node.name): node for node in sum(pipelines.values()).nodes}

        # TODO: Check if hooks are indeed called for ops and not assets
        # TODO: Remove _graph and add _asset
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

                data = dataset.load()

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

        return ConfigurableDatasetIOManager(**dataset_params)

    def to_dagster(self) -> dict[str, dg.IOManagerDefinition]:
        """Get the IO managers from Kedro datasets.

        Returns:
            Dict[str, IOManagerDefinition]: A dictionary of DagsterIO managers.
        """
        named_io_managers = {}
        for dataset_name in sum(pipelines.values()).datasets():
            asset_name = dagster_format(dataset_name)
            if _is_asset_name(asset_name):
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

                named_io_managers[f"{self._env}__{asset_name}_io_manager"] = self._translate_dataset(
                    dataset,
                    dataset_name,
                )

        return named_io_managers
