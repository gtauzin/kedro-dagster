"""Dagster io manager translation from Kedro catalog."""

from logging import getLogger
from pathlib import PurePosixPath
from typing import Any

import dagster as dg
from kedro.io import MemoryDataset
from pydantic import ConfigDict

from kedro_dagster.utils import _create_pydantic_model_from_dict, _is_asset_name

LOGGER = getLogger(__name__)


class CatalogTranslator:
    """Translate Kedro datasets into Dagster IO managers."""

    def _translate_dataset(self, dataset: Any, dataset_name: str) -> dg.IOManagerDefinition:
        """Create a Dagster IO manager from a Kedro dataset.

        Args:
            dataset: The Kedro dataset to wrap into an IO manager.
            dataset_name: The name of the dataset.

        Returns:
            IOManagerDefinition: A Dagster IO manager.
        """
        # TODO: Figure out why this ConfigDict does not allow to see the config of the io managers in dagit
        dataset_config = {
            key: val if not isinstance(val, PurePosixPath) else str(val)
            for key, val in dataset._describe().items()
            if key not in ["version"]
            and val is not None  # TODO: Why are those condition necessary? We could want to edit them on launchpad
        }  # | {"dataset": dataset}

        # TODO: Make use of KedroDataCatalog.to_config() to get the config of the dataset

        DatasetModel = _create_pydantic_model_from_dict(
            dataset_config,
            __base__=dg.Config,
            __config__=ConfigDict(arbitrary_types_allowed=True),
        )

        hook_manager = self._hook_manager
        named_nodes = self._named_nodes

        class ConfiguredDatasetIOManager(DatasetModel, dg.ConfigurableIOManager):
            f"""IO Manager for kedro dataset `{dataset_name}`."""

            def handle_output(self, context: dg.OutputContext, obj):
                node_name = context.op_def.name
                node = named_nodes[node_name]
                hook_manager.hook.before_dataset_saved(
                    dataset_name=dataset_name,
                    data=obj,
                    node=node,
                )

                dataset.save(obj)

                hook_manager.hook.after_dataset_saved(
                    dataset_name=dataset_name,
                    data=obj,
                    node=node,
                )

            def load_input(self, context: dg.InputContext):
                node_name = context.op_def.name
                node = named_nodes[node_name]
                hook_manager.hook.before_dataset_loaded(
                    dataset_name=dataset_name,
                    node=node,
                )

                data = dataset.load()

                hook_manager.hook.after_dataset_loaded(
                    dataset_name=dataset_name,
                    data=data,
                    node=node,
                )

                return data

        return ConfiguredDatasetIOManager(**dataset_config)

    def translate_catalog(self) -> dict[str, dg.IOManagerDefinition]:
        """Get the IO managers from Kedro datasets.

        Returns:
            Dict[str, IOManagerDefinition]: A dictionary of DagsterIO managers.
        """
        LOGGER.info("Creating IO managers...")
        for dataset_name in self._catalog.list():
            if _is_asset_name(dataset_name):
                dataset = self._catalog._get_dataset(dataset_name)

                if isinstance(dataset, MemoryDataset):
                    continue

                self.named_resources_[f"{dataset_name}_io_manager"] = self._translate_dataset(
                    dataset,
                    dataset_name,
                )
