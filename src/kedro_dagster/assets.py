"""Dagster asset definitons from Kedro nodes."""

from logging import getLogger

import dagster as dg
from kedro.framework.project import pipelines
from kedro.io import MemoryDataset
from kedro.pipeline.node import Node
from pydantic import ConfigDict

from kedro_dagster.utils import (
    _create_pydantic_model_from_dict,
    _get_node_pipeline_name,
    _is_asset_name,
)

LOGGER = getLogger(__name__)


class AssetsCreator:
    """Create Dagster assets from Kedro nodes."""

    def _create_asset(self, node: Node):
        """Create a Dagster asset from a Kedro node.

        Args:
            node: The Kedro node to wrap into an asset.

        Returns:
            AssetDefinition: A Dagster asset.
        """
        node_pipeline_name = _get_node_pipeline_name(pipelines, node)

        ins, params = {}, {}
        for asset_name in node.inputs:
            if not asset_name.startswith("params:"):
                ins[asset_name] = dg.AssetIn(
                    key=asset_name,
                    # input_manager_key=f"{asset_name}_io_manager",
                )
            else:
                params[asset_name] = self._context.catalog.load(asset_name)

        outs = {}
        for asset_name in node.outputs:
            metadata, description = None, None
            if asset_name in self._context.catalog.list():
                dataset = self._context.catalog._get_dataset(asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")

            io_manager_key = "io_manager"
            if asset_name in self._context.catalog.list() and not isinstance(
                self._context.catalog._get_dataset(asset_name), MemoryDataset
            ):
                io_manager_key = f"{asset_name}_io_manager"

            outs[asset_name] = dg.AssetOut(
                key=asset_name,
                description=description,
                metadata=metadata,
                io_manager_key=io_manager_key,
            )

        # Node parameters are mapped to Dagster configs
        NodeParametersConfig = _create_pydantic_model_from_dict(
            params,
            __base__=dg.Config,
            __config__=ConfigDict(extra="allow", frozen=False),
        )

        # Define a multi_asset from a Kedro node
        @dg.multi_asset(
            name=node.name,
            description=f"Kedro node {node.name} wrapped as a Dagster multi asset.",
            group_name=node_pipeline_name,
            ins=ins,
            outs=outs,
            required_resource_keys={"pipeline_hook"},
            op_tags=node.tags,  # T)DO: Does this work?
        )
        def node_asset(
            context,
            config: NodeParametersConfig,
            **inputs,
        ):
            # TODO: Passing config does not work
            outputs = self._named_nodes[node.name](
                **inputs,
            )

            for output_asset_name, output_asset in outputs.items():
                context.resources.pipeline_hook.add_run_results(output_asset_name, output_asset)

            if len(outputs) > 1:
                return tuple(outputs.values())

            elif len(outputs) == 1:
                return list(outputs.values())[0]

        return node_asset

    def create_assets(self):
        LOGGER.info("Building asset list...")
        default_pipeline = pipelines.get("__default__")

        # Assets that are not generated through dagster are external and
        # registered with AssetSpec
        for external_asset_name in default_pipeline.inputs():
            if _is_asset_name(external_asset_name):
                dataset = self._context.catalog._get_dataset(external_asset_name)
                metadata = getattr(dataset, "metadata", None) or {}
                description = metadata.pop("description", "")
                asset = dg.AssetSpec(
                    external_asset_name,
                    group_name="external",
                    description=description,
                    metadata=metadata,
                ).with_io_manager_key(io_manager_key=f"{external_asset_name}_io_manager")
                self.named_assets_[external_asset_name] = asset

        # Create assets from Kedro nodes that have outputs
        for node in default_pipeline.nodes:
            if len(node.outputs):
                asset = self._create_asset(node)
                self.named_assets_[node.name] = asset
