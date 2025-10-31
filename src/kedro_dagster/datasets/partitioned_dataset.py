import copy
import operator
import os
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import dagster as dg
from cachetools import cachedmethod
from kedro.io.core import _load_obj
from kedro_datasets.partitions import PartitionedDataset

TYPE_KEY = "type"
_DEFAULT_PACKAGES = ["dagster.", ""]


def parse_dagster_definition(
    config: dict[str, Any],
) -> tuple[type[Any], dict[str, Any]]:
    """Parse and instantiate a partition definition class using a config.

    Args:
        config (dict[str, Any]): Partition definition config. Must contain a `type` key with a
            fully-qualified class name or a class object.

    Returns:
        tuple[type[PartitionsDefinition], dict[str, Any]]: Class object and remaining config.
    """
    config = copy.deepcopy(config)
    definition_type = config.pop(TYPE_KEY)

    class_obj: type[Any] | None = None
    error_msg = None
    if isinstance(definition_type, str):
        if len(definition_type.strip(".")) != len(definition_type):
            raise TypeError("'type' class path does not support relative paths or paths ending with a dot.")
        class_paths = (prefix + definition_type for prefix in _DEFAULT_PACKAGES)

        # Try to resolve the class by attempting a few import paths; record the last error message if all fail
        for class_path in class_paths:
            try:
                tmp, error_msg = _load_obj(class_path)  # Try to load partition class
            except TypeError:  # noqa: BLE001
                try:
                    tmp = _load_obj(class_path)
                except Exception as exc:
                    error_msg = str(exc)
                    raise TypeError(f"Error loading class '{class_path}': {error_msg}")

            if tmp is not None:
                class_obj = tmp
                break

        if class_obj is None:  # If no valid class was found, raise an error
            default_error_msg = f"Class '{definition_type}' not found, is this a typo?"
            raise TypeError(f"{error_msg if error_msg else default_error_msg}")

    if class_obj is None:
        class_obj = definition_type

    return class_obj, config


class DagsterPartitionedDataset(PartitionedDataset):
    """A Kedro dataset that integrates with Dagster's partitioning system.

    Args:
        path (str): Base path for partitions.
        dataset (dict[str, Any]): Underlying dataset config.
        partition (dict[str, Any]): Partition definition config or type.
        partition_mapping (dict[str, Any] | None): Optional downstream partition mappings.
        filepath_arg (str): Arg name in underlying dataset to pass the resolved path.
        filename_suffix (str): Optional suffix appended to partitioned filename.
        credentials (dict[str, Any] | None): Credentials for the underlying dataset.
        load_args (dict[str, Any] | None): Load args for the underlying dataset.
        fs_args (dict[str, Any] | None): Filesystem args for the underlying dataset.
        overwrite (bool): Whether to overwrite on save.
        save_lazily (bool): Whether to save lazily.
        metadata (dict[str, Any] | None): Arbitrary metadata.

    Examples:
        ```python
        from kedro_dagster.datasets import DagsterPartitionedDataset

        partitioned_dataset = DagsterPartitionedDataset(
            path="data/partitions/",
            dataset=CSVDataSet(
                filepath_arg="filepath",
                sep=",",
            ),
            partition=StaticPartitionsDefinition(["2023-01-01", "2023-01-02"]),
            partition_mapping={"downstream_dataset": TimeWindowPartitionMapping()},
            filename_suffix=".csv",
        )
        ```
    """

    def __init__(
        self,
        *,
        path: str,
        dataset: dict[str, Any],
        partition: dict[str, Any],
        partition_mapping: dict[str, Any] | None = None,
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        overwrite: bool = False,
        save_lazily: bool = True,
        metadata: dict[str, Any] | None = None,
    ):
        super().__init__(
            path=path,
            dataset=dataset,
            filepath_arg=filepath_arg,
            filename_suffix=filename_suffix,
            credentials=credentials,
            load_args=load_args,
            fs_args=fs_args,
            overwrite=overwrite,
            save_lazily=save_lazily,
            metadata=metadata,
        )

        partition = partition if isinstance(partition, dict) else {"type": partition}
        self._validate_partitions_definition(partition)
        self._partition_type, self._partition_config = parse_dagster_definition(partition)

        self._partition_mapping: dict[str, Any] | None = None
        if partition_mapping is not None:
            self._partition_mapping = {}
            for downstream_dataset_name, downstream_partition_mapping in partition_mapping.items():
                downstream_mapping_cfg = (
                    downstream_partition_mapping
                    if isinstance(downstream_partition_mapping, dict)
                    else {"type": downstream_partition_mapping}
                )
                downstream_partition_mapping_type, downstream_partition_mapping_config = parse_dagster_definition(
                    downstream_mapping_cfg
                )
                self._partition_mapping[downstream_dataset_name] = {
                    "type": downstream_partition_mapping_type,
                    "config": downstream_partition_mapping_config,
                }

    def _validate_partitions_definition(self, partition: dict[str, Any]) -> None:
        """Validate minimal structure for partition definition.

        Args:
            partition (dict[str, Any]): Partition definition config.
        """
        if "type" not in partition:
            raise ValueError("Partition definition must contain the 'type' key.")

    def _get_partitions_definition(self) -> dg.PartitionsDefinition:
        """Instantiate and return the Dagster partitions definition.

        Returns:
            PartitionsDefinition: Instantiated partitions definition.
        """
        try:
            partition_def = self._partition_type(**self._partition_config)
        except Exception as exc:
            raise ValueError(
                f"Failed to instantiate partitions definition "
                f"'{self._partition_type.__name__}' with config: {self._partition_config}"
            ) from exc

        return partition_def

    def _get_mapped_downstream_dataset_names(self) -> list[str]:
        """Return downstream dataset names that have a partition mapping.

        Returns:
            list[str]: Mapped downstream dataset names.
        """
        if self._partition_mapping is None:
            return []

        return list(self._partition_mapping.keys())

    def _get_partition_mappings(self) -> dict[str, dg.PartitionMapping] | None:
        """Instantiate and return configured partition mappings, if any.

        Returns:
            dict[str, PartitionMapping] | None: Mapping per downstream dataset or None.
        """
        if self._partition_mapping is None:
            return None

        partition_mappings: dict[str, dg.PartitionMapping] = {}
        for (
            downstream_dataset_name,
            downstream_partition_mapping_info,
        ) in self._partition_mapping.items():
            # downstream_partition_mapping_info["type"] is a class object; cast it for typing
            partition_mapping_type = cast(
                type[dg.PartitionMapping],
                downstream_partition_mapping_info["type"],
            )
            partition_mapping_config: dict[str, Any] = downstream_partition_mapping_info["config"]

            try:
                partition_mapping = partition_mapping_type(**partition_mapping_config)
            except Exception as exc:
                raise ValueError(
                    f"Failed to instantiate partition mapping "
                    f"'{downstream_partition_mapping_info['type'].__name__}' "
                    f"with config: {downstream_partition_mapping_info['config']}"
                ) from exc

            partition_mappings[downstream_dataset_name] = partition_mapping

        return partition_mappings

    # TODO: Cache?
    def _list_available_partition_keys(self) -> list[str]:
        """List available partition keys present on the filesystem.

        Returns:
            list[str]: Available partition keys.
        """
        available_partitions: list[str] = self._list_partitions()

        partition_keys: list[str] = []
        base_path = Path(self._normalized_path)
        for partition in available_partitions:
            try:
                key = os.path.relpath(str(partition), start=str(base_path))
            except Exception:
                key = Path(partition).name

            if self._filename_suffix and key.endswith(self._filename_suffix):
                key = key[: -len(self._filename_suffix)]
            partition_keys.append(key)

        return partition_keys

    @cachedmethod(cache=operator.attrgetter("_partition_cache"))
    def _list_partitions(self) -> list[str]:
        """List partition paths according to the partitions definition.

        Returns:
            list[str]: Full paths of discovered partitions.
        """
        if self._partition_type is dg.DynamicPartitionsDefinition:
            return cast(list[str], super()._list_partitions())

        partitions_def = self._get_partitions_definition()
        partition_keys = partitions_def.get_partition_keys()

        base_path = Path(self._normalized_path)
        partitions: list[str] = []

        for key in partition_keys:
            # Check candidate paths: prefer key + filename_suffix if suffix is provided
            candidates: list[Path] = []
            if self._filename_suffix:
                candidates.append(base_path / f"{key}{self._filename_suffix}")
            candidates.append(base_path / key)

            for candidate in candidates:
                try:
                    if self._filesystem.exists(candidate):
                        partitions.append(str(candidate))
                        break
                except Exception:
                    # Ignore errors for individual candidates and continue checking others
                    continue

        return partitions

    def _get_filepath(self, partition: str) -> str:
        """Compute the full filepath for a given partition key.

        Args:
            partition (str): Partition key.

        Returns:
            str: Full path to the partition.
        """
        partition_def = self._get_partitions_definition()
        if partition_def is None:
            raise ValueError("Partition definition could not be instantiated.")

        if partition not in partition_def.get_partition_keys():
            raise ValueError(f"Partition '{partition}' not found in partition definition.")

        return str(Path(self._normalized_path) / partition)

    def load(self) -> dict[str, Callable[[], Any]]:
        """Load partitioned data lazily as callables per partition key.

        Returns:
            dict[str, Callable[[], Any]]: Map of partition key to loader callable.
        """
        if self._partition_type is dg.DynamicPartitionsDefinition:
            instance = dg.DagsterInstance.get()
            instance.add_dynamic_partitions(self._partition_config["name"], self._list_available_partition_keys())

        loaded_data = cast(dict[str, Callable[[], Any]], super().load())

        # Normalize keys to logical partition keys (e.g., "p1") instead of full paths.
        base = Path(self._normalized_path)
        normalized: dict[str, Callable[[], Any]] = {}
        for raw_key, value in loaded_data.items():
            key_str = str(raw_key)
            # Only attempt to relativize if key looks like a path pointing under base
            if os.path.isabs(key_str) or key_str.startswith(str(base)):
                try:
                    key_rel = os.path.relpath(key_str, start=str(base))
                except Exception:
                    key_rel = Path(key_str).name
            else:
                key_rel = key_str

            if self._filename_suffix and key_rel.endswith(self._filename_suffix):
                key_rel = key_rel[: -len(self._filename_suffix)]

            normalized[key_rel] = value

        return normalized

    def save(self, data: dict[str, Any]) -> None:
        """Save partitioned data.

        Args:
            data (dict[str, Any]): Map of partition key to data.
        """

        if self._partition_type is not dg.DynamicPartitionsDefinition:
            partitions_def = self._get_partitions_definition()
            partition_keys = partitions_def.get_partition_keys()
            if not isinstance(data, dict):
                raise TypeError(f"Expected data to be a dict mapping partition keys to data, but got: {type(data)}")
            elif all(key not in partition_keys for key in data.keys()):
                raise ValueError(
                    "No matching partitions found to save the provided data. Partition keys: "
                    f"{list(data.keys())}. Expected keys: {partition_keys}"
                )

        super().save(data)

    def _describe(self) -> dict[str, Any]:
        """Return a JSON-serializable description of the dataset configuration.

        Returns:
            dict[str, Any]: Description including dataset, partition, and mapping metadata.
        """
        partitioned_dataset_description = super()._describe()

        clean_partition_config = (
            {k: v for k, v in self._partition_config.items()}
            if isinstance(self._partition_config, dict)
            else self._partition_config
        )

        partitioned_dataset_description = partitioned_dataset_description | {
            "partition_type": self._partition_type.__name__,
            "partition_config": clean_partition_config,
        }

        if self._partition_mapping is not None:
            for downstream_dataset_name, downstream_partition_mapping_info in self._partition_mapping.items():
                downstream_partition_config: dict[str, Any] = downstream_partition_mapping_info["config"]
                clean_downstream_partition_mapping_config = (
                    {k: v for k, v in downstream_partition_config.items()}
                    if isinstance(downstream_partition_config, dict)
                    else downstream_partition_config
                )

                partitioned_dataset_description = partitioned_dataset_description | {
                    downstream_dataset_name: {
                        "partition_mapping": downstream_partition_mapping_info["type"].__name__,
                        "partition_mapping_config": clean_downstream_partition_mapping_config,
                    }
                }
        return partitioned_dataset_description  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        """Return a human-friendly representation of the dataset."""
        object_description = self._describe()

        # Dummy object to call _pretty_repr
        # Only clean_dataset_config parameters are exposed
        kwargs = deepcopy(self._dataset_config)
        kwargs[self._filepath_arg] = ""
        dataset = self._dataset_type(**kwargs)

        object_description_repr = {
            "filepath": object_description["path"],
            "dataset": dataset._pretty_repr(object_description["dataset_config"]),
            "partition": dataset._pretty_repr(object_description["partition_config"]),
        }

        # Render partition mappings (if any) from the description
        if self._partition_mapping:
            mapping_repr: dict[str, Any] = {}
            for downstream_dataset_name in self._partition_mapping.keys():
                downstream_desc = object_description.get(downstream_dataset_name)
                if isinstance(downstream_desc, dict) and "partition_mapping_config" in downstream_desc:
                    mapping_repr[downstream_dataset_name] = dataset._pretty_repr(
                        downstream_desc["partition_mapping_config"]
                    )
            if mapping_repr:
                object_description_repr["partition_mapping"] = mapping_repr

        return self._pretty_repr(object_description_repr)  # type: ignore[no-any-return]

    def _exists(self) -> bool:
        """Return True when at least one partition exists."""
        return bool(self._list_partitions())
