import copy
import operator
from collections.abc import Callable
from copy import deepcopy
from typing import Any

import dagster as dg
from cachetools import cachedmethod
from kedro.io.core import _load_obj
from kedro_datasets.partitions import PartitionedDataset

TYPE_KEY = "type"
_DEFAULT_PACKAGES = ["dagster.", ""]


def parse_dagster_definition(
    config: dict[str, Any],
) -> tuple[type[dg.PartitionsDefinition], dict[str, Any]]:
    """Parse and instantiate a partition definition class using the configuration provided.

    Args:
        config: Partition definition config dictionary. It *must* contain the `type` key
            with fully qualified class name or the class object.

    Raises:
        DatasetError: If the function fails to parse the configuration provided.

    Returns:
        2-tuple: (Dataset class object, configuration dictionary)
    """
    config = copy.deepcopy(config)
    definition_type = config.pop(TYPE_KEY)

    class_obj = None
    error_msg = None
    if isinstance(definition_type, str):
        if len(definition_type.strip(".")) != len(definition_type):
            raise TypeError("'type' class path does not support relative paths or paths ending with a dot.")
        class_paths = (prefix + definition_type for prefix in _DEFAULT_PACKAGES)

        for class_path in class_paths:
            tmp, error_msg = _load_obj(class_path)  # Load partition class, capture the warning

            if tmp is not None:
                class_obj = tmp
                break

        if class_obj is None:  # If no valid class was found, raise an error
            default_error_msg = f"Class '{definition_type}' not found, is this a typo?"
            raise TypeError(f"{error_msg if error_msg else default_error_msg}")

    if not class_obj:
        class_obj = definition_type

    # TODO
    # if not issubclass(class_obj, dg.PartitionsDefinition):
    #     raise ValueError(
    #         f"Partition definition type '{class_obj.__module__}.{class_obj.__qualname__}' "
    #         f"is invalid: all partition definition types must extend 'PartitionsDefinition'."
    #     )

    return class_obj, config


class DagsterPartitionedDataset(PartitionedDataset):
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

        self._partition_mapping = None
        if partition_mapping is not None:
            self._partition_mapping = {}
            for downstream_dataset_name, downstream_partition_mapping in partition_mapping.items():
                downstream_partition_mapping = downstream_partition_mapping if isinstance(downstream_partition_mapping, dict) else {"type": downstream_partition_mapping}
                downstream_partition_mapping_type, downstream_partition_mapping_config = parse_dagster_definition(downstream_partition_mapping)
                self._partition_mapping[downstream_dataset_name] = {
                    "type": downstream_partition_mapping_type,
                    "config": downstream_partition_mapping_config,
                }

    def _validate_partitions_definition(self, partition: dict[str, Any]) -> None:
        if "type" not in partition:
            raise ValueError("Partition definition must contain the 'type' key.")

    def _get_partitions_definition(self) -> dg.PartitionsDefinition:
        try:
            partition_def = self._partition_type(**self._partition_config)
        except Exception as exc:
            raise ValueError(
                f"Failed to instantiate partitions definition "
                f"'{self._partition_type.__name__}' with config: {self._partition_config}"
            ) from exc

        return partition_def

    def _get_mapped_downstream_dataset_names(self) -> list[str]:
        if self._partition_mapping is None:
            return []

        return list(self._partition_mapping.keys())

    def _get_partition_mappings(self) -> dict[str, dg.PartitionMapping] | None:
        if self._partition_mapping is None :
            return None

        partition_mappings = {}
        for downstream_dataset_name in self._partition_mapping.keys():
            try:
                partition_mapping_type, partition_mapping_config = (
                    self._partition_mapping[downstream_dataset_name]["type"],
                    self._partition_mapping[downstream_dataset_name]["config"],
                )
                partition_mapping = partition_mapping_type(**partition_mapping_config)
            except Exception as exc:
                raise ValueError(
                    f"Failed to instantiate partition mapping "
                    f"'{self._partition_mapping[downstream_dataset_name]['type'].__name__}' "
                    f"with config: {self._partition_mapping[downstream_dataset_name]['config']}"
                ) from exc
            
            partition_mappings[downstream_dataset_name] = partition_mapping

        return partition_mappings

    # TODO: Cache?
    def _list_available_partition_keys(self) -> list[str]:
        available_partitions = self._list_partitions()

        partition_keys = []
        for partition in available_partitions:
            base_path = self._normalized_path.rstrip("/") + "/"
            if partition.startswith(base_path):
                key = partition[len(base_path):]
                if self._filename_suffix and key.endswith(self._filename_suffix):
                    key = key[: -len(self._filename_suffix)]
                partition_keys.append(key)

        return partition_keys

    @cachedmethod(cache=operator.attrgetter("_partition_cache"))
    def _list_partitions(self) -> list[str]:
        if self._partition_type is dg.DynamicPartitionsDefinition:
            return super()._list_partitions()

        partitions_def = self._get_partitions_definition()
        partition_keys = partitions_def.get_partition_keys()

        base_path = self._normalized_path.rstrip("/") + "/"
        partitions: list[str] = []

        for key in partition_keys:
            # Check candidate paths: prefer key + filename_suffix if suffix is provided
            candidates: list[str] = []
            if self._filename_suffix:
                candidates.append(base_path + key + self._filename_suffix)
            candidates.append(base_path + key)

            for candidate in candidates:
                try:
                    if self._filesystem.exists(candidate):
                        partitions.append(candidate)
                        break
                except Exception:
                    # Ignore errors for individual candidates and continue checking others
                    continue

        return partitions

    def _get_filepath(self, partition: str) -> str:
        partition_def = self._get_partitions_definition()
        if partition_def is None:
            raise ValueError("Partition definition could not be instantiated.")

        if partition not in partition_def.get_partition_keys():
            raise ValueError(f"Partition '{partition}' not found in partition definition.")

        file_path = self._normalized_path.rstrip("/") + "/" + partition
        return file_path

    def load(self) -> dict[str, Callable[[], Any]]:
        if self._partition_type is dg.DynamicPartitionsDefinition:
            instance = dg.DagsterInstance.get()
            instance.add_dynamic_partitions(self._partition_config["name"], self._list_available_partition_keys())

        return super().load()

    def save(self, data: dict[str, Any]) -> None:
        if self._partition_type is dg.DynamicPartitionsDefinition:
            instance = dg.DagsterInstance.get()
            instance.add_dynamic_partitions(self._partition_config["name"], self._list_available_partition_keys())

        return super().save(data)

    def _describe(self) -> dict[str, Any]:
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
            for downstream_dataset_name, (self._partition_mapping_type, self._partition_mapping_config) in self._partition_mapping.items():
                downstream_partition_config = self._partition_mapping[downstream_dataset_name]["config"]
                clean_downstream_partition_mapping_config = (
                    {k: v for k, v in downstream_partition_config.items()}
                    if isinstance(downstream_partition_config, dict)
                    else downstream_partition_config
                )

                partitioned_dataset_description = partitioned_dataset_description | {
                    downstream_dataset_name: {
                        "partition_mapping": self._partition_mapping[downstream_dataset_name]["type"].__name__,
                        "partition_mapping_config": clean_downstream_partition_mapping_config,
                    }
                }

        return partitioned_dataset_description # type: ignore[no-any-return]

    def __repr__(self) -> str:
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

        if self._partition_mapping_type is not None:
            object_description_repr["partition_mapping"] = dataset._pretty_repr(object_description["partition_mapping_config"])

        return self._pretty_repr(object_description_repr)  # type: ignore[no-any-return]

    def _exists(self) -> bool:
        return bool(self._list_partitions())
