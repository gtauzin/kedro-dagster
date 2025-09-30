import copy
import operator
from collections.abc import Callable
from copy import deepcopy
from typing import Any

import dagster as dg
from cachetools import cachedmethod
from kedro.utils import _load_obj
from kedro_datasets.partitions import PartitionedDataset

TYPE_KEY = "type"
_DEFAULT_PACKAGES = ["dagster.", ""]


def parse_partition_definition(
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
    partition_type = config.pop(TYPE_KEY)

    class_obj = None
    error_msg = None
    if isinstance(partition_type, str):
        if len(partition_type.strip(".")) != len(partition_type):
            raise TypeError("'type' class path does not support relative paths or paths ending with a dot.")
        class_paths = (prefix + partition_type for prefix in _DEFAULT_PACKAGES)

        for class_path in class_paths:
            tmp, error_msg = _load_obj(class_path)  # Load partition class, capture the warning

            if tmp is not None:
                class_obj = tmp
                break

        if class_obj is None:  # If no valid class was found, raise an error
            default_error_msg = f"Class '{partition_type}' not found, is this a typo?"
            raise TypeError(f"{error_msg if error_msg else default_error_msg}")

    if not class_obj:
        class_obj = partition_type

    if not issubclass(class_obj, dg.PartitionsDefinition):
        raise ValueError(
            f"Partition definition type '{class_obj.__module__}.{class_obj.__qualname__}' "
            f"is invalid: all partition definition types must extend 'PartitionsDefinition'."
        )

    return class_obj, config


class DagsterPartitionedDataset(PartitionedDataset):
    def __init__(  # noqa: PLR0913
        self,
        *,
        path: str,
        dataset: dict[str, Any],
        partition: dict[str, Any],
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        overwrite: bool = False,
        save_lazily: bool = True,
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
        )

        partition = partition if isinstance(partition, dict) else {"type": dataset}
        self._partition_type, self._partition_config = parse_partition_definition(dataset)

    def _get_partitions_definition(self) -> dg.PartitionsDefinition | None:
        try:
            return self._partition_type(**self._partition_config)
        except Exception as exc:
            raise ValueError(
                f"Failed to instantiate partition definition "
                f"'{self._partition_type.__name__}' with config: {self._partition_config}"
            ) from exc

    @cachedmethod(cache=operator.attrgetter("_partition_cache"))
    def _list_partitions(self) -> list[str]:
        return [
            path
            for path in self._filesystem.find(self._normalized_path, **self._load_args)
            if path.endswith(self._filename_suffix)
        ]

    def load(self, partition: str | None = None) -> dict[str, Callable[[], Any]]:
        return super().load()  # type: ignore[no-any-return]

    def save(self, data: dict[str, Any], partition: str | None = None) -> None:
        if isinstance(data, dict):
            return super().save(data)  # type: ignore[no-any-return]

        self._invalidate_caches()

    def _describe(self) -> dict[str, Any]:
        partitioned_dataset_description = super()._describe()

        clean_partition_config = (
            {k: v for k, v in self._partition_config.items()}
            if isinstance(self._partition_config, dict)
            else self._partition_config
        )
        return partitioned_dataset_description | {  # type: ignore[no-any-return]
            "partition_type": self._partition_type.__name__,
            "partition_config": clean_partition_config,
        }

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

        return self._pretty_repr(object_description_repr)  # type: ignore[no-any-return]

    def _exists(self) -> bool:
        return bool(self._list_partitions())
