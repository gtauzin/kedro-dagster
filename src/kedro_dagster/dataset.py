import copy
from typing import Any

from kedro.io import AbstractDataset


class DagsterPartitionDataset(AbstractDataset):
    """
    ``DagsterPartitionDataset`` is a Kedro dataset wrapper that integrates
    Dagster partitioning into Kedro pipelines. It dynamically resolves
    partition keys at runtime, allowing for seamless support of Dagster
    partitions, backfills, and partition dependencies within Kedro's data catalog.

    This dataset wraps an underlying dataset configuration and injects
    the current Dagster partition key into its parameters, enabling
    partition-specific data loading and saving. It also carries metadata
    about upstream dependencies and partition mappings for integration with
    kedro-dagster translation logic.

    Example:
    ::

        >>> from kedro.io import DataCatalog
        >>> catalog = DataCatalog.from_config({
        ...     "sales_summary": {
        ...         "type": "path.to.DagsterPartitionDataset",
        ...         "dataset": {
        ...             "type": "pandas.CSVDataset",
        ...             "filepath": "data/sales/{partition}/data.csv",
        ...         },
        ...         "partitions": {
        ...             "type": "DailyPartitionsDefinition",
        ...             "start_date": "2023-01-01"
        ...         },
        ...         "depends_on": {
        ...             "raw_sales_data": {
        ...                 "partition_mapping": {
        ...                     "type": "IdentityPartitionMapping"
        ...                 }
        ...             }
        ...         }
        ...     }
        ... })
        >>> sales_summary = catalog.load("sales_summary")
    """

    def __init__(
        self,
        dataset: dict[str, Any],
        partitions: dict[str, Any],
        depends_on: dict[str, Any] | None = None,
    ):
        """
        Initialize a new instance of ``DagsterPartitionDataset``.

        Args:
            dataset: Configuration dictionary for the underlying Kedro dataset.
                Must include the 'type' key and any other required parameters.
                The configuration can include placeholders like '{partition}'
                which will be replaced with the actual partition key at runtime.
            partitions: Dictionary defining the Dagster partitioning scheme.
                Used for metadata and translated into a Dagster PartitionsDefinition.
            depends_on: Optional mapping of upstream dataset names to their
                partition mapping configurations. This metadata is used by
                kedro-dagster to set up AssetIn(partition_mapping=...) for
                dependencies in Dagster.
        """
        super().__init__()
        self._dataset_config = copy.deepcopy(dataset)
        self._partitions = partitions
        self._depends_on = copy.deepcopy(depends_on) or {}
