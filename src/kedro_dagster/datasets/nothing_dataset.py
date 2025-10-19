from typing import Any

from kedro.io.core import AbstractDataset


class DagsterNothingDataset(AbstractDataset):
    """A Kedro dataset used to represent placeholder outputs.

    This dataset can be used in Kedro pipelines to signify that a node has completed its task
    without actually producing any tangible output. It is particularly useful for enforcing
    execution order in pipelines where certain nodes need to run after others without passing data.
    """

    def __init__(
        self,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create a dataset that performs no I/O and returns None.

        Args:
            metadata (dict[str, Any] | None): Optional metadata stored alongside the dataset.
        """
        super().__init__()

        self._metadata = metadata or {}

    def load(self) -> Any:
        """Return None for every load.

        Returns:
            Any: Always None.
        """
        return None

    def save(self, data: Any) -> None:
        """Do nothing when saving data.

        Args:
            data (Any): Ignored.
        """
        # Intentionally do not persist anything
        return None

    def _exists(self) -> bool:
        """Always report that the dataset exists.

        Returns:
            bool: Always True.
        """
        return True

    def _describe(self) -> dict[str, Any]:
        """Return a JSON-serializable description of the dataset.

        Returns:
            dict[str, Any]: Basic dataset type info.
        """
        return {"type": self.__class__.__name__}
