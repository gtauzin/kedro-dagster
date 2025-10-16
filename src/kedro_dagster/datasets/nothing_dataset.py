from typing import Any

from kedro.io.core import AbstractDataset


class DagsterNothingDataset(AbstractDataset):
    """A Kedro dataset that does nothing, used to represent placeholder outputs.
    This dataset can be used in Kedro pipelines to signify that a node has completed its task
    without actually producing any tangible output. It is particularly useful for enforcing
    execution order in pipelines where certain nodes need to run after others without passing data.
    """
    def __init__(
        self,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()

        self._metadata = metadata or {}

    def load(self) -> Any:
        """Return None for every load."""
        return None

    def save(self, data: Any) -> None:
        """Do nothing when saving data."""
        # Intentionally do not persist anything
        return None

    def _exists(self) -> bool:
        """Always report that the dataset exists."""
        return True

    def _describe(self) -> dict[str, Any]:
        return {"type": self.__class__.__name__}
