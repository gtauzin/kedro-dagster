from typing import Any

from kedro.io.core import AbstractDataSet


class DagsterNothingDataset(AbstractDataSet):
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
