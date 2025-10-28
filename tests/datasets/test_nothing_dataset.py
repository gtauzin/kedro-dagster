# mypy: ignore-errors

from __future__ import annotations

from kedro_dagster.datasets.nothing_dataset import NOTHING_OUTPUT, DagsterNothingDataset


class TestDagsterNothingDataset:
    def test_load_returns_none(self):
        """Loading returns the sentinel NOTHING_OUTPUT value (None-like)."""
        ds = DagsterNothingDataset(metadata={"a": 1})
        assert ds.load() == NOTHING_OUTPUT

    def test_save_is_noop(self):
        """Saving is a no-op for Nothing datasets and returns None."""
        ds = DagsterNothingDataset()
        assert ds.save("ignored") is None

    def test_exists_always_true(self):
        """Nothing datasets always report existence as True."""
        ds = DagsterNothingDataset()
        assert ds._exists() is True

    def test_describe_type(self):
        """_describe returns the dataset type for Nothing dataset."""
        ds = DagsterNothingDataset()
        assert ds._describe() == {"type": "DagsterNothingDataset"}
