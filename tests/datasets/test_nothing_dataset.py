from __future__ import annotations

from kedro_dagster.datasets.nothing_dataset import DagsterNothingDataset


class TestDagsterNothingDataset:
    def test_load_returns_none(self):
        ds = DagsterNothingDataset(metadata={"a": 1})
        assert ds.load() is None

    def test_save_is_noop(self):
        ds = DagsterNothingDataset()
        assert ds.save("ignored") is None

    def test_exists_always_true(self):
        ds = DagsterNothingDataset()
        assert ds._exists() is True

    def test_describe_type(self):
        ds = DagsterNothingDataset()
        assert ds._describe() == {"type": "DagsterNothingDataset"}
