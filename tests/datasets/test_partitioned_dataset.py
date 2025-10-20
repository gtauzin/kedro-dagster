# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest

from kedro_dagster.datasets.partitioned_dataset import (
    DagsterPartitionedDataset,
    parse_dagster_definition,
)


def _make_static_dataset(tmp_path: Path, filename_suffix: str = "") -> DagsterPartitionedDataset:
    base = tmp_path / "data" / "03_primary" / "intermediate"
    base.mkdir(parents=True, exist_ok=True)
    # Create candidate files for p1, p2
    if filename_suffix:
        (base / f"p1{filename_suffix}").write_text("one")
        (base / f"p2{filename_suffix}").write_text("two")
    else:
        (base / "p1").write_text("one")
        (base / "p2").write_text("two")

    return DagsterPartitionedDataset(
        path=str(base),
        dataset={"type": "pandas.CSVDataset"},
        partition={"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
        filename_suffix=filename_suffix,
    )


class TestParseDagsterDefinition:
    def test_with_string_and_class(self):
        cls, cfg = parse_dagster_definition({"type": "StaticPartitionsDefinition", "partition_keys": ["a"]})
        assert cls is dg.StaticPartitionsDefinition
        assert cfg == {"partition_keys": ["a"]}

        cls2, cfg2 = parse_dagster_definition({"type": dg.IdentityPartitionMapping, "foo": 1})
        assert cls2 is dg.IdentityPartitionMapping
        assert cfg2 == {"foo": 1}

    def test_invalid_raises(self):
        with pytest.raises(TypeError):
            parse_dagster_definition({"type": ".StaticPartitionsDefinition"})


class TestDagsterPartitionedDataset:
    def test_validate_partition_type_missing_key(self):
        ds = DagsterPartitionedDataset(
            path="in-memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["p1"]},
        )
        with pytest.raises(ValueError):
            ds._validate_partitions_definition({})

    def test_get_partitions_definition_and_keys(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path)
        pd = ds._get_partitions_definition()
        assert isinstance(pd, dg.StaticPartitionsDefinition)
        assert set(pd.get_partition_keys()) == {"p1", "p2"}

    def test_list_partitions_and_keys_without_suffix(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path)
        parts = ds._list_partitions()
        EXPECTED_PARTS = 2
        assert len(parts) == EXPECTED_PARTS and all(
            str(tmp_path / "data" / "03_primary" / "intermediate") in p for p in parts
        )
        keys = ds._list_available_partition_keys()
        assert set(keys) == {"p1", "p2"}

    def test_list_partitions_and_keys_with_suffix(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path, filename_suffix=".csv")
        keys = ds._list_available_partition_keys()
        assert set(keys) == {"p1", "p2"}

    def test_get_filepath_valid_and_invalid(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path)
        ok = ds._get_filepath("p1")
        assert ok.endswith("/p1")
        with pytest.raises(ValueError):
            ds._get_filepath("missing")

    def test_partition_mappings_instantiation(self):
        ds = DagsterPartitionedDataset(
            path="memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a"]},
            partition_mapping={"downstream": {"type": "IdentityPartitionMapping"}},
        )
        assert ds._get_mapped_downstream_dataset_names() == ["downstream"]
        mappings = ds._get_partition_mappings()
        assert isinstance(mappings, dict)
        assert isinstance(mappings["downstream"], dg.IdentityPartitionMapping)

    def test_describe_and_repr_include_partition_info(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path)
        desc = ds._describe()
        assert desc["partition_type"] == "StaticPartitionsDefinition"
        assert desc["partition_config"] == {"partition_keys": ["p1", "p2"]}
        rep = repr(ds)
        assert "partition" in rep and "dataset" in rep

    def test_exists_matches_partition_listing(self, tmp_path: Path):
        ds = _make_static_dataset(tmp_path)
        assert ds._exists() is True

    def test_dynamic_partitions_triggers_instance_calls(self, monkeypatch, tmp_path: Path):
        base = tmp_path / "data" / "03_primary" / "dynamic"
        base.mkdir(parents=True, exist_ok=True)

        calls: list[tuple[str, list[str]]] = []

        class DummyInstance:
            def add_dynamic_partitions(self, name: str, keys: list[str]) -> None:  # noqa: D401
                calls.append((name, keys))

        monkeypatch.setattr(dg.DagsterInstance, "get", lambda: DummyInstance())

        ds = DagsterPartitionedDataset(
            path=str(base),
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "DynamicPartitionsDefinition", "name": "dyn"},
        )

        with pytest.raises(Exception):
            ds.load()
        assert calls and calls[0] == ("dyn", [])
