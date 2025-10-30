# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest
from kedro.io.core import DatasetError

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


def test_definintion_parsing_with_string_and_class():
    """parse_dagster_definition accepts dotted class path or class object and returns config."""
    cls, cfg = parse_dagster_definition({"type": "StaticPartitionsDefinition", "partition_keys": ["a"]})
    assert cls is dg.StaticPartitionsDefinition
    assert cfg == {"partition_keys": ["a"]}

    cls2, cfg2 = parse_dagster_definition({"type": dg.IdentityPartitionMapping, "foo": 1})
    assert cls2 is dg.IdentityPartitionMapping
    assert cfg2 == {"foo": 1}


def test_definition_parsing_invalid_raises():
    """Invalid type path (relative/ends with dot) should raise TypeError."""
    with pytest.raises(TypeError):
        parse_dagster_definition({"type": ".StaticPartitionsDefinition"})


def test_definition_parsing_invalid_raises_with_message():
    """Error message explains that relative paths or trailing dots are not supported."""
    with pytest.raises(TypeError) as exc:
        parse_dagster_definition({"type": ".StaticPartitionsDefinition"})
    assert "does not support relative paths or paths ending with a dot" in str(exc.value)


class TestDagsterPartitionedDataset:
    def test_validate_partition_type_missing_key(self):
        """Missing required partition_keys in definition triggers ValueError."""
        dataset = DagsterPartitionedDataset(
            path="in-memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["p1"]},
        )
        with pytest.raises(ValueError):
            dataset._validate_partitions_definition({})

    def test_get_partitions_definition_and_keys(self, tmp_path: Path):
        """Instantiate partitions definition and enumerate available keys."""
        dataset = _make_static_dataset(tmp_path)
        partitions_def = dataset._get_partitions_definition()
        assert isinstance(partitions_def, dg.StaticPartitionsDefinition)
        assert set(partitions_def.get_partition_keys()) == {"p1", "p2"}

    def test_list_partitions_and_keys_without_suffix(self, tmp_path: Path):
        """List partition filepaths and available keys when no filename_suffix is configured."""
        dataset = _make_static_dataset(tmp_path)
        partitions = dataset._list_partitions()
        EXPECTED_PARTS = 2
        assert len(partitions) == EXPECTED_PARTS and all(
            str(tmp_path / "data" / "03_primary" / "intermediate") in p for p in partitions
        )
        keys = dataset._list_available_partition_keys()
        assert set(keys) == {"p1", "p2"}

    def test_list_partitions_and_keys_with_suffix(self, tmp_path: Path):
        """Keys are correctly extracted when filename_suffix is used."""
        dataset = _make_static_dataset(tmp_path, filename_suffix=".csv")
        keys = dataset._list_available_partition_keys()
        assert set(keys) == {"p1", "p2"}

    def test_get_filepath_valid_and_invalid(self, tmp_path: Path):
        """Resolve filepath for a valid partition key and raise for unknown keys."""
        dataset = _make_static_dataset(tmp_path)
        ok = dataset._get_filepath("p1")
        assert ok.endswith("p1")
        with pytest.raises(ValueError) as exc:
            dataset._get_filepath("missing")
        assert str(exc.value) == "Partition 'missing' not found in partition definition."

    def test_partition_mappings_instantiation(self):
        """Instantiate partition mapping objects from configuration."""
        dataset = DagsterPartitionedDataset(
            path="memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a"]},
            partition_mapping={"downstream": {"type": "IdentityPartitionMapping"}},
        )
        assert dataset._get_mapped_downstream_dataset_names() == ["downstream"]
        mappings = dataset._get_partition_mappings()
        assert isinstance(mappings, dict)
        assert isinstance(mappings["downstream"], dg.IdentityPartitionMapping)

    def test_describe_and_repr_include_partition_info(self, tmp_path: Path):
        """_describe and __repr__ include partition type and configuration details."""
        dataset = _make_static_dataset(tmp_path)
        description = dataset._describe()
        assert description["partition_type"] == "StaticPartitionsDefinition"
        assert description["partition_config"] == {"partition_keys": ["p1", "p2"]}
        representation = repr(dataset)
        assert "partition" in representation and "dataset" in representation

    def test_exists_matches_partition_listing(self, tmp_path: Path):
        """exists() returns True when partition files are present at the path."""
        dataset = _make_static_dataset(tmp_path)
        assert dataset._exists() is True

    def test_dynamic_partitions_triggers_instance_calls(self, monkeypatch, tmp_path: Path):
        """DynamicPartitionsDefinition registers (empty) keys with DagsterInstance and errors on load."""
        base = tmp_path / "data" / "03_primary" / "dynamic"
        base.mkdir(parents=True, exist_ok=True)

        calls: list[tuple[str, list[str]]] = []

        class DummyInstance:
            def add_dynamic_partitions(self, name: str, keys: list[str]) -> None:
                calls.append((name, keys))

        monkeypatch.setattr(dg.DagsterInstance, "get", lambda: DummyInstance())

        dataset = DagsterPartitionedDataset(
            path=str(base),
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "DynamicPartitionsDefinition", "name": "dyn"},
        )

        with pytest.raises(Exception) as exc:
            dataset.load()
        assert "No partitions found in" in str(exc.value)
        assert calls and calls[0] == ("dyn", [])

    def test_get_partitions_definition_instantiation_error_message(self):
        """Missing required fields for partitions definition yields a helpful error message."""
        dataset = DagsterPartitionedDataset(
            path="memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "DynamicPartitionsDefinition"},  # missing required 'name'
        )
        with pytest.raises(ValueError) as exc:
            dataset._get_partitions_definition()
        assert (
            str(exc.value)
            == "Failed to instantiate partitions definition 'DynamicPartitionsDefinition' with config: {}"
        )

    def test_partition_mappings_instantiation_error_message(self):
        """Invalid partition mapping configuration yields a helpful instantiation error."""
        dataset = DagsterPartitionedDataset(
            path="memory",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a"]},
            partition_mapping={"down": {"type": "IdentityPartitionMapping", "unknown": 1}},
        )
        with pytest.raises(ValueError) as exc:
            dataset._get_partition_mappings()
        assert (
            str(exc.value)
            == "Failed to instantiate partition mapping 'IdentityPartitionMapping' with config: {'unknown': 1}"
        )

    def test_save_non_dict_type_error_message(self, tmp_path: Path):
        """Saving a non-dict raises DatasetError and includes the inner validation message."""
        dataset = _make_static_dataset(tmp_path)
        with pytest.raises(DatasetError) as exc:
            dataset.save(["not", "a", "dict"])
        # Wrapped by Kedro's DatasetError; ensure our inner message is present
        assert "Expected data to be a dict mapping partition keys to data, but got: <class 'list'>" in str(exc.value)

    def test_save_no_matching_keys_error_message(self, tmp_path: Path):
        """Saving dict with keys not present in defined partitions should raise with message."""
        dataset = _make_static_dataset(tmp_path)
        with pytest.raises(DatasetError) as exc:
            dataset.save({"missing": 1})
        assert "No matching partitions found to save the provided data." in str(exc.value)

    def test_list_partitions_prefers_suffix_when_both_exist(self, tmp_path: Path):
        """When both suffixed and unsuffixed files exist, prefer suffixed entries in listings."""
        dataset = _make_static_dataset(tmp_path, filename_suffix=".csv")
        base = tmp_path / "data" / "03_primary" / "intermediate"
        # create unsuffixed candidates too
        (base / "p1").write_text("dup-one")
        (base / "p2").write_text("dup-two")
        parts = dataset._list_partitions()
        assert str(base / "p1.csv") in parts and str(base / "p1") not in parts
        assert str(base / "p2.csv") in parts and str(base / "p2") not in parts

    def test_list_partitions_handles_exists_exception(self, tmp_path: Path, monkeypatch):
        """Fallback to unsuffixed files when filesystem.exists raises for suffixed paths."""
        dataset = _make_static_dataset(tmp_path, filename_suffix=".csv")
        base = tmp_path / "data" / "03_primary" / "intermediate"
        # create unsuffixed candidate too to allow fallback
        (base / "p1").write_text("dup-one")

        def flaky_exists(path: str) -> bool:
            if path.endswith(".csv"):
                raise OSError("boom")
            return True

        monkeypatch.setattr(dataset._filesystem, "exists", flaky_exists, raising=True)
        parts = dataset._list_partitions()
        assert str(base / "p1") in parts  # falls back to unsuffixed

    def test_list_available_partition_keys_dedup_and_suffix_stripped(self, tmp_path: Path):
        """Deduplicate keys and strip filename suffix when generating partition keys."""
        dataset = _make_static_dataset(tmp_path, filename_suffix=".csv")
        base = tmp_path / "data" / "03_primary" / "intermediate"
        # duplicate unsuffixed for one key
        (base / "p1").write_text("dup-one")
        keys = dataset._list_available_partition_keys()
        assert set(keys) == {"p1", "p2"}

    def test_exists_false_when_no_partitions(self, tmp_path: Path):
        """exists() returns False when no partition files are present under the path."""
        empty_base = tmp_path / "data" / "03_primary" / "empty"
        empty_base.mkdir(parents=True, exist_ok=True)
        dataset = DagsterPartitionedDataset(
            path=str(empty_base),
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
        )
        assert dataset._exists() is False

    def test_dynamic_partitions_triggers_instance_calls_with_keys(self, monkeypatch, tmp_path: Path):
        """Dynamic partitions register existing keys with DagsterInstance during load."""
        base = tmp_path / "data" / "03_primary" / "dynamic_nonempty"
        base.mkdir(parents=True, exist_ok=True)
        # create files to generate keys
        (base / "k1").write_text("one")
        (base / "k2").write_text("two")

        calls: list[tuple[str, list[str]]] = []

        class DummyInstance:
            def add_dynamic_partitions(self, name: str, keys: list[str]) -> None:
                calls.append((name, keys))

        monkeypatch.setattr(dg.DagsterInstance, "get", lambda: DummyInstance())

        dataset = DagsterPartitionedDataset(
            path=str(base),
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "DynamicPartitionsDefinition", "name": "dyn2"},
        )

        # load should not raise; it returns lazy loaders, but must call instance with existing keys
        dataset.load()
        assert calls and calls[0][0] == "dyn2" and set(calls[0][1]) == {"k1", "k2"}
