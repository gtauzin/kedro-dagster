# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest
from kedro.io.core import DatasetError

import kedro_dagster.datasets.partitioned_dataset as _pdmod  # used for monkeypatching os.path.relpath
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

    def test_get_partitions_definition_instantiation_error_message(self):
        """Missing required fields for partitions definition yields a helpful error message."""
        # StaticPartitionsDefinition missing required 'partition_keys'
        with pytest.raises(NotImplementedError):
            # Will fail validation before getting to instantiation
            DagsterPartitionedDataset(
                path="memory",
                dataset={"type": "pandas.CSVDataset"},
                partition={"type": "DynamicPartitionsDefinition"},  # not supported
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

    def test_load_relpath_exception_falls_back_to_basename(self, monkeypatch, tmp_path: Path):
        """If os.path.relpath raises, load() should fall back to basename and strip suffix."""
        base = tmp_path / "data" / "03_primary" / "intermediate"
        base.mkdir(parents=True, exist_ok=True)
        (base / "p1.csv").write_text("one")

        dataset = DagsterPartitionedDataset(
            path=str(base),
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["p1"]},
            filename_suffix=".csv",
        )

        # Ensure super().load() returns an absolute path key so the code path
        # that calls os.path.relpath is taken. We monkeypatch the parent
        # PartitionedDataset.load to return a mapping with an absolute key.
        abs_key = str(base / "p1.csv")
        monkeypatch.setattr(
            _pdmod.PartitionedDataset,
            "load",
            lambda self: {abs_key: (lambda: None)},
            raising=True,
        )

        # Force relpath to raise to exercise the except branch in load()
        monkeypatch.setattr(
            _pdmod.os.path,
            "relpath",
            lambda *a, **k: (_ for _ in ()).throw(OSError("boom")),
            raising=True,
        )

        loaded = dataset.load()
        # Expect the logical key 'p1' after basename fallback and suffix stripping
        assert "p1" in loaded

    def test_unsupported_partition_definition_time_window_raises(self):
        """Test that TimeWindowPartitionsDefinition raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={
                    "type": "TimeWindowPartitionsDefinition",
                    "start": "2024-01-01",
                    "cron_schedule": "0 0 * * *",
                    "fmt": "%Y-%m-%d",
                },
            )
        assert "TimeWindowPartitionsDefinition" in str(exc.value)
        assert "StaticPartitionsDefinition" in str(exc.value)

    def test_unsupported_partition_definition_daily_raises(self):
        """Test that DailyPartitionsDefinition raises NotImplementedError."""
        # DailyPartitionsDefinition is a subclass of TimeWindowPartitionsDefinition
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={
                    "type": "DailyPartitionsDefinition",
                    "start_date": "2024-01-01",
                    "fmt": "%Y-%m-%d",
                },
            )
        assert "DailyPartitionsDefinition" in str(exc.value)
        assert "StaticPartitionsDefinition" in str(exc.value)

    def test_unsupported_partition_definition_hourly_raises(self):
        """Test that HourlyPartitionsDefinition raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={
                    "type": "HourlyPartitionsDefinition",
                    "start_date": "2024-01-01",
                },
            )
        assert "HourlyPartitionsDefinition" in str(exc.value)
        assert "StaticPartitionsDefinition" in str(exc.value)

    def test_unsupported_partition_definition_multi_raises(self):
        """Test that MultiPartitionsDefinition raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={
                    "type": "MultiPartitionsDefinition",
                    "partitions_defs": {
                        "date": {"type": "StaticPartitionsDefinition", "partition_keys": ["2024-01"]},
                        "color": {"type": "StaticPartitionsDefinition", "partition_keys": ["red"]},
                    },
                },
            )
        assert "MultiPartitionsDefinition" in str(exc.value)
        assert "StaticPartitionsDefinition" in str(exc.value)

    def test_unsupported_partition_definition_dynamic_raises(self):
        """Test that DynamicPartitionsDefinition raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={"type": "DynamicPartitionsDefinition", "name": "dyn"},
            )
        assert "DynamicPartitionsDefinition" in str(exc.value)
        assert "StaticPartitionsDefinition" in str(exc.value)

    def test_unsupported_partition_mapping_all_raises(self):
        """Test that AllPartitionMapping raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
                partition_mapping={"downstream": {"type": "AllPartitionMapping"}},
            )
        assert "AllPartitionMapping" in str(exc.value)
        assert "StaticPartitionMapping" in str(exc.value)
        assert "IdentityPartitionMapping" in str(exc.value)

    def test_unsupported_partition_mapping_time_window_raises(self):
        """Test that TimeWindowPartitionMapping raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc:
            DagsterPartitionedDataset(
                path="data/parts",
                dataset={"type": "pandas.CSVDataset"},
                partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
                partition_mapping={"downstream": {"type": "TimeWindowPartitionMapping", "start_offset": -1}},
            )
        assert "TimeWindowPartitionMapping" in str(exc.value)
        assert "StaticPartitionMapping" in str(exc.value)
        assert "IdentityPartitionMapping" in str(exc.value)

    def test_supported_identity_partition_mapping(self):
        """Test that IdentityPartitionMapping is accepted."""
        dataset = DagsterPartitionedDataset(
            path="data/parts",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
            partition_mapping={"downstream": {"type": "IdentityPartitionMapping"}},
        )
        mappings = dataset._get_partition_mappings()
        assert mappings is not None
        assert isinstance(mappings["downstream"], dg.IdentityPartitionMapping)

    def test_supported_static_partition_mapping(self):
        """Test that StaticPartitionMapping is accepted."""
        dataset = DagsterPartitionedDataset(
            path="data/parts",
            dataset={"type": "pandas.CSVDataset"},
            partition={"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
            partition_mapping={
                "downstream": {
                    "type": "StaticPartitionMapping",
                    "downstream_partition_keys_by_upstream_partition_key": {"a": "x", "b": "y"},
                }
            },
        )
        mappings = dataset._get_partition_mappings()
        assert mappings is not None
        assert isinstance(mappings["downstream"], dg.StaticPartitionMapping)
