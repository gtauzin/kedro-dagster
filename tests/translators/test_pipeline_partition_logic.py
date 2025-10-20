from __future__ import annotations

import dagster as dg
import pytest
from kedro.io import DataCatalog, MemoryDataset

from kedro_dagster.pipelines import PipelineTranslator


class DummyContext:
    def __init__(self, catalog: DataCatalog):
        self.catalog = catalog
        # Not used in the methods under test, but present to satisfy initializer
        self._hook_manager = None  # type: ignore[assignment]


class DummyNode:
    def __init__(self, name: str, inputs: list[str], outputs: list[str]):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs


def _make_translator(catalog: DataCatalog, asset_partitions: dict[str, dict] | None = None) -> PipelineTranslator:
    return PipelineTranslator(
        dagster_config={},
        context=DummyContext(catalog),
        project_path="/tmp/project",
        env="local",
        session_id="test-session",
        named_assets={},
        asset_partitions=asset_partitions or {},
        named_op_factories={},
        named_resources={},
        named_executors={},
        enable_mlflow=False,
    )


def test_enumerate_partition_keys_raises_for_multi_partitions():
    catalog = DataCatalog()
    translator = _make_translator(catalog)

    mpd = dg.MultiPartitionsDefinition(
        partitions_defs={
            "date": dg.StaticPartitionsDefinition(["2024-01", "2024-02"]),
            "color": dg.StaticPartitionsDefinition(["red", "blue"]),
        }
    )

    with pytest.raises(NotImplementedError):
        translator._enumerate_partition_keys(mpd)


def test_get_node_partition_keys_raises_on_mixed_outputs():
    # one partitioned output and one non-partitioned non-nothing output -> error
    catalog = DataCatalog(
        datasets={
            "in": MemoryDataset(),
            "out_non_partitioned": MemoryDataset(),
        }
    )

    asset_partitions = {
        # only the partitioned output is declared here
        "out_partitioned": {
            "partitions_def": dg.StaticPartitionsDefinition(["p1", "p2"]),
        }
    }

    translator = _make_translator(catalog, asset_partitions)
    node = DummyNode(
        name="n1",
        inputs=["in"],
        outputs=["out_partitioned", "out_non_partitioned"],
    )

    with pytest.raises(ValueError) as excinfo:
        translator._get_node_partition_keys(node)

    assert "mixed partitioned and non-partitioned" in str(excinfo.value)


def test_get_node_partition_keys_identity_mapping():
    # input and output both partitioned with same keys -> identity mapping
    catalog = DataCatalog(datasets={"in": MemoryDataset(), "out": MemoryDataset()})
    partitions_def = dg.StaticPartitionsDefinition(["2024-01", "2024-02"])

    asset_partitions = {
        "in": {"partitions_def": partitions_def, "partition_mappings": {}},
        "out": {"partitions_def": partitions_def, "partition_mappings": {}},
    }

    translator = _make_translator(catalog, asset_partitions)
    node = DummyNode(name="n1", inputs=["in"], outputs=["out"])

    mapping = translator._get_node_partition_keys(node)

    # Expect mapping entries like "in|2024-01" -> "out|2024-01"
    assert mapping == {
        "in|2024-01": "out|2024-01",
        "in|2024-02": "out|2024-02",
    }


def test_get_node_partition_keys_all_partition_mapping_downstream_selection():
    # AllPartitionMapping should still return a single mapped downstream key
    catalog = DataCatalog(datasets={"in": MemoryDataset(), "out": MemoryDataset()})
    in_def = dg.StaticPartitionsDefinition(["a", "b"])
    out_def = dg.StaticPartitionsDefinition(["x", "y"])

    # Configure an explicit AllPartitionMapping for downstream "out"
    asset_partitions = {
        "in": {
            "partitions_def": in_def,
            "partition_mappings": {"out": dg.AllPartitionMapping()},
        },
        "out": {"partitions_def": out_def, "partition_mappings": {}},
    }

    translator = _make_translator(catalog, asset_partitions)
    node = DummyNode(name="n1", inputs=["in"], outputs=["out"])

    mapping = translator._get_node_partition_keys(node)
    # Our implementation currently takes the first mapped downstream key when multiple are returned
    # so assert that at least the keys exist and downstream keys are among out_def keys.
    assert set(mapping.keys()) == {"in|a", "in|b"}
    assert all(m in {"out|x", "out|y"} for m in mapping.values())
