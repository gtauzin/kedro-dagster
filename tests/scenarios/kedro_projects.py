# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path

from ..helpers import dagster_executors_config, dagster_schedules_config, make_jobs_config
from .project_factory import KedroProjectOptions


def options_exec_filebacked(env: str) -> KedroProjectOptions:
    """Scenario: Minimal catalog with a file-backed output2 and a simple in-process executor job."""
    catalog = {
        "input_dataset": {"type": "MemoryDataset"},
        "intermediate": {"type": "MemoryDataset"},
        "output2": {
            "type": "pandas.CSVDataset",
            "filepath": "data/08_reporting/output2.csv",
            "save_args": {"index": False},
        },
        # Additional outputs referenced by the default test pipeline
        "output3": {"type": "MemoryDataset"},
        "output4": {"type": "MemoryDataset"},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def options_partitioned_intermediate_output2(env: str) -> KedroProjectOptions:
    """Scenario: Partitioned intermediate and output2 with identity partition mapping."""
    catalog = {
        "input_dataset": {"type": "MemoryDataset"},
        "intermediate": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/intermediate",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
            "partition_mapping": {"output2": {"type": "IdentityPartitionMapping"}},
        },
        "output2": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/08_reporting/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
        },
        # Additional outputs referenced by the default test pipeline
        "output3": {"type": "MemoryDataset"},
        "output4": {"type": "MemoryDataset"},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def options_integration_full(env: str) -> KedroProjectOptions:
    """Scenario: Integration-style project with executors, schedules, jobs and file-backed output2."""
    catalog = {
        "input_dataset": {"type": "MemoryDataset"},
        "intermediate": {"type": "MemoryDataset"},
        "output_dataset": {"type": "MemoryDataset"},
        "output2": {
            "type": "pandas.CSVDataset",
            "filepath": "data/08_reporting/output2.csv",
            "save_args": {"index": False},
        },
        "output3": {"type": "MemoryDataset"},
        "output4": {"type": "MemoryDataset"},
    }

    dagster = {
        "executors": dagster_executors_config(),
        "schedules": dagster_schedules_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="inproc", schedule="daily"),
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def pipeline_registry_default() -> str:
    return """
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input_dataset"], "intermediate", name="node0", tags=["tag0", "tag1"]),
            node(identity, ["intermediate"], "output_dataset", name="node1"),
            node(identity, ["intermediate"], "output2", name="node2", tags=["tag0"]),
            node(identity, ["intermediate"], "output3", name="node3", tags=["tag1", "tag2"]),
            node(identity, ["intermediate"], "output4", name="node4", tags=["tag2"]),
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "ds": pipeline,
    }
"""


def options_with_custom_pipeline(env: str, pipeline_registry_py: str | None = None, **kwargs) -> KedroProjectOptions:
    """Generic scenario that also injects a custom pipeline registry source.

    kwargs are forwarded into KedroProjectOptions (e.g., catalog=..., dagster=...).
    If pipeline_registry_py is None, uses the default registry defined above.
    """
    if pipeline_registry_py is None:
        pipeline_registry_py = pipeline_registry_default()
    return KedroProjectOptions(env=env, pipeline_registry_py=pipeline_registry_py, **kwargs)


def options_partitioned_identity_mapping(env: str) -> KedroProjectOptions:
    """Scenario: Partition-mapping focused config for tests in test_partition_mappings.py."""
    catalog = {
        "input_dataset": {"type": "MemoryDataset"},
        "intermediate": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/intermediate",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
            "partition_mapping": {"output2": {"type": "IdentityPartitionMapping"}},
        },
        "output2": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/08_reporting/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
        },
    }

    return KedroProjectOptions(env=env, catalog=catalog, pipeline_registry_py=pipeline_registry_default())


def options_hooks_filebacked(
    env: str, input_csv: str | Path, primary_dir: str | Path, output_dir: str | Path
) -> KedroProjectOptions:
    """Scenario: File-backed datasets for hooks e2e test using tmp paths.

    Datasets: input, intermediate, output, output2, output3, output4 as CSVs pointing to the provided directories.
    Job: default with in-process executor.
    """
    input_csv = str(input_csv)
    primary_dir = Path(primary_dir)
    output_dir = Path(output_dir)

    catalog = {
        "input_dataset": {"type": "pandas.CSVDataset", "filepath": input_csv},
        "intermediate": {"type": "pandas.CSVDataset", "filepath": str(primary_dir / "intermediate.csv")},
        "output_dataset": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output.csv")},
        "output2": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output2.csv")},
        "output3": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output3.csv")},
        "output4": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output4.csv")},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )
