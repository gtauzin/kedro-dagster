# mypy: ignore-errors

from __future__ import annotations

from pathlib import Path
from typing import Any

from .project_factory import KedroProjectOptions


def dagster_executors_config() -> dict[str, Any]:
    return {
        "seq": {"in_process": {}},
        "multiproc": {"multiprocess": {"max_concurrent": 2}},
    }


def dagster_loggers_config() -> dict[str, Any]:
    return {
        "console": {
            "log_level": "INFO",
        },
        "file_logger": {
            "log_level": "DEBUG",
            "handlers": [
                {
                    "class": "logging.FileHandler",
                    "filename": "test.log",
                    "level": "DEBUG",
                }
            ],
        },
    }


def dagster_schedules_config() -> dict[str, Any]:
    return {
        "daily": {
            "cron_schedule": "0 6 * * *",
            "execution_timezone": "UTC",
        }
    }


def make_jobs_config(
    pipeline_name: str = "__default__", executor: str = "seq", schedule: str | None = None
) -> dict[str, Any]:
    job: dict[str, Any] = {
        "pipeline": {"pipeline_name": pipeline_name},
        "executor": executor,
    }
    if schedule is not None:
        job["schedule"] = schedule

    return {"default": job}


def pipeline_registry_default() -> str:
    return """
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input_ds"], "intermediate_ds", name="node0", tags=["tag0", "tag1"]),
            node(identity, ["intermediate_ds"], "output_ds", name="node1"),
            node(identity, ["intermediate_ds"], "output2_ds", name="node2", tags=["tag0"]),
            node(identity, ["intermediate_ds"], "output3_ds", name="node3", tags=["tag1", "tag2"]),
            node(identity, ["intermediate_ds"], "output4_ds", name="node4", tags=["tag2"]),
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "pipe": pipeline,
    }
"""


def options_no_dagster_config(env: str) -> KedroProjectOptions:
    """Minimal catalog with no dagster.yml config."""
    catalog = {
        "input_ds": {"type": "MemoryDataset"},
        "intermediate_ds": {"type": "MemoryDataset"},
        "output2_ds": {"type": "MemoryDataset"},
        # Additional outputs referenced by the default test pipeline
        "output3_ds": {"type": "MemoryDataset"},
        "output4_ds": {"type": "MemoryDataset"},
    }

    return KedroProjectOptions(env=env, catalog=catalog, pipeline_registry_py=pipeline_registry_default())


def options_exec_filebacked(env: str) -> KedroProjectOptions:
    """Minimal catalog with a file-backed output2 and a simple in-process executor job."""
    catalog = {
        "input_ds": {"type": "MemoryDataset"},
        "intermediate_ds": {"type": "MemoryDataset"},
        "output2_ds": {
            "type": "pandas.CSVDataset",
            "filepath": "data/08_reporting/output2.csv",
            "save_args": {"index": False},
        },
        # Additional outputs referenced by the default test pipeline
        "output3_ds": {"type": "MemoryDataset"},
        "output4_ds": {"type": "MemoryDataset"},
    }

    dagster = {
        "loggers": dagster_loggers_config(),
        "executors": dagster_executors_config(),
        "schedules": dagster_schedules_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="seq", schedule="daily"),
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def options_partitioned_intermediate_output2(env: str) -> KedroProjectOptions:
    """Partitioned intermediate and output2 with identity partition mapping."""
    catalog = {
        "input_ds": {"type": "MemoryDataset"},
        "intermediate_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/intermediate",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
            "partition_mapping": {"output2_ds": {"type": "IdentityPartitionMapping"}},
        },
        "output2_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/08_reporting/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
        },
        # Additional outputs referenced by the default test pipeline
        "output3_ds": {"type": "MemoryDataset"},
        "output4_ds": {"type": "MemoryDataset"},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def options_partitioned_identity_mapping(env: str) -> KedroProjectOptions:
    """Partition-mapping focused config for tests in test_partition_mappings.py."""
    catalog = {
        "input_ds": {"type": "MemoryDataset"},
        "intermediate_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/intermediate",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
            "partition_mapping": {"output2_ds": {"type": "IdentityPartitionMapping"}},
        },
        "output2_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/08_reporting/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
        },
    }

    return KedroProjectOptions(env=env, catalog=catalog, pipeline_registry_py=pipeline_registry_default())


def options_partitioned_static_mapping(env: str) -> KedroProjectOptions:
    """Partitioned intermediate and output2 with a StaticPartitionMapping.

    Upstream partitions: p1, p2
    Downstream partitions (output2): a, b
    Mapping: p1 -> a, p2 -> b
    """
    catalog = {
        "input_ds": {"type": "MemoryDataset"},
        "intermediate_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/03_primary/intermediate",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["p1", "p2"]},
            "partition_mapping": {
                "output2_ds": {
                    "type": "StaticPartitionMapping",
                    "downstream_partition_keys_by_upstream_partition_key": {
                        "p1": ["a"],
                        "p2": ["b"],
                    },
                }
            },
        },
        "output2_ds": {
            "type": "kedro_dagster.datasets.DagsterPartitionedDataset",
            "path": "data/08_reporting/output2",
            "dataset": {"type": "pandas.CSVDataset", "save_args": {"index": False}},
            "partition": {"type": "StaticPartitionsDefinition", "partition_keys": ["a", "b"]},
        },
        # Additional outputs referenced by the default test pipeline
        "output3_ds": {"type": "MemoryDataset"},
        "output4_ds": {"type": "MemoryDataset"},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def options_hooks_filebacked(
    env: str, input_csv: str | Path, primary_dir: str | Path, output_dir: str | Path
) -> KedroProjectOptions:
    """File-backed datasets for hooks e2e test using tmp paths.

    Datasets: input, intermediate, output, output2, output3, output4 as CSVs pointing to the provided directories.
    Job: default with in-process executor.
    """
    input_csv = str(input_csv)
    primary_dir = Path(primary_dir)
    output_dir = Path(output_dir)

    catalog = {
        "input_ds": {"type": "pandas.CSVDataset", "filepath": input_csv},
        "intermediate_ds": {"type": "pandas.CSVDataset", "filepath": str(primary_dir / "intermediate.csv")},
        "output_ds": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output.csv")},
        "output2_ds": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output2.csv")},
        "output3_ds": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output3.csv")},
        "output4_ds": {"type": "pandas.CSVDataset", "filepath": str(output_dir / "output4.csv")},
    }

    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }

    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_default()
    )


def pipeline_registry_multiple_inputs() -> str:
    return """
from kedro.pipeline import Pipeline, node


def add(a, b):
    return a + b


def identity(x):
    return x


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input_a"], "a_cleaned", name="clean_a"),
            node(identity, ["input_b"], "b_cleaned", name="clean_b"),
            # Multiple inputs consumed by a single node
            node(add, ["a_cleaned", "b_cleaned"], "sum", name="add_ab"),
        ],
        tags="multi_inputs",
    )
    return {"__default__": pipeline, "multi_inputs": pipeline}
"""


def options_multiple_inputs(env: str) -> KedroProjectOptions:
    catalog = {
        "input_a": {"type": "MemoryDataset"},
        "input_b": {"type": "MemoryDataset"},
        "a_cleaned": {"type": "MemoryDataset"},
        "b_cleaned": {"type": "MemoryDataset"},
        "sum": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_multiple_inputs()
    )


def pipeline_registry_multiple_outputs_tuple() -> str:
    return """
from kedro.pipeline import Pipeline, node


def split_even_odd(numbers):
    evens = [n for n in numbers if n % 2 == 0]
    odds = [n for n in numbers if n % 2 != 0]
    return evens, odds


def count(items):
    return len(items)


def register_pipelines():
    pipeline = Pipeline(
        [
            # Multiple outputs via tuple
            node(split_even_odd, "input_numbers", ["even_numbers", "odd_numbers"], name="split"),
            node(count, "even_numbers", "even_count", name="count_even"),
            node(count, "odd_numbers", "odd_count", name="count_odd"),
        ],
        tags="multi_outputs_tuple",
    )
    return {"__default__": pipeline, "multi_outputs_tuple": pipeline}
"""


def options_multiple_outputs_tuple(env: str) -> KedroProjectOptions:
    catalog = {
        "input_numbers": {"type": "MemoryDataset"},
        "even_numbers": {"type": "MemoryDataset"},
        "odd_numbers": {"type": "MemoryDataset"},
        "even_count": {"type": "MemoryDataset"},
        "odd_count": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_multiple_outputs_tuple()
    )


def pipeline_registry_multiple_outputs_dict() -> str:
    return """
from kedro.pipeline import Pipeline, node


def fanout(x):
    # Return a dict matching the mapping in the node outputs
    return {"o1": x, "o2": x * 2}


def add_one(x):
    return x + 1


def register_pipelines():
    pipeline = Pipeline(
        [
            # Multiple outputs via dict mapping -> datasets mapping
            node(fanout, "input_value", {"o1": "value_copy", "o2": "value_double"}, name="fanout"),
            node(add_one, "value_double", "value_double_plus1", name="transform"),
        ],
        tags="multi_outputs_dict",
    )
    return {"__default__": pipeline, "multi_outputs_dict": pipeline}
"""


def options_multiple_outputs_dict(env: str) -> KedroProjectOptions:
    catalog = {
        "input_value": {"type": "MemoryDataset"},
        "value_copy": {"type": "MemoryDataset"},
        "value_double": {"type": "MemoryDataset"},
        "value_double_plus1": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_multiple_outputs_dict()
    )


def pipeline_registry_no_outputs_node() -> str:
    return """
from kedro.pipeline import Pipeline, node


LOGS = []


def sink(x):
    # Pretend side-effect (e.g., logging, external call). Returns None.
    LOGS.append(x)
    return None


def make_message(x):
    return f"value={x}"


def register_pipelines():
    pipeline = Pipeline(
        [
            node(make_message, "input_value", "message", name="prepare"),
            # No outputs: side-effect only node
            node(sink, "message", None, name="sink"),
        ],
        tags="no_outputs",
    )
    return {"__default__": pipeline, "no_outputs": pipeline}
"""


def options_no_outputs_node(env: str) -> KedroProjectOptions:
    catalog = {
        "input_value": {"type": "MemoryDataset"},
        "message": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_no_outputs_node()
    )


def pipeline_registry_nothing_assets() -> str:
    return """
from kedro.pipeline import Pipeline, node


def produce_nothing(x):
    # Emit a Nothing output; function returns an empty mapping
    return {}


def passthrough_with_gate(x, _gate):
    # Gate is a Nothing input; just return the value
    return x


def register_pipelines():
    pipeline = Pipeline(
        [
            node(produce_nothing, ["input_value"], ["start_signal"], name="produce_nothing"),
            node(passthrough_with_gate, ["input_value", "start_signal"], "output_value", name="gated_passthrough"),
        ],
        tags="nothing_assets",
    )
    return {"__default__": pipeline, "nothing_assets": pipeline}
"""


def options_nothing_assets(env: str) -> KedroProjectOptions:
    catalog = {
        "input_value": {"type": "MemoryDataset"},
        # Explicit Nothing dataset used for output and gating input
        "start_signal": {"type": "kedro_dagster.datasets.DagsterNothingDataset"},
        "output_value": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_nothing_assets()
    )


def pipeline_registry_group_name_metadata() -> str:
    """Pipeline with nodes that have datasets with group_name metadata."""
    return """
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input_ds"], "intermediate_ds", name="node0"),
            node(identity, ["intermediate_ds"], "output_custom_group", name="node1"),
            node(identity, ["intermediate_ds"], "output_default_group", name="node2"),
        ]
    )
    return {
        "__default__": pipeline,
        "test_pipeline": pipeline,
    }
"""


def options_group_name_metadata(env: str) -> KedroProjectOptions:
    """Catalog with datasets that have group_name in metadata."""
    catalog = {
        "input_ds": {
            "type": "MemoryDataset",
            "metadata": {
                "group_name": "custom_external_group",
            },
        },
        "intermediate_ds": {"type": "MemoryDataset"},
        "output_custom_group": {
            "type": "MemoryDataset",
            "metadata": {
                "group_name": "custom_output_group",
            },
        },
        "output_default_group": {"type": "MemoryDataset"},
    }
    dagster = {
        "executors": {"seq": {"in_process": {}}},
        "jobs": {"default": {"pipeline": {"pipeline_name": "__default__"}, "executor": "seq"}},
    }
    return KedroProjectOptions(
        env=env, catalog=catalog, dagster=dagster, pipeline_registry_py=pipeline_registry_group_name_metadata()
    )


def pipeline_registry_spaceflights() -> str:
    """Pipeline registry mimicking spaceflights-pandas starter structure."""
    return '''
from kedro.pipeline import Pipeline, node


def preprocess_companies(companies):
    """Preprocess companies data."""
    return companies


def preprocess_shuttles(shuttles):
    """Preprocess shuttles data."""
    return shuttles


def create_model_input_table(preprocessed_companies, preprocessed_shuttles, reviews):
    """Combine preprocessed data into model input table."""
    return {"companies": preprocessed_companies, "shuttles": preprocessed_shuttles}


def split_data(model_input_table, parameters):
    """Split data into training and test sets."""
    return model_input_table, model_input_table, [0], [1]


def train_model(X_train, y_train):
    """Train a regression model."""
    return {"model": "trained"}


def evaluate_model(regressor, X_test, y_test):
    """Evaluate the trained model."""
    return None


def register_pipelines():
    # Data processing pipeline
    data_processing = Pipeline(
        [
            node(
                preprocess_companies,
                inputs="companies",
                outputs="preprocessed_companies",
                name="preprocess_companies_node",
            ),
            node(
                preprocess_shuttles,
                inputs="shuttles",
                outputs="preprocessed_shuttles",
                name="preprocess_shuttles_node",
            ),
            node(
                create_model_input_table,
                inputs=["preprocessed_companies", "preprocessed_shuttles", "reviews"],
                outputs="model_input_table",
                name="create_model_input_table_node",
            ),
        ],
        tags="data_processing",
    )

    # Data science pipeline
    data_science = Pipeline(
        [
            node(
                split_data,
                inputs=["model_input_table", "params:model_options"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_node",
            ),
            node(
                train_model,
                inputs=["X_train", "y_train"],
                outputs="regressor",
                name="train_model_node",
            ),
            node(
                evaluate_model,
                inputs=["regressor", "X_test", "y_test"],
                outputs=None,
                name="evaluate_model_node",
            ),
        ],
        tags="data_science",
    )

    return {
        "__default__": data_processing + data_science,
        "data_processing": data_processing,
        "data_science": data_science,
    }
'''


def options_spaceflights_quickstart(env: str) -> KedroProjectOptions:
    """Scenario mimicking docs/pages/getting-started.md spaceflights quickstart.

    Creates a project structure with:
    - data_processing pipeline (preprocess_companies, preprocess_shuttles, create_model_input_table)
    - data_science pipeline (split_data, train_model, evaluate_model)
    - dagster.yml with jobs: default, parallel_data_processing
    - Schedules, executors, and loggers as documented

    Note: The data_science pipeline job is omitted since it has a cross-pipeline
    dependency on model_input_table (produced by data_processing). The parallel_data_processing
    job demonstrates the node_names filtering feature from the docs.
    """
    catalog = {
        # Raw data inputs
        "companies": {"type": "MemoryDataset"},
        "shuttles": {"type": "MemoryDataset"},
        "reviews": {"type": "MemoryDataset"},
        # Intermediate data
        "preprocessed_companies": {"type": "MemoryDataset"},
        "preprocessed_shuttles": {"type": "MemoryDataset"},
        "model_input_table": {"type": "MemoryDataset"},
        # Data science outputs
        "X_train": {"type": "MemoryDataset"},
        "X_test": {"type": "MemoryDataset"},
        "y_train": {"type": "MemoryDataset"},
        "y_test": {"type": "MemoryDataset"},
        "regressor": {"type": "MemoryDataset"},
    }

    # Configuration matching docs/pages/getting-started.md example
    # (without data_science job which has cross-pipeline dependency)
    dagster = {
        "loggers": {
            "console_logger": {
                "log_level": "INFO",
                "formatters": {
                    "simple": {
                        "format": "[%(asctime)s] %(levelname)s - %(message)s",
                    },
                },
                "handlers": [
                    {
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout",
                        "formatter": "simple",
                    },
                ],
            },
        },
        "schedules": {
            "daily": {
                "cron_schedule": "0 0 * * *",
            },
        },
        "executors": {
            "sequential": {"in_process": {}},
            "multiprocess": {"multiprocess": {"max_concurrent": 2}},
        },
        "jobs": {
            "default": {
                "pipeline": {"pipeline_name": "__default__"},
                "executor": "sequential",
            },
            "parallel_data_processing": {
                "pipeline": {
                    "pipeline_name": "data_processing",
                    "node_names": ["preprocess_companies_node", "preprocess_shuttles_node"],
                },
                "loggers": ["console_logger"],
                "schedule": "daily",
                "executor": "multiprocess",
            },
        },
    }

    parameters = {
        "model_options": {
            "test_size": 0.2,
            "random_state": 42,
        },
    }

    return KedroProjectOptions(
        env=env,
        catalog=catalog,
        dagster=dagster,
        parameters=parameters,
        pipeline_registry_py=pipeline_registry_spaceflights(),
    )
