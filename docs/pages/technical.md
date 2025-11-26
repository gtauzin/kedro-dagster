# Technical documentation

This section provides an in-depth look at the architecture, configuration, and core concepts behind Kedro-Dagster. Here you'll find details on how Kedro projects are mapped to Dagster constructs, how to configure orchestration, and how to customize the integration for advanced use cases.

## How the translation works

Kedro-Dagster reads your Kedro project and the configuration under `conf/<ENV>/` to generate a Dagster code location. The selected environment determines which `catalog.yml` and `dagster.yml` are loaded. Translators then build Dagster assets and IO managers from the Kedro catalog, map nodes to ops and multi-assets, and construct jobs by filtering pipelines according to `dagster.yml`. All generated objects are registered in a single `dagster.Definitions` instance exposed by the Kedro-Dagster's generated `definitions.py`.

For a walkthrough with concrete examples, see the [example page](example.md).

## Kedro-Dagster concept mapping

Kedro-Dagster translates core Kedro concepts into their Dagster equivalents. Understanding this mapping helps you reason about how your Kedro project appears and behaves in Dagster.

| Kedro Concept   | Dagster Concept      | Description |
|-----------------|----------------------|-------------|
| **Node**        | Op,&nbsp;Asset            | Each [Kedro node](https://docs.kedro.org/en/stable/nodes_and_pipelines/nodes.html) becomes a Dagster op. Node parameters are passed as config. |
| **Pipeline**    | Job                  | [Kedro pipelines](https://docs.kedro.org/en/stable/nodes_and_pipelines/pipeline_introduction.html) are filtered and translated into a Dagster job. Jobs can be scheduled and can target executors. |
| **Dataset**     | Asset,&nbsp;IO&nbsp;Manager    | Each [Kedro data catalog](https://docs.kedro.org/en/stable/data/data_catalog.html)'s dataset become Dagster assets managed by a dedicated IO managers. |
| **Hooks**       | Hooks,&nbsp;Sensors       | [Kedro hooks](https://docs.kedro.org/en/stable/hooks/index.html#hooks) are executed at the appropriate points in the Dagster job lifecycle. |
| **Parameters**  | Config,&nbsp;Resources    | [Kedro parameters](https://docs.kedro.org/en/stable/configuration/parameters.html) are passed as Dagster config. |
| **Logging**     | Logger               | [Kedro logging](https://docs.kedro.org/en/stable/logging/index.html) is integrated with Dagster's logging system. |

Additionally, we provide Kedro datasets, namely `DagsterPartitionedDataset` and `DagsterNothingDataset`, to enable [Dagster partitions](https://docs.dagster.io/guides/build/partitions-and-backfills).

### Catalog

Kedro-Dagster translates Kedro datasets into Dagster assets and IO managers. This allows you to use Kedro's [Data Catalog](https://docs.kedro.org/en/stable/data/data_catalog.html) with Dagster's asset materialization and IO management features.

For the Kedro pipelines specified in `dagster.yml`, the following Dagster objects are defined:

- **External assets**: Input datasets to the pipelines are registered as Dagster external assets.
- **Assets**: Output datasets to the pipelines are defined as Dagster assets
- **IO Managers**: Custom Dagster IO managers are created for each dataset involved in the deployed pipelines mapping both their save and load functions.

See the API reference for [`CatalogTranslator`](reference.md#catalogtranslator) for more details.

!!! note
    Each Kedro dataset can take a `metadata` parameter to define additional metadata for the corresponding Dagster asset, such as a description. This description will appear in the Dagster UI.

### Group name metadata

In addition to common metadata like `description`, a Kedro dataset's `metadata` can include a `group_name` field. When present, `group_name` is used to set the asset's group in Dagster and overrides the default group inferred from the node's pipeline. For multi-asset translations, `group_name` is applied per-AssetOut, allowing each asset produced by a multi-asset to belong to its own group when needed.

### Node

Kedro nodes are translated into Dagster ops and assets. Each node becomes a Dagster op, and, additionally, nodes that return outputs are mapped to Dagster multi-assets.

For the Kedro pipelines specified in `dagster.yml`, the following Dagster objects are defined:

- **Ops**: Each Kedro node within the pipelines is mapped to a Dagster op.
- **Assets**: Kedro nodes that return output datasets are registered as Dagster multi-assets.
- **Parameters**: Node parameters are passed as Dagster config to enable them to be modified in a Dagster run launchpad.

See the API reference for [`NodeTranslator`](reference.md#nodetranslator) for more details.

### Pipeline

Kedro pipelines are translated into Dagster jobs. Each job can be filtered, scheduled, and assigned an executor via configuration.

- **Jobs**: Each pipeline is mapped to a Dagster job.
- **Filtering**: Jobs are defined granuarily from Kedro pipelines by allowing the filtering of their nodes, namespaces, tags, and inputs/outputs.

If one of the datasets involved in the pipeline is a `DagsterPartitionedDataset`, the corresponding job will fan-out the nodes the partitioned datasets are involved in according to the defined partitions.

See the API reference for [`PipelineTranslator`](reference.md#pipelinetranslator) for more details.

### Hook

Kedro-Dagster preserves all [Kedro hooks](https://docs.kedro.org/en/stable/hooks/index.html#hooks) in the Dagster context. Hooks are executed at the appropriate points in the Dagster job lifecycle. Catalog hooks are called in the `handle_output` and `load_input` function of each Dagster IO manager. Node hooks are plugged in the appropriate Dagster Op. As for the Context hook, they are called within a Dagster Op running at the beginning of each job along with the `before_pipeline_run` pipeline hook. The `after_pipeline_run` is called in a Dagster op running at the end of each job. Finally the `on_pipeline_error` pipeline, is embedded in a dedicated Dagster sensor that is triggered by a run failure.

## Compatibility notes between Kedro and Dagster

### Naming conventions

Dagster enforces strong constraints for asset, op, and job names  as they must match the regex `^[A-Za-z0-9_]+$`. As those Dagster objects are created directly from Kedro datasets, nodes, and pipelines, Kedro-Dagster applies a small set of deterministic transformations so Kedro names map predictably to Dagster names:

- **Datasets**: only the dot "." namespace separator is converted to a double underscore "__" when mapping a Kedro dataset name to a Dagster-friendly identifier. Example: `my.dataset.name` -> `my__dataset__name`. Other characters (for example, hyphens `-`) are preserved by the formatter. Internally Kedro-Dagster will get back the dataset name from the asset name by replacing double underscored by dots.
- **Nodes**: dots are replaced with double underscores to keep namespaces (`my.node` -> `my__node`). If the resulting node name still contains disallowed characters (anything outside A–Z, a–z, 0–9 and underscore), the node name is replaced with a stable hashed placeholder of the form `unnamed_node_<md5>` to ensure it meets Dagster's constraints.

These rules are implemented in `src/kedro_dagster/utils.py` by `format_dataset_name`, `format_node_name`, and `unformat_asset_name` and are intentionally minimal and deterministic so names remain readable while complying with Dagster's requirements.

### Logging

Any configuration of Dagster loggers should not be directly attempted as Dagster CLI and API will override them. Therefore, one cannot use Kedro's `logging.yml` configuration file to configure `dagster` loggers. In place, Kedro-Dagster provides ways to integrate Dagster logging into Kedro projects.

#### CLI

It is possible to run Dagster CLI commands from within a Kedro project using the `kedro dagster <dg command>` wrapper. This ensures that the Kedro environment is properly set up when running Dagster commands. Those commands accept the `--log-level` and `--log-format` options to configure logging, where `--log-format` supports `colored`, `json`, and `rich`.

!!! note
    The `rich` formatter is not based on the `rich` library like the Kedro logging handler of the same name. It should be understood as a simple formatter with enhanced readability.

To ensure homogeneity in log formatting between Kedro, Kedro-Dagster, Dagster, and any third-party libraries used, Kedro-Dagster provides implementations of the Dagster formatters. They can be used directly in the Kedro's `logging.yml` configuration file as follows:

```yaml

formatters:
  simple:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

  colored:
    "()": kedro_dagster.dagster_colored_formatter

  json:
    "()": kedro_dagster.dagster_json_formatter

  rich:
    "()": kedro_dagster.dagster_rich_formatter

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    stream: ext://sys.stdout
    formatter: colored

loggers:
  kedro:
    level: INFO

  kedro_dagster:
    level: INFO

root:
  handlers: [console]
```

#### In-code logging

##### Overview

Kedro-Dagster integrates Kedro's logging with Dagster's logging system to provide unified log visibility. Logs generated within your Kedro node functions can be captured and displayed in the Dagster UI when you use the `kedro_dagster.logging` module.

**How it works:**

The `kedro_dagster.logging.getLogger` function automatically detects the execution context:

- During Dagster runs: Returns a Dagster logger (logs appear in Dagster UI)
- During Kedro runs: Returns a standard Python logger (logs appear in terminal)

This allows the same node code to work seamlessly in both Kedro and Dagster contexts.

##### Using kedro_dagster.logging in node functions

Logs generated within Kedro nodes are captured if `getLogger` is imported from the `kedro_dagster.logging` module instead of the `logging` package and the `getLogger` calls are made inside the node functions. These logs are then displayed in the Dagster UI, allowing for easier tracing and debugging of pipeline executions.

**Example:**

```python

def process_data(data: pd.DataFrame) -> pd.DataFrame:
    from kedro_dagster.logging import getLogger
    logger = getLogger(__name__)
    logger.info(f"Processing {len(data)} rows")

    # Your processing logic
    processed = data.dropna()

    logger.info(f"After processing: {len(processed)} rows")
    return processed
```

!!! tip "Logger Creation"
    Always create the logger **inside** the node function, not at module level. This ensures the logger is properly initialized in the Dagster execution context.

#####  Custom logger configuration

Additionally, to configure logging within Dagster runs, use the Kedro-Dagster `loggers` section of the `dagster.yml` configuration file to define and customize loggers for your Dagster runs:

```yaml
loggers:
  console_logger: # Logger name
    log_level: INFO
    handlers:
      - class: logging.StreamHandler
        level: INFO
        stream: ext://sys.stdout
        formatter: simple
    formatters:
      simple:
        format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

jobs:
  my_job:
    pipeline:
      pipeline_name: __default__
    executor: sequential
    loggers: [console_logger]
```

##### Custom handlers, formatters, and filters

You can define custom formatters and filters using either the `()` callable syntax or the `class` key:

```yaml
loggers:
  advanced_logger:
    log_level: DEBUG
    formatters:
      custom_formatter:
        "()": my_package.formatters.CustomFormatter
        prefix: "CUSTOM"
        format: "%(message)s"
      class_formatter:
        class: my_package.formatters.AnotherFormatter
        custom_param: "value"
    filters:
      custom_filter:
        "()": my_package.filters.CustomFilter
        keyword: "important"
    handlers:
      - class: logging.StreamHandler
        level: DEBUG
        formatter: custom_formatter
        filters: [custom_filter]
```

See the [Example page](example.md#custom-logging-integration) for a complete example of configuring logging in Kedro-Dagster.

## MLflow integration

Kedro-Dagster provides seamless integration with MLflow when using [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow) for experiment tracking. When MLflow is configured in your Kedro project, Kedro-Dagster automatically captures MLflow run information and displays it in the Dagster UI.

### Automatic MLflow run tracking

When a Kedro node executes within a Dagster context and the active MLflow run triggered by the Kedro-MLflow hook is detected, Kedro-Dagster automatically:

1. **Captures run metadata**: Extracts experiment ID, run ID, and tracking URI from the active MLflow run
2. **Generates run URLs**: Creates clickable links to view the MLflow run in the MLflow UI
3. **Logs to Dagster**: Records MLflow run information in Dagster logs for easy access and debugging

Those details appear in the Dagster run logs, run tags, and asset materialization metadata, allowing users to quickly navigate between Dagster runs and their corresponding MLflow experiments.

### Configuration requirements

To enable MLflow integration:

1. **Install kedro-mlflow**: See the [Kedro-MLflow installation instructions](https://kedro-mlflow.readthedocs.io/en/latest/source/02_getting_started/01_installation/01_installation.html)
2. **Configure MLflow** in your Kedro project following the [kedro-mlflow documentation](https://kedro-mlflow.readthedocs.io/)
3. **MLflow UI configuration**: For example, define UI host and port or tracking URI in `conf/<env>/mlflow.yml`:

```yaml
ui:
  host: localhost
  port: 5000

server:
  mlflow_tracking_uri: mlruns
```

## Kedro datasets for Dagster partitioning

Kedro-Dagster provides two custom datasets to enable Dagster partitioning and asset management within Kedro projects:

- **`DagsterPartitionedDataset`**: A Kedro dataset that is partitioned according to Dagster's partitioning scheme. This allows for more efficient data processing and management within Kedro pipelines.
- **`DagsterNothingDataset`**: A special Kedro dataset that represents a "no-op" or empty dataset in Dagster. This can be useful for cases where an order in execution between two nodes needs to be enforced.

!!! warning "Experimental Status"
    Dagster partitions support in Kedro-Dagster is **experimental** and currently supports only a limited subset of Dagster's partition types. The plugin validates partition definitions and mappings at dataset instantiation and will raise clear errors for unsupported types.

### Supported partition definitions and mappings

Currently, only **`StaticPartitionsDefinition`** is supported:

- Defines a fixed list of partition keys
- Ideal for finite sets of partitions (e.g., specific dates, regions, model variants)
- See [Dagster StaticPartitionsDefinition docs](https://docs.dagster.io/api/dagster/partitions#dagster.StaticPartitionsDefinition)

Example:

```yaml
my_dataset:
  type: kedro_dagster.DagsterPartitionedDataset
  partition:
    type: dagster.StaticPartitionsDefinition
    partition_keys: ["2023-01-01", "2023-01-02", "2023-01-03"]
```

Two partition mapping types are supported:

1. **`StaticPartitionMapping`**: Explicitly maps upstream partition keys to downstream partition keys
   - Full control over partition relationships
   - Example: Map 3 upstream partitions to 10 downstream partitions

2. **`IdentityPartitionMapping`**: One-to-one mapping where partition keys match exactly
   - Simplest mapping when upstream and downstream use the same partition keys
   - No explicit configuration needed

Example with `StaticPartitionMapping`:

```yaml
upstream_dataset:
  type: kedro_dagster.DagsterPartitionedDataset
  partition:
    type: dagster.StaticPartitionsDefinition
    partition_keys: ["1.csv", "2.csv", "3.csv"]
  partition_mappings:
    downstream_dataset:
      type: dagster.StaticPartitionMapping
      downstream_partition_keys_by_upstream_partition_key:
        1.csv: 10.csv
        2.csv: 20.csv
        3.csv: 30.csv
```

Example with `IdentityPartitionMapping`:

```yaml
upstream_dataset:
  type: kedro_dagster.DagsterPartitionedDataset
  partition:
    type: dagster.StaticPartitionsDefinition
    partition_keys: ["A", "B", "C"]
  partition_mappings:
    downstream_dataset:
      type: dagster.IdentityPartitionMapping  # Keys match exactly
```

### Unsupported Partition Types

The following Dagster partition types are **not supported** and will raise validation errors:

**Partition Definitions**:

- ❌ `TimeWindowPartitionsDefinition` - Time-based partitions (daily, hourly, etc.)
- ❌ `DynamicPartitionsDefinition` - Partitions that can be added/removed at runtime
- ❌ `MultiPartitionsDefinition` - Composite partitions with multiple dimensions

**Partition Mappings**:

- ❌ `TimeWindowPartitionMapping` - Maps time-based partitions
- ❌ `DimensionPartitionMapping` - Maps multi-partition dimensions
- ❌ `AllPartitionMapping` - Maps all partitions from upstream
- ❌ `LastPartitionMapping` - Maps to the most recent partition
- ❌ Any other custom partition mapping types

!!! info "Need Support for These Features?"
    If you have a use case that requires any of these unsupported partition types, please [open an issue](https://github.com/gtauzin/kedro-dagster/issues) describing your requirements. Community feedback helps prioritize which features to add in future releases.

### `DagsterPartitionedDataset`

This dataset wraps Kedro’s `PartitionedDataset` to enable Dagster partitioning and optional partition mappings to downstream datasets. When a job includes a `DagsterPartitionedDataset`, Dagster will schedule and materialize per-partition runs; you can select keys in the Launchpad or use backfills for ranges.

#### Example Usage

A `DagsterPartitionedDataset` can be defined in your Kedro data catalog as follows:

```yaml
my_downstream_partitioned_dataset:
  type: kedro_dagster.datasets.DagsterPartitionedDataset
  path: data/01_raw/my_data/
  dataset: # Underlying Kedro PartitionedDataset configuration
    type: pandas.CSVDataSet
  partition: dagster.StaticPartitionsDefinition # Define Dagster partitions
    partitions:
      - 2023-01-01.csv
      - 2023-01-02.csv
      - 2023-01-03.csv
```

To define a partition mapping to downstream datasets, you can use the `partition_mappings` parameter:

```yaml
my_upstream_partitioned_dataset:
  type: kedro_dagster.datasets.DagsterPartitionedDataset
  partition: dagster.StaticPartitionsDefinition
    partitions:
      - 2023-01-01.csv
      - 2023-01-02.csv
      - 2023-01-03.csv
  partition_mappings:
    my_downstream_partitioned_dataset: # Map to downstream dataset
      type: dagster.StaticPartitionMapping
      downstream_partition_keys_by_upstream_partition_key:
        1.csv: 2023-01-01.csv
        2.csv: 2023-01-02.csv
        3.csv: 2023-01-03.csv
```

The dataset mapped to in `partition_mappings` can also be refered to using a pattern with the `{}` syntax:

```yaml
my_upstream_partitioned_dataset:
  ...
  partition_mappings:
    {namespace}.partitioned_dataset: # Map to downstream dataset
      type: dagster.StaticPartitionMapping
  ...
```

!!! note
    The `partition` and `partition_mapping` parameters expect Dagster partition definitions and mappings. Refer to the [Dagster Partitions documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions) for more details on available partition types and mappings.

See the API reference for [`DagsterPartitionedDataset`](reference.md#dagsterpartitioneddataset) for more details.

### `DagsterNothingDataset`

A dummy dataset representing a Dagster asset of type `Nothing` without associated data used to enforce links between nodes. It does not read or write any data but allows you to create dependencies between nodes in your Kedro pipelines that translate to Dagster assets of type `Nothing`.

#### Example usage

It is straightforward to define a `DagsterNothingDataset` in your Kedro data catalog as follows:

```yaml
my_nothing_dataset:
  type: kedro_dagster.datasets.DagsterNothingDataset
  metadata:
      description: "Nothing dataset."
```

See the API reference for [`DagsterNothingDataset`](reference.md#dagsternothingdataset) for more details.

### When to use Dagster partitions

Use `DagsterPartitionedDataset` when:

- You have a **finite, predefined set** of partitions (e.g., list of regions, model variants, specific date ranges)
- You want to leverage Dagster UI's partition selection and backfill features
- Your partition keys are static and won't change frequently

### Future roadmap

Support for additional partition types may be added in future releases. Track progress and request features at:

- [Kedro-Dagster Issue Tracker](https://github.com/gtauzin/kedro-dagster/issues)

If you have a use case requiring specific partition types, please open a feature request with your requirements.

## Project configuration

Kedro-Dagster expects a standard [Kedro project structure](https://docs.kedro.org/en/stable/get_started/kedro_concepts.html#kedro-project-directory-structure). The main configuration file for Dagster integration is `dagster.yml`, located in your Kedro project's `conf/<ENV_NAME>/` directory.

### dagster.yml

This YAML file defines jobs, executors, and schedules for your project.

!!! example

  ```yaml
  schedules:
    my_job_schedule: # Name of the schedule
      cron_schedule: "0 0 * * *" # Parameterst of the schedule

  executors:
    my_executor: # Name of the executor
      multiprocess: # Parameters of the executor
        max_concurrent: 2

  jobs:
    my_job: # Name of the job
      pipeline: # Parameters of its corresponding pipeline
        pipeline_name: __default__
        node_namespace: my_namespace
      executor: my_executor
      schedule: my_job_schedule
  ```

- **jobs**: Map [Kedro pipelines](https://docs.kedro.org/en/stable/nodes_and_pipelines/pipeline_introduction.html) to Dagster jobs, with optional [filtering](https://docs.kedro.org/en/stable/api/kedro.pipeline.Pipeline.html#kedro.pipeline.Pipeline.filter).
- **executors**: Define how jobs are executed (in-process, multiprocess, k8s, etc) by picking executors from those [implemented in Dagster](https://docs.dagster.io/guides/operate/run-executors#example-executors).
- **schedules**: Set up cron-based or custom schedules for jobs.

#### Customizing schedules

You can define multiple schedules for your jobs using cron syntax. See the [Dagster scheduling documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) and the [API Reference](reference.md#scheduleoptions) for more details.

#### Customizing executors

Kedro-Dagster supports several executor types for running your jobs, such as in-process, multiprocess, Dask, Docker, Celery, and Kubernetes. You can customize executor options in your `dagster.yml` file under the `executors` section.

For each [available Dagster executor](https://docs.dagster.io/guides/operate/run-executors#example-executors), there is a corresponding configuration Pydantic model documented in the [API reference](reference.md#executoroptions).

##### Example: Custom multiprocess executor

You can select `multiprocess` as the executor type corresponding to the [multiprocess Dagster executor](https://docs.dagster.io/api/dagster/execution#dagster.multiprocess_executor) and configure it according to the [MultiprocessExecutorOptions](reference.md#multiprocessexecutoroptions).

```yaml
executors:
  my_multiprocess_executor:
    multiprocess:
      max_concurrent: 4
```

##### Example: Custom Docker executor

Similarly, we can configure a [Docker Dagster executor](https://docs.dagster.io/api/libraries/dagster-docker#dagster_docker.docker_executor) with the available parameters defined in [`DockerExecutorOptions`](reference.md#dockerexecutoroptions).

```yaml
executors:
  my_docker_executor:
    docker_executor:
      image: my-custom-image:latest
      registry: "my_registry.com"
      network: "my_network"
      networks: ["my_network_1", "my_network_2"]
      container_kwargs:
        volumes:
          - "/host/path:/container/path"
        environment:
          - "ENV_VAR=value"
```

!!! note
  The `docker_executor` requires the `dagster-docker` package.

#### Customizing jobs

You can filter which nodes, tags, or inputs/outputs are included in each job. Each job can be associated with a pre-defined executor and/or schedule. See the [Kedro pipeline documentation](https://docs.kedro.org/en/stable/api/kedro.pipeline.Pipeline.html#kedro.pipeline.Pipeline.filter) for more on pipelines and filtering. The accepted pipeline parameters are documented in the associated Pydantic model, [`PipelineOptions`](reference.md#pipelineoptions).

To each job, you can assign a schedule and/or an executor by name if it was previously defined in the configuration file.

### definitions.py

The `definitions.py` file is auto-generated by the plugin and serves as the main entry point for Dagster to discover all translated Kedro objects. It contains the Dagster [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object, which registers all jobs, assets, resources, schedules, and sensors derived from your Kedro project.

In most cases, you should not manually edit `definitions.py`; instead, update your Kedro project or `dagster.yml` configuration.

---

## Next steps

- **Getting started:** Follow the [step-by-step tutorial](getting-started.md) to set up Kedro-Dagster in your project.
- **Advanced example:** See the [example documentation](example.md) for a real-world use case.
- **API reference:** Explore the [API reference](reference.md) for details on available classes, functions, and configuration options.
- **External documentation:** For more on Kedro concepts, see the [Kedro documentation](https://kedro.readthedocs.io/en/stable/). For Dagster concepts, see the [Dagster documentation](https://docs.dagster.io/).
