# Reference

This section provides an overview of the Kedro-Dagster CLI and API documentation.

## Kedro-Dagster CLI

Kedro-Dagster provides CLI commands to initialize and run the translation of your Kedro project into Dagster.

### `kedro dagster init`

Initializes Dagster integration for your Kedro project by generating the necessary `definitions.py` and configuration files.

::: kedro_dagster.cli.init

---

### `kedro dagster dev`

Starts the Dagster development UI and launches your Kedro pipelines as Dagster jobs for interactive development and monitoring.

::: kedro_dagster.cli.dev

if Dagster version >= 1.10.6, the command maps to the more recente `dg dev` Dagster CLI command.

---

### CLI wrappers for Dagster `dg` commands

Kedro-Dagster also provides wrappers around all [Dagster `dg`](https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference) CLI commands to facilitate working within a Kedro project context. These commands are extended to include an `--env, -e` option to automatically load the Kedro project and the configuration associated with the specified Kedro environment.

!!! danger
    These commands relies on passing down the `KEDRO_ENV` environment variable based on the passed `--env` option to the child Dagster CLI process. Dagster CLI might override this environment variable if it is defined in `.env` or other environment management tools. Make sure to avoid conflicts when using these commands.

**Examples:**

This command lists all Dagster assets, jobs, sensors, and resources generated from the Kedro pipelines in the project, using the `local` Kedro environment for configuration:

```bash
kedro dagster list defs --env=local
```

This command launches an asset jobs to materialized the specified assets defined in the 'local' Kedro environment:

```bash
kedro dagster launch --assets local/price_predictor/preprocessed_shuttles --env=local
```

## Kedro-Dagster API

### Configuration

The following classes define the configuration schema for Kedro-Dagster's `dagster.yml`, using Pydantic models.

#### `KedroDagsterConfig`

Main configuration class for Kedro-Dagster, representing the structure of the `dagster.yml` file.

::: kedro_dagster.config.kedro_dagster.KedroDagsterConfig

---

#### `JobOptions`

Configuration options for a Dagster job, including pipeline filtering, executor, and schedule.

::: kedro_dagster.config.job.JobOptions

---

#### `PipelineOptions`

Options for filtering and configuring Kedro pipelines by name, namespaces, tags, or inputs/outputs to define jobs.

::: kedro_dagster.config.job.PipelineOptions

---

#### `ExecutorOptions`

Base class for executor configuration. See specific executor option classes below.

::: kedro_dagster.config.execution.ExecutorOptions

---

##### `InProcessExecutorOptions`

Options for the in-process executor.

::: kedro_dagster.config.execution.InProcessExecutorOptions

---

##### `MultiprocessExecutorOptions`

Options for the multiprocess executor.

::: kedro_dagster.config.execution.MultiprocessExecutorOptions

---

##### `DaskExecutorOptions`

Options for the Dask executor.

::: kedro_dagster.config.execution.DaskExecutorOptions

where `DaskClusterConfig` is defined as:

::: kedro_dagster.config.execution.DaskClusterConfig

---

##### `DockerExecutorOptions`

Options for the Docker-based executor.

::: kedro_dagster.config.execution.DockerExecutorOptions

---

##### `CeleryExecutorOptions`

Options for the Celery-based executor.

::: kedro_dagster.config.execution.CeleryExecutorOptions

---

##### `CeleryDockerExecutorOptions`

Options for the Celery executor with Docker support.

::: kedro_dagster.config.execution.CeleryDockerExecutorOptions

---

##### `K8sJobExecutorOptions`

Options for the Kubernetes-based executor.

::: kedro_dagster.config.execution.K8sJobExecutorOptions

where `K8sJobConfig` is defined as:

::: kedro_dagster.config.execution.K8sJobConfig

---

##### `CeleryK8sJobExecutorOptions`

Options for the Celery executor with Kubernetes support.

::: kedro_dagster.config.execution.CeleryK8sJobExecutorOptions

where `K8sJobConfig` is defined as:

::: kedro_dagster.config.execution.K8sJobConfig

---

#### `ScheduleOptions`

Options for defining Dagster schedules.

::: kedro_dagster.config.automation.ScheduleOptions

---

#### `LoggerOptions`

Options for defining Dagster loggers.

::: kedro_dagster.config.logging.LoggerOptions

---

### Translation modules

The following classes are responsible for translating Kedro concepts into Dagster constructs:

#### `KedroProjectTranslator`

Translates an entire Kedro project into a Dagster code location, orchestrating the translation of pipelines, datasets, hooks, and loggers.

::: kedro_dagster.translator.KedroProjectTranslator

---

#### `DagsterCodeLocation`

Collects the Dagster job, asset, resource, executor, schedule, sensor, and loggers definitions generated for the Kedro project-based Dagster code location.

::: kedro_dagster.translator.DagsterCodeLocation

---

#### `CatalogTranslator`

Translates Kedro datasets into Dagster IO managers and assets, enabling seamless data handling between Kedro and Dagster.

::: kedro_dagster.catalog.CatalogTranslator

---

#### `NodeTranslator`

Converts Kedro nodes into Dagster ops and assets, handling Kedro parameter passing.

::: kedro_dagster.nodes.NodeTranslator

---

#### `PipelineTranslator`

Maps Kedro pipelines to Dagster jobs, supporting pipeline filtering, hooks, job configuration, and resource assignment.

::: kedro_dagster.pipelines.PipelineTranslator

---

#### `KedroRunTranslator`

Manages translation of Kedro run parameters and hooks into Dagster resources and sensors, including error handling and context propagation.

::: kedro_dagster.kedro.KedroRunTranslator

---

#### `ExecutorCreator`

Creates Dagster executors from configuration, allowing for granular execution strategies.

::: kedro_dagster.dagster.ExecutorCreator

---

#### `LoggerCreator`

Translates Kedro loggers to Dagster loggers for unified logging across both frameworks.

::: kedro_dagster.dagster.LoggerCreator

---

#### `ScheduleCreator`

Generates Dagster schedules from configuration, enabling automated pipeline execution.

::: kedro_dagster.dagster.ScheduleCreator

---

### Datasets

The following classes define custom Kedro-Dagster datasets for enabling Dagster partitioning and asset management within Kedro projects.

#### `DagsterPartitionedDataset`

Works as a wrapper around Kedro's `PartitionedDataset` to enable Dagster partitioning capabilities.

`catalog.yml` example snippet:

```yaml
my_partitioned_table:
  type: kedro_dagster.DagsterPartitionedDataset
  path: data/02_intermediate/<env>/tables/my_table/
  dataset:
    type: pandas.CSVDataset
  partition:
    type: dagster.StaticPartitionsDefinition
    partition_keys: ["10.csv", "20.csv", "30.csv"]
```

::: kedro_dagster.datasets.DagsterPartitionedDataset

---

#### `DagsterNothingDataset`

A dummy dataset representing a Dagster asset of type `Nothing` without associated data used to enforce links between nodes.

`catalog.yml` example snippet:

```yaml
my_barrier:
  type: kedro_dagster.DagsterNothingDataset
  metadata:
    description: "Barrier to enforce execution order"
```

::: kedro_dagster.datasets.DagsterNothingDataset

### Logging

#### `logging` module drop-in replacement

Integration of Kedro logging with Dagster's logging system to ensure logs from Kedro nodes are captured in the Dagster UI.

!!! note
    The `kedro_dagster.logging` module provides a `getLogger` function that Kedro nodes can use to obtain loggers compatible with Dagster's logging system. This ensures that logs generated within Kedro nodes are properly captured and displayed in the Dagster UI. We recommend replacing standard `logging.getLogger` calls in your Kedro nodes with `kedro_dagster.logging.getLogger` to benefit from this integration.

::: kedro_dagster.logging.getLogger

#### Dagster CLI logger formatting utilities

Implementation of Dagster CLI logger formatters to be used in Kedro's `logging.yml` configuration for unifying logging output between Dagster, Kedro, Kedro-Dagster, or any third-party libraries.

##### `dagster_colored_formatter`

Formatter used in Kedro-Dagster CLI commands when `--log-format colored` is specified.

::: kedro_dagster.logging.dagster_colored_formatter

---

##### `dagster_rich_formatter`

Formatter used in Kedro-Dagster CLI commands when `--log-format rich` is specified.

::: kedro_dagster.logging.dagster_rich_formatter

---

##### `dagster_json_formatter`

Formatter used in Kedro-Dagster CLI commands when `--log-format json` is specified.

::: kedro_dagster.logging.dagster_json_formatter

### Utilities

Helper functions for formatting, filtering, and supporting translation between Kedro and Dagster concepts.

::: kedro_dagster.utils
