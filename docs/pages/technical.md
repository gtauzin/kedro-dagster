# Technical documentation

This section provides an in-depth look at the architecture, configuration, and core concepts behind Kedro-Dagster. Here you'll find details on how Kedro projects are mapped to Dagster constructs, how to configure orchestration, and how to customize the integration for advanced use cases.

## How the translation works

Kedro-Dagster reads your Kedro project and the configuration under `conf/<ENV>/` to generate a Dagster code location. The selected environment determines which `catalog.yml` and `dagster.yml` are loaded. Translators then build Dagster assets and IO managers from the Kedro catalog, map nodes to ops and multi-assets, and construct jobs by filtering pipelines according to `dagster.yml`. All generated objects are registered in a single `dagster.Definitions` instance exposed by the Kedro-Dagster's generated `definitions.py`.

When you run the UI with `kedro dagster dev -e <ENV>`, the command loads the active environment, applies any overrides found in the `dev` section of `dagster.yml` (for example, log level or port), and serves the generated Definitions. For a walkthrough with concrete examples, see the [example page](example.md).

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

See the API reference for [`CatalogTranslator`](api.md#catalogtranslator) for more details.

!!! note
    Each Kedro dataset can take a `metadata` parameter to define additional metadata for the corresponding Dagster asset, such as a description. This description will appear in the Dagster UI.

### Node

Kedro nodes are translated into Dagster ops and assets. Each node becomes a Dagster op, and, additionally, nodes that return outputs are mapped to Dagster multi-assets.

For the Kedro pipelines specified in `dagster.yml`, the following Dagster objects are defined:

- **Ops**: Each Kedro node within the pipelines is mapped to a Dagster op.
- **Assets**: Kedro nodes that return output datasets are registered as Dagster multi-assets.
- **Parameters**: Node parameters are passed as Dagster config to enable them to be modified in a Dagster run launchpad.

See the API reference for [`NodeTranslator`](api.md#nodetranslator) for more details.

### Pipeline

Kedro pipelines are translated into Dagster jobs. Each job can be filtered, scheduled, and assigned an executor via configuration.

- **Jobs**: Each pipeline is mapped to a Dagster job.
- **Filtering**: Jobs are defined granuarily from Kedro pipelines by allowing the filtering of their nodes, namespaces, tags, and inputs/outputs.

If one of the datasets involved in the pipeline is a `DagsterPartitionedDataset`, the corresponding job will fan-out the nodes the partitioned datasets are involved in according to the defined partitions.

See the API reference for [`PipelineTranslator`](api.md#pipelinetranslator) for more details.

### Hook

Kedro-Dagster preserves all [Kedro hooks](https://docs.kedro.org/en/stable/hooks/index.html#hooks) in the Dagster context. Hooks are executed at the appropriate points in the Dagster job lifecycle. Catalog hooks are called in the `handle_output` and `load_input` function of each Dagster IO manager. Node hooks are plugged in the appropriate Dagster Op. As for the Context hook, they are called within a Dagster Op running at the beginning of each job along with the `before_pipeline_run` pipeline hook. The `after_pipeline_run` is called in a Dagster op running at the end of each job. Finally the `on_pipeline_error` pipeline, is embedded in a dedicated Dagster sensor that is triggered by a run failure.

## Compatibility notes between Kedro and Dagster

### Naming conventions

Dagster enforces strong constraints for asset, op, and job names  as they must match the regex `^[A-Za-z0-9_]+$`. As those Dagster objects are created directly from Kedro datasets, nodes, and pipelines, Kedro-Dagster applies a small set of deterministic transformations so Kedro names map predictably to Dagster names:

- **Datasets**: only the dot "." namespace separator is converted to a double underscore "__" when mapping a Kedro dataset name to a Dagster-friendly identifier. Example: `my.dataset.name` -> `my__dataset__name`. Other characters (for example, hyphens `-`) are preserved by the formatter. Internally Kedro-Dagster will get back the dataset name from the asset name by replacing double underscored by dots.
- **Nodes**: dots are replaced with double underscores to keep namespaces (`my.node` -> `my__node`). If the resulting node name still contains disallowed characters (anything outside A–Z, a–z, 0–9 and underscore), the node name is replaced with a stable hashed placeholder of the form `unnamed_node_<md5>` to ensure it meets Dagster's constraints.

These rules are implemented in `src/kedro_dagster/utils.py` by `format_dataset_name`, `format_node_name`, and `unformat_asset_name` and are intentionally minimal and deterministic so names remain readable while complying with Dagster's requirements.

## Kedro datasets for Dagster partitioning

Kedro-Dagster provides two custom datasets to enable Dagster partitioning and asset management within Kedro projects:

- **`DagsterPartitionedDataset`**: A Kedro dataset that is partitioned according to Dagster's partitioning scheme. This allows for more efficient data processing and management within Kedro pipelines.
- **`DagsterNothingDataset`**: A special Kedro dataset that represents a "no-op" or empty dataset in Dagster. This can be useful for cases where an order in execution between two nodes needs to be enforced.

!!! danger
  Dagster partitions support is currently experimental. Please open an issue if you encounter problems or have feature requests.

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

!!! danger
    `MultiPartitionsDefinition` is currently not supported.

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

See the API reference for [`DagsterPartitionedDataset`](api.md#dagsterpartitioneddataset) for more details.

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

See the API reference for [`DagsterNothingDataset`](api.md#dagsternothingdataset) for more details.

## Project configuration

Kedro-Dagster expects a standard [Kedro project structure](https://docs.kedro.org/en/stable/get_started/kedro_concepts.html#kedro-project-directory-structure). The main configuration file for Dagster integration is `dagster.yml`, located in your Kedro project's `conf/<ENV_NAME>/` directory.

### dagster.yml

This YAML file defines `dagster dev` options, jobs, executors, and schedules for your project.

!!! example
  ```yaml
  dev:
    log_level: info
    log_format: colored
    host: 127.0.0.1
    port: "3000"
    live_data_poll_rate: "2000"

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

You can define multiple schedules for your jobs using cron syntax. See the [Dagster scheduling documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) and the [API Reference](api.md#scheduleoptions) for more details.

#### Customizing executors

Kedro-Dagster supports several executor types for running your jobs, such as in-process, multiprocess, Dask, Docker, Celery, and Kubernetes. You can customize executor options in your `dagster.yml` file under the `executors` section.

For each [available Dagster executor](https://docs.dagster.io/guides/operate/run-executors#example-executors), there is a corresponding configuration Pydantic model documented in the [API reference](api.md#executoroptions).

##### Example: Custom multiprocess executor

You can select `multiprocess` as the executor type corresponding to the [multiprocess Dagster executor](https://docs.dagster.io/api/dagster/execution#dagster.multiprocess_executor) and configure it according to the [MultiprocessExecutorOptions](api.md#multiprocessexecutoroptions).

```yaml
executors:
  my_multiprocess_executor:
    multiprocess:
      max_concurrent: 4
```

##### Example: Custom Docker executor

Similarly, we can configure a [Docker Dagster executor](https://docs.dagster.io/api/libraries/dagster-docker#dagster_docker.docker_executor) with the available parameters defined in [`DockerExecutorOptions`](api.md#dockerexecutoroptions).

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

You can filter which nodes, tags, or inputs/outputs are included in each job. Each job can be associated with a pre-defined executor and/or schedule. See the [Kedro pipeline documentation](https://docs.kedro.org/en/stable/api/kedro.pipeline.Pipeline.html#kedro.pipeline.Pipeline.filter) for more on pipelines and filtering. The accepted pipeline parameters are documented in the associated Pydantic model, [`PipelineOptions`](api.md#pipelineoptions).

To each job, you can assign a schedule and/or an executor by name if it was previously defined in the configuration file.

### definitions.py

The `definitions.py` file is auto-generated by the plugin and serves as the main entry point for Dagster to discover all translated Kedro objects. It contains the Dagster [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object, which registers all jobs, assets, resources, schedules, and sensors derived from your Kedro project.

In most cases, you should not manually edit `definitions.py`; instead, update your Kedro project or `dagster.yml` configuration.

---

## Next steps

- **Getting started:** Follow the [step-by-step tutorial](getting-started.md) to set up Kedro-Dagster in your project.
- **Advanced example:** See the [example documentation](example.md) for a real-world use case.
- **API reference:** Explore the [API reference](api.md) for details on available classes, functions, and configuration options.
- **External documentation:** For more on Kedro concepts, see the [Kedro documentation](https://kedro.readthedocs.io/en/stable/). For Dagster concepts, see the [Dagster documentation](https://docs.dagster.io/).
