# Example

This section introduces an advanced Kedro project with Dagster hosted on the [Kedro-Dagster example repository](https://github.com/gtauzin/kedro-dagster-example). You can find a visualization of the pipelines defined in this project below in the [shared Kedro-Viz page](https://gtauzin.github.io/kedro-dagster-example/).

## Project overview

This Kedro project builds on the [Kedro Spaceflights tutorial](https://docs.kedro.org/en/stable/tutorial/spaceflights_tutorial.html), augmented with dynamic pipelines following the [GetInData blog post](https://getindata.com/blog/kedro-dynamic-pipelines/) and modified to make full use of [Dagster's partitions](https://docs.dagster.io/guides/build/partitions-and-backfills).

!!! note
    Here, parameters for dynamic pipelines are namespaced via YAML inheritance rather than the custom `merge` resolver approach presented in the original tutorial.

Additionally, the project features:

- **Multi-environment support**: Easily switch between `local`, `dev`, `staging`, and `prod` environments. Each environment has its own `dagster.yml` and `catalog.yml` in `conf/<ENV_NAME>/`.
- **MLflow integration**: [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow) is used for experiment tracking and model registry. Configure MLflow in your Kedro project and Kedro-Dagster will make it available as a Dagster resource.
- **Hyperparameter tuning with Optuna**: Integrate Optuna for distributed hyperparameter optimization via the [`optuna.StudyDataset`](https://docs.kedro.org/projects/kedro-datasets/en/latest/api/kedro_datasets_experimental.optuna.StudyDataset.html) Kedro dataset.

## Quick Start

1. **Clone the repository**:

   ```bash
   git clone https://github.com/gtauzin/kedro-dagster-example.git
   cd kedro-dagster-example
   ```

2. **Install dependencies** (using [uv](https://github.com/astral-sh/uv) for reproducible environments):

   ```bash
   uv sync
   source .venv/bin/activate
   ```

3. **Run Kedro pipelines** as usual:

   ```bash
   uv run kedro run --env <KEDRO_ENV>
   ```

   Replace `<KEDRO_ENV>` with your target environment (e.g., `local`).

4. (Optional) This example repository is already initialized for Kedro-Dagster

   You do not need to run `kedro dagster init` for this repo; `definitions.py` and per‑environment `conf/<ENV>/dagster.yml` are already present.

5. **List generated Dagster definitions** for each Kedro environment.

   For the `local` environment:

   ```bash
   kedro dagster list defs --env "local"
   ```

6. **Explore pipelines in Dagster UI**:

   ```bash
   kedro dagster dev -e "local"
   ```

   The `dev` environment requires a Postgres database. You can run one locally using Docker:

   ```bash
   docker compose -f docker/pipelines-dev.docker-compose.yml up -d
   ```

   Then, set the appropriate environment variables so that the Kedro catalog can connect to the database:

   ```bash
   export POSTGRES_USER=dev_db
   export POSTGRES_PASSWORD=dev_password
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   ```

   Your Kedro datasets appear as Dagster assets and pipelines as Dagster jobs.

<figure markdown>
![Lineage graph of assets](../images/example/local_asset_graph_dark.png#only-dark){data-gallery="assets-dark"}
![Lineage graph of assets](../images/example/local_asset_graph_light.png#only-light){data-gallery="assets-light"}
<figcaption>Dagster Asset Lineage Graph generated from the example Kedro project.</figcaption>
</figure>

<figure markdown>
![List of assets](../images/example/local_asset_list_dark.png#only-dark){data-gallery="assets-dark"}
![List of assets](../images/example/local_asset_list_light.png#only-light){data-gallery="assets-light"}
<figcaption>Dagster Asset List generated from the example Kedro project.</figcaption>
</figure>

## Technical guide

This section explains how the example repository is wired to Dagster: environments, configuration, partitions, MLflow/Optuna integration, and how Kedro objects are translated.

### Dynamic pipelines

This example builds pipelines dynamically and separates them across environments using the `KEDRO_ENV` environment variable that is set at runtime by the Kedro and Kedro-Dagster CLI commands. The selected environment controls which configuration files are loaded from `conf/<ENV>/` and, as a result, which jobs are translated and shown in the Dagster UI.

Use `--env` for Kedro commands and `-e` for the Dagster UI. For example:

```bash
kedro dagster dev -e "dev"
```

When you launch the UI, Kedro‑Dagster constructs Dagster jobs from the active environment’s `dagster.yml` (filters, executor, schedules) and reads datasets from that environment’s `catalog.yml`. Changing the Kedro environment switches both the configuration and the set of dynamic pipelines considered, which in turn changes the jobs that appear in the UI.

Pipelines are built dynamically and parameterized by namespace and tags. Rather than introducing a custom `merge` resolver, parameters are namespaced via YAML inheritance, which keeps the configuration simple and makes per‑environment overrides straightforward. The split of dynamic pipelines across environments is orchestrated in the example’s [`settings.py`](https://github.com/gtauzin/kedro-dagster-example/blob/main/src/kedro_dagster_example/settings.py), which defines which pipelines are active per environment:

```python
if KEDRO_ENV == "local":
    DYNAMIC_PIPELINES_MAPPING = {
        "reviews_predictor": ["base", "candidate1"],
        "price_predictor": [
            "base",
            "candidate1",
            "test1",
        ],
    }
elif KEDRO_ENV == "dev":
    DYNAMIC_PIPELINES_MAPPING = {
        "price_predictor": ["test1"],
    }
elif KEDRO_ENV == "staging":
    DYNAMIC_PIPELINES_MAPPING = {
        "reviews_predictor": ["candidate1"],
        "price_predictor": ["candidate1"],
    }
elif KEDRO_ENV == "prod":
    DYNAMIC_PIPELINES_MAPPING = {
        "reviews_predictor": ["base"],
        "price_predictor": ["base"],
    }
else:
    raise ValueError(f"Unknown KEDRO_ENV value: {KEDRO_ENV}")
```

 and [`pipeline_registry.py`](https://github.com/gtauzin/kedro-dagster-example/blob/main/src/kedro_dagster_example/pipeline_registry.py), which registers only the pipelines relevant to the active environment:

```python
def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns
    -------
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()

    env_pipeline_names = ["data_processing", "data_science"]
    if KEDRO_ENV in ["local", "dev"]:
        env_pipeline_names.append("model_tuning")

    pipelines = {pipeline_name: pipelines[pipeline_name] for pipeline_name in env_pipeline_names}

    # https://github.com/kedro-org/kedro/issues/2526
    pipelines["__default__"] = sum(pipelines.values(), start=Pipeline([]))
    return pipelines
```

The `DYNAMIC_PIPELINES_MAPPING` is then used to build pipelines variants dynamically in each pipeline definition function.

### Environments configuration at a glance

The example repository demonstrates how a data science project evolves across environments by changing which pipelines are active and how jobs are executed/scheduled.

The example defines four environments, `local`, `dev`, `staging`, and `prod`, each under `conf/<ENV>/` with its own `catalog.yml` and `dagster.yml`. Initialization is already performed in this repository: `src/kedro_dagster_example/definitions.py` and all per‑environment `dagster.yml` files are present.

While `staging` and `prod` are similar, `local` is geared towards development with in‑process execution and `dev` introduces multiprocessing and scheduling, as well as a new `model_tuning` pipeline.

The `dev` section in `dagster.yml` configures how `kedro dagster dev` behaves (log level/format, host/port, and live polling rate). The executor and schedule sections define how jobs are executed and scheduled and the jobs section defines which Kedro pipelines are translated into Dagster jobs.

Example `conf/prod/dagster.yml` (trimmed):

```yaml
dev:
  log_level: "info"
  log_format: "colored"
  port: "3000"
  host: "127.0.0.1"
  live_data_poll_rate: "2000"

executors:
  sequential:
    in_process:

  multiprocessing:
    multiprocess:
      max_concurrent: 2

schedules:
  daily:
    cron_schedule: "30 2 * * *"

jobs:
  reviews_predictor_data_processing_base:
    pipeline:
      pipeline_name: data_processing
      node_namespaces:
      - reviews_predictor
      tags:
      - base
    executor: multiprocessing
    schedule: daily

  reviews_predictor_data_science_base:
    pipeline:
      pipeline_name: data_science
      node_namespaces:
      - reviews_predictor
      tags:
      - base
    executor: multiprocessing
    schedule: daily
```

In practice, each Dagster job is generated from a filtered Kedro pipeline (selected by `pipeline_name` and optionally narrowed by `node_namespaces` or `tags`). The job uses the configured executor (`in_process`, `multiprocess`, and so on). When you run `kedro dagster dev -e local`, the UI listens on `127.0.0.1:3000` and prints colored logs, as specified above.

See also the API reference entries for filtering and execution options: [`PipelineOptions`](reference.md#pipelineoptions) and executor models.

### Partitions in practice

The `local` environment uses Dagster partitions via a dedicated Kedro dataset wrapper. Companies data are preprocessed in partitions, defined as static CSV files. The catalog defines both the upstream partitioned dataset and its downstream counterpart with a partition mapping:

```yaml
companies_dagster_partition:
  type: kedro_dagster.DagsterPartitionedDataset
  path: data/01_raw/companies/
  dataset:
    type: pandas.CSVDataset
  partition:
    type: dagster.StaticPartitionsDefinition
    partition_keys: ["1.csv","2.csv", "3.csv"]
  partition_mapping:
    "{namespace}.preprocessed_companies_dagster_partition":
      type: dagster.StaticPartitionMapping
      downstream_partition_keys_by_upstream_partition_key:
        1.csv: 10.csv
        2.csv: 20.csv
        3.csv: 30.csv

"{namespace}.preprocessed_companies_dagster_partition":
   type: kedro_dagster.DagsterPartitionedDataset
   path: data/02_intermediate/<env>/{namespace}/preprocessed_companies/
   dataset:
      type: pandas.CSVDataset
   partition:
      type: dagster.StaticPartitionsDefinition
      partition_keys: ["10.csv", "20.csv", "30.csv"]
```

Partition mappings are implemented via `partition_mappings` on `DagsterPartitionedDataset` (pattern targets like `{namespace}.partitioned_dataset` are allowed) defined in the upstream dataset for each downstream datasets. Here, the partition mapping above links upstream raw company files (`1.csv`, `2.csv`, `3.csv`) to downstream preprocessed files (`10.csv`, `20.csv`, `30.csv`).

In practice, when a job involving partitioned datasets is launched in Dagster, for each upstream partition key (e.g., `1.csv`) a separate dagster op will be created to compute the corresponding downstream partition key (`10.csv`) based on the defined mapping.

!!! note
   `DagsterPartitionedDataset` reduces to Kedro's `PartitionedDataset` when ran outside of Dagster, so the pipeline remains runnable with `kedro run` and outputs the same results.

It is also possible to materialize partitioned assets using the Dagster UI. You can launch a job that touches a partitioned asset and select a specific partition key in the Launchpad. For larger runs, use a backfill to materialize multiple partition keys at once.

### Enforcing order with `Nothing` assets

Once all preprocessed company data partitions are ready, we collect them for downstream processing. To do that, we use a Kedro `PartitionedDataset` to gather the preprocessed partitions. However, as the preprocessing node and the collection node are separate, we need to ensure that the collection only happens after all preprocessing is complete. To achieve this, we use a `DagsterNothingDataset` to enforce that preprocessing is done before collection:

```yaml
"{namespace}.is_company_preprocessing_done":
   type: kedro_dagster.DagsterNothingDataset

"{namespace}.preprocessed_companies_partition":
   type: partitions.PartitionedDataset
   path: data/02_intermediate/<env>/{namespace}/preprocessed_companies/
   dataset:
      type: pandas.CSVDataset
```

These appear as `Nothing` assets in Dagster and only enforce dependencies.

### Optuna-based hyperparameter tuning

The example demonstrates how to use the [`optuna.StudyDataset`](https://docs.kedro.readthedocs.io/en/stable/advanced/experimental.html#optuna-studydataset) Kedro dataset for distributed hyperparameter optimization via Optuna.

The `StudyDataset` allows Kedro to load and save Optuna studies, which collect optimization trials. It allows users to define an Optuna sampler and pruner via `load_args` and supports multiple backends (SQLite, PostgreSQL, and so on) for study storage.


In this example, two backends are demonstrated:

- SQLite for `local` (stored under `data/`)

   ```yaml
   "{namespace}.{variant}.study":
   type: kedro_datasets_experimental.optuna.StudyDataset
   backend: sqlite
   database: data/06_models/local/{namespace}/{variant}/optuna.db
   study_name: "{namespace}.{variant}"
   load_args:
      sampler:
         class: TPESampler
         n_startup_trials: 2
         n_ei_candidates: 5
      pruner:
         class: NopPruner
   ```

- PostgreSQL for `dev` (backed by the provided Docker Compose service and credentials).

   ```yaml
   "{namespace}.{variant}.study":
   type: kedro_datasets_experimental.optuna.StudyDataset
   backend: postgresql
   database: dev_db
   study_name: "{namespace}.{variant}"
   load_args:
      sampler:
         class: TPESampler
         n_startup_trials: 2
         n_ei_candidates: 5
      pruner:
         class: NopPruner
   versioned: true
   credentials: dev_optuna
   ```

For the `dev` environment you must:

1) Start Postgres: `docker compose -f docker/pipelines-dev.docker-compose.yml up -d`
2) Export connection env vars (username/password/host/port)

   ```bash
   export POSTGRES_USER=dev_db
   export POSTGRES_PASSWORD=dev_password
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   ```

3) Run `kedro dagster dev`

### MLflow integration

`kedro-dagster` integrates with `kedro-mlflow` to expose MLflow tracking and model registry as a Dagster resource. The example project is preconfigured for MLflow tracking with `kedro-mlflow` and configuration directory for each environment includes an `mlflow.yml`.

Models and artifacts are tracked via Kedro-MLflow datasets in the catalog, e.g.:

```yaml
"{namespace}.{variant}.regressor":
   type: kedro_mlflow.io.models.MlflowModelTrackingDataset
   flavor: mlflow.sklearn
   artifact_path: "{namespace}/{variant}/regressor"
```

Additionally, we show how to use MLflow alongside Optuna in a Kedro project throught the use of the Optuna's `MLflowCallback`, which logs Optuna trials as MLflow runs. The callback is added to the study in the [tuning node](https://github.com/gtauzin/kedro-dagster-example/blob/main/src/kedro_dagster_example/pipelines/model_tuning/nodes.py#L72-L112). This creates a nested MLflow run for each Optuna trial, allowing you to track hyperparameter optimization experiments directly in MLflow.

### Common pitfalls and troubleshooting

- Dev database not reachable: Ensure Docker container is up and env vars match `conf/dev/credentials.yml` (example uses `dev_optuna`).
- UI didn’t reflect config changes: Stop and restart `kedro dagster dev`; some changes aren’t hot‑reloaded by Dagster.
- Asset names vs Kedro names: Dots in Kedro dataset names become `__` in Dagster; this is expected and reversible.

---

## Next steps

- **Technical documentation:** Explore the full [technical documentation](technical.md) for mapping details and configuration models.
- **Reference:** See the [Kedro-Dagster reference](reference.md) for API and CLI details.
