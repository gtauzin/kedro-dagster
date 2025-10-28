# Example

This section introduces an advanced Kedro project with Dagster, inspired by the [Kedro-Dagster Example Repository](https://github.com/gtauzin/kedro-dagster-example).

!!! danger
    This documentation section is a work in progress. Please check back later for a more complete guide!

## Project Overview

This repo builds on the [Kedro Spaceflights tutorial](https://docs.kedro.org/en/stable/tutorial/spaceflights_tutorial.html), augmented with dynamic pipelines following the [GetInData blog post](https://getindata.com/blog/kedro-dynamic-pipelines/) and modified to make full use of [Dagster's partitions](https://docs.dagster.io/guides/build/partitions-and-backfills).

!!! note
    Here, parameters for dynamic pipelines are namespaced via YAML inheritance rather than a custom `merge` resolver.

Additionally, the project features:

- **Multi-environment support**: Easily switch between `local`, `dev`, `staging`, and `prod` environments. Each environment has its own `dagster.yml` and `catalog.yml` in `conf/<ENV_NAME>/`.
- **MLflow integration**: [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow) is used for experiment tracking and model registry. Configure MLflow in your Kedro project and it will be available as a Dagster resource.
- **Hyperparameter tuning with Optuna**: Integrate Optuna for distributed hyperparameter optimization via the [`optuna.StudyDataset`](https://docs.kedro.org/projects/kedro-datasets/en/latest/api/kedro_datasets_experimental.optuna.StudyDataset.html) Kedro dataset.

## Quick Start

1. **Install dependencies** (using [uv](https://github.com/astral-sh/uv) for reproducible environments):

   ```bash
   uv sync
   source .venv/bin/activate
   ```

2. **Run Kedro pipelines** as usual:

   ```bash
   uv run kedro run --env <KEDRO_ENV>
   ```

   Replace `<KEDRO_ENV>` with your target environment (e.g., `local`).

3. **Explore pipelines in Dagster UI**:

   ```bash
   export KEDRO_ENV=local
   kedro dagster dev
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

---

## Next Steps

- Explore the [Technical Documentation](technical.md) for advanced configuration and customization.
- See the [API Reference](api.md).
