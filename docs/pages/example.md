# Example

This section provides practical, real-world examples and advanced usage patterns for Kedro-Dagster.

---

# Kedro-Dagster Example

Guide to deploying a Kedro project with Dagster based on the [Kedro-Dagster Example Repository](https://github.com/gtauzin/kedro-dagster-example).

## Multi-Environment Setup

Kedro-Dagster supports multiple environments (e.g., `local`, `dev`, `staging`, `prod`). Each environment has its own `dagster.yml` and `catalog.yml` in `conf/<ENV_NAME>/`. This allows you to:

- Use local data and settings for development.
- Point to cloud or production data in `dev`, `staging`, or `prod`.
- Build separate Docker images per environment for deployment.

## Kedro-MLflow Integration

Kedro-Dagster is designed to be used alongside [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow) for experiment tracking and model registry. Configure MLflow in your Kedro project and it will be available as a Dagster resource.

## Multiple Execution Targets

Kedro-Dagster maps all the executors available in Dagster, thus allowing different filtered pipelines to be ran on specific compute targets.

For a full example project, see the [Kedro-Dagster Example Repository](https://github.com/gtauzin/kedro-dagster-example).
