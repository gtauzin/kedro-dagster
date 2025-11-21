
# Unreleased

## Major features and improvements
- Wrap all Dagster `dg` CLI commands to be run from within a Kedro project with `kedro dagster <dg command>`.
- Add a `kedro_dagster.logging` meant to replace `logging` imports in Kedro nodes so loggers are captured and integrated with Dagster.
- Add `loggers` section to `dagster.yml` configuration file to configure Dagster run loggers.
- Rename `LoggerTranslator` to `LoggerCreator` for consistency with `ExecutorCreator` and `SchedulerCreator`.
- Declared direct dependency on `pydantic>=1.0.0,<3.0.0` and enable version-agnostic Pydantic configuration.
- Kedro-Dagster is now available on conda-forge and can be installed with `conda` or `mamba`.
- Allow setting `group_name` in a dataset's `metadata` to override the pipeline-derived group name; `group_name` is also applied per-AssetOut for multi-assets so each asset can have an individual group.
- Add links to MLflow run in Dagster run logs, run tags, and materialized asset metadata.

## Bug fixes and other changes
- Fix how `LoggerCreator` creates loggers for Dagster runs. Generic logging configuration is now supported from `dagster.yml`.

## Breaking changes to the API
- `env` is now a required parameter of `KedroProjectTranslator`.
- Remove `dev` config in `dagster.yml`.

## Thanks for supporting contributions

[Guillaume Tauzin](https://github.com/gtauzin).

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Release 0.3.0

## Major features and improvements

* Add `DagsterNothingDataset`, a Kedro dataset that performs no I/O but enforces node dependency.
* Add `DagsterPartitionedDataset`, a Kedro dataset for partitioned data compatible with Dagster's asset partitions.
* Enable fanning out Kedro nodes when creating the Dagster graph when using `DagsterPartitionedDataset` with multiple partition keys.
* Add support for Kedro >= 1.0.0 and Dagster >= 1.12.0.

## Bug fixes and other changes

* Fix bug involving unnamed Kedro nodes making `kedro dagster dev` crash
* Fix defaults on K8S execution configuration

## Breaking changes to the API

## Thanks for supporting contributions

[Guillaume Tauzin](https://github.com/gtauzin).

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Release 0.2.0

This release is a complete refactoring of Kedro-Dagster and its first stable version.

## Thanks for supporting contributions

[Guillaume Tauzin](https://github.com/gtauzin).

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Pre-Release 0.1.1

## Major features and improvements

## Bug fixes and other changes

* Fixed CLI entrypoint.
* Set up documentation, behavior tests, unit tests and CI.

## Breaking changes to the API

## Thanks for supporting contributions

[Guillaume Tauzin](https://github.com/gtauzin).

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Pre-Release 0.1.0:

Initial release of Kedro-Dagster.

## Thanks to our main contributors

[Guillaume Tauzin](https://github.com/gtauzin).

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
