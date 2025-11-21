# Unreleased

## Major features and improvements

## Bug fixes and other changes

## Breaking changes to the API

## Thanks for supporting contributions

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Release 0.4.0

## Major features and improvements
- Wrap all Dagster `dg` CLI commands to be run from within a Kedro project with `kedro dagster <dg command>` by @gtauzin.
- Add a `kedro_dagster.logging` meant to replace `logging` imports in Kedro nodes so loggers are captured and integrated with Dagster by @gtauzin.
- Add `loggers` section to `dagster.yml` configuration file to configure Dagster run loggers by @gtauzin.
- Rename `LoggerTranslator` to `LoggerCreator` for consistency with `ExecutorCreator` and `SchedulerCreator` by @gtauzin.
- Declared direct dependency on `pydantic>=1.0.0,<3.0.0` and enable version-agnostic Pydantic configuration by @gtauzin.
- Kedro-Dagster is now available on conda-forge and can be installed with `conda` or `mamba` by @rxm7706 and @gtauzin.
- Allow setting `group_name` in a dataset's `metadata` to override the pipeline-derived group name; `group_name` is also applied per-AssetOut for multi-assets so each asset can have an individual group by @gtauzin.
- Add links to MLflow run in Dagster run logs, run tags, and materialized asset metadata by @gtauzin.

## Bug fixes and other changes
- Fix how `LoggerCreator` creates loggers for Dagster runs. Generic logging configuration is now supported from `dagster.yml` by @gtauzin.

## Breaking changes to the API
- `env` is now a required parameter of `KedroProjectTranslator` by @gtauzin.
- Remove `dev` config in `dagster.yml` by @gtauzin.

## Thanks for supporting contributions

- @gtauzin
- @rxm7706

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Release 0.3.0

## Major features and improvements

- Add `DagsterNothingDataset`, a Kedro dataset that performs no I/O but enforces node dependency by @gtauzin.
- Add `DagsterPartitionedDataset`, a Kedro dataset for partitioned data compatible with Dagster's asset partitions by @gtauzin.
- Enable fanning out Kedro nodes when creating the Dagster graph when using `DagsterPartitionedDataset` with multiple partition keys by @gtauzin.
- Add support for Kedro >= 1.0.0 and Dagster >= 1.12.0 by @gtauzin.

## Bug fixes and other changes

- Fix bug involving unnamed Kedro nodes making `kedro dagster dev` crash by @gtauzin.
- Fix defaults on K8S execution configuration by @gtauzin.

## Breaking changes to the API

## Thanks for supporting contributions

- @gtauzin

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Release 0.2.0

This release is a complete refactoring of Kedro-Dagster and its first stable version.

## Thanks for supporting contributions

- @gtauzin

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Pre-Release 0.1.1

## Major features and improvements

## Bug fixes and other changes

- Fixed CLI entrypoint by @gtauzin.
- Set up documentation, behavior tests, unit tests and CI by @gtauzin.

## Breaking changes to the API

## Thanks for supporting contributions

- @gtauzin

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.

# Pre-Release 0.1.0:

Initial release of Kedro-Dagster.

## Thanks to our main contributors

- @gtauzin

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
