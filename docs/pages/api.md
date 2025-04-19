# API Reference

This section provides an overview and links to the Kedro-Dagster API documentation.

## Command Line Interface

Kedro-Dagster provides CLI commands to initialize and run the translation of your Kedro project into Dagster.

### `kedro dagster init`

Initializes Dagster integration for your Kedro project by generating the necessary `definitions.py` and configuration files.

::: kedro_dagster.cli.init

**Usage:**

```bash
uv run kedro dagster init --env <ENV_NAME> --force --silent
```

- `--env`: The Kedro environment where the `dagster.yml` should be created (default: `local`).
- `--force`: Overwrite existing files without prompting.
- `--silent`: Suppress output messages when files are modified.

### `kedro dagster dev`

Starts the Dagster development UI and launches your Kedro pipelines as Dagster jobs for interactive development and monitoring.

::: kedro_dagster.cli.dev

**Usage:**

```bash
uv run kedro dagster dev --env <ENV_NAME> --log-level <LEVEL> --log-format <FORMAT> --port <PORT> --host <HOST> --live-data-poll-rate <RATE>
```

- `--env`: The Kedro environment to use (e.g., `local`).
- `--log-level`: Logging level (`debug`, `info`, `warning`, `error`, or `critical`).
- `--log-format`: Log output format (`colored`, `json`, `default`).
- `--port`: Port for the Dagster UI.
- `--host`: Host address for the Dagster UI.
- `--live-data-poll-rate`: Polling rate for live data in milliseconds.

If specified, those options will override the ones specified in your `dagster.yml`.

## Configuration

TODO pydantic classes

## Translation Modules

The following classes are responsible for translating Kedro concepts into Dagster constructs:

### `KedroProjectTranslator`

Translates an entire Kedro project into a Dagster code location, orchestrating the translation of pipelines, datasets, resources, jobs, schedules, sensors, and loggers.

::: kedro_dagster.translator.KedroProjectTranslator

---

### `DagsterCodeLocation`

Represents the collection of Dagster definitions (jobs, assets, resources, etc.) generated from a Kedro project.

::: kedro_dagster.translator.DagsterCodeLocation

---

### `CatalogTranslator`

Translates Kedro datasets into Dagster IO managers and assets, enabling seamless data handling between Kedro and Dagster.

::: kedro_dagster.catalog.CatalogTranslator

---

### `NodeTranslator`

Converts Kedro nodes into Dagster ops and assets, handling parameter passing, output mapping, and hook integration.

::: kedro_dagster.nodes.NodeTranslator

---

### `PipelineTranslator`

Maps Kedro pipelines to Dagster jobs, supporting pipeline filtering, hooks, job configuration, and resource assignment.

::: kedro_dagster.pipelines.PipelineTranslator

---

### `KedroRunTranslator`

Manages translation of Kedro run parameters and hooks into Dagster resources and sensors, including error handling and context propagation.

::: kedro_dagster.kedro.KedroRunTranslator

---

### `ExecutorCreator`

Creates Dagster executors from configuration, allowing for flexible job execution strategies.

::: kedro_dagster.dagster.ExecutorCreator

---

### `LoggerTranslator`

Translates Kedro loggers to Dagster loggers for unified logging across both frameworks.

::: kedro_dagster.dagster.LoggerTranslator

---

### `ScheduleCreator`

Generates Dagster schedules from configuration, enabling automated pipeline execution.

::: kedro_dagster.dagster.ScheduleCreator

---

### Utilities

Helper functions for formatting, filtering, and supporting translation between Kedro and Dagster concepts.

::: kedro_dagster.utils
