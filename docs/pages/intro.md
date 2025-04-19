# Introduction

Kedro‑Dagster is a  plugin that seamlessly connects your **Kedro** data science pipelines to **Dagster’s** orchestration engine. With minimal setup, you can run, schedule, and monitor Kedro projects in Dagster, taking advantage of its rich UI, asset lineage tracking, and cloud‑native executors without altering your existing codebase.

## What Is Kedro?

[Kedro](https://kedro.readthedocs.io/) is a Python framework for building reproducible, maintainable, and modular data science code. It enforces best practices such as separation of concerns, configuration management, and a data catalog, ensuring that pipelines are production‑ready from the get-go.

## What Is Dagster?

[Dagster](https://docs.dagster.io/) is a modern Python data orchestrator designed around the concept of assets. It provides:

- **Jobs & assets**: Define data transformations and treat their outputs as first‑class assets.
- **Scheduling & sensors**: Configure cron jobs and event‑driven triggers.
- **Observability**: Track lineage, view logs, and debug runs through a web UI.
- **Execution environments**: Run locally, on Kubernetes, or with cloud‑native executors.

Dagster scales from local development to enterprise deployments, emphasizing testability and modularity every step of the way.

## Why Choose Kedro‑Dagster?

Kedro and Dagster share an asset-first philosophy. In Kedro, assets are datasets passed between modular nodes that make up a pipeline. Dagster mirrors this by treating the output of each computation as an asset with associated lineage, metadata, and execution semantics. This alignment allows Kedro pipelines to be translated into Dagster assets with minimal effort, preserving structure and enabling rich observability out of the box.&#x20;

However, what makes Kedro‑Dagster shine is that it leverages the complementarity of both frameworks. Whether you're starting from Kedro or Dagster, Kedro‑Dagster allows each tool to play to its strengths. Kedro provides a robust developer experience for building pipelines—modular, testable, and backed by strong configuration and data cataloging. Dagster, in turn, brings a powerful orchestration layer with scheduling, logging, asset monitoring, and execution control.

### For Kedro Users

- **No code changes:** Integrate Dagster without modifying your existing Kedro datasets, config, or pipelines.
- **Enhanced orchestration and observability:** Use Dagster’s UI to visualize, launch and schedule runs, inspect logs, trace asset lineage, and monitor pipeline health. Gain real-time insights into execution status, track data dependencies, and debug failures with full context.
- **Variety of execution targets:** Run your pipelines locally, on a remote machine, using Docker, or scale out on Kubernetes and other Dagster-supported environments.

### For Dagster Users

- **Built‑in data connectors:** Kedro’s `DataCatalog` provides a centralized and declarative way to manage all data inputs and outputs across environments. It supports a wide range of data sources out of the box from local CSVs and Parquet files to cloud storage like S3 and GCS.&#x20;
- **Straightforward asset and workflow creation:** Kedro makes it simple to define pipelines as sequences of modular, reusable nodes without worrying about orchestration logic. These pipelines are automatically translated into Dagster assets, enabling you to develop locally and immediately visualize and orchestrate your work in Dagster’s UI with minimal configuration.
- **Full control over Kedro-based Dagster objects:** Kedro projects are seamlessly translated into Dagster code locations. Any aspect of the generated Dagster assets, jobs, executor or resources can be modified on the Dagster UI without modifying the Kedro code.

## Key features

### Configuration‑Driven Workflows

Centralize orchestration settings in a `dagster.yml` file, where you can:

- Define jobs (mapped to filtered Kedro pipelines).
- Assign executors, retries, and resource limits.
- Assign cron jobs-based schedules.

### Customization

The core integration lives in the auto‑generated `definitions.py`. For specialized requirements such as custom resources, deployment patterns, or non‑standard executors, you can extend or override parts of these definitions manually.

### Kedro Hooks preservation

Kedro-Dagster is designed so that Kedro Hooks are preserved and called at the appropriate time. TODO

### MLflow Compatibility

Harness the capabilities of MLflow using [Kedro-MLflow](https://github.com/Galileo-Galilei/kedro-mlflow) in conjunction with Dagster’s [MLflow integration](https://dagster.io/integrations/dagster-mlflow). Whether you run your pipelines using Kedro or Dagster, you can track experiments, log models, and register artifacts automatically throught the `mlflow.yml` configuration file.

### Logger Integration

Unifies Kedro and Dagster logging. TODO + say it is a WIP

## Limitations & Considerations

While Kedro‑Dagster's objective is to provides a powerful bridge between Kedro and Dagster, there are a few important caveats to be aware of:

1. **Dagster for ML pipelines deployment:**
   Dagster originated as a data workflow orchestrator and has been expanding ML support gradually. If you wish to deploy pipelines on your customer's premise and require features like role-based access control, your options may be limited depending on the Dagster deployment setup. Refer to the [Dagster Deployment Options](https://docs.dagster.io/guides/deploy) and connect with the community on the [Dagster Slack](https://dagster.io/slack) for deployment-specific advice.

2. **Evolving feature parity:**
   Both Kedro and Dagster are evolving rapidly; new core features, experimental APIs, or integrations (e.g., sensors, new asset metadata tags) may not yet be exposed in Kedro-Dagster. We encourage you to contribute or raise issues on our [Issue Tracker](https://github.com/gtauzin/kedro-dagster/issues) to prioritize missing functionality.

3. **Compatibility:**
   Both Kedro and Dagster are under active development. Breaking changes in either framework can temporarily affect Kedro‑Dagster integration until a new plugin release addresses them. Always pin your Kedro, Dagster, and Kedro‑Dagster versions and test changes before bumping them.

---

## Contributing & Community

We welcome contributions, feedback, and questions:

- **Report issues or request features:** [GitHub Issues](https://github.com/gtauzin/kedro-dagster/issues)
- **Join the discussion:** [Kedro Slack](https://slack.kedro.org/)
- **Contributing Guide:** [CONTRIBUTING.md](https://github.com/gtauzin/kedro-dagster/blob/main/CONTRIBUTING.md)

---

## Next Steps

- **Getting Started**: Follow our step‑by‑step tutorial in [getting-started.md](getting-started.md).
- **Advanced Example**: Browse the [Example Documentation](example.md) for finding out how to deploy an advanced real-life data science Kedro project with Dagster.
