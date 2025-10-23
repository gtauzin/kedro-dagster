![](images/logo_dark.png#only-dark){width=800}
![](images/logo_light.png#only-light){width=800}

# Welcome to Kedro-Dagster's documentation

Kedro-Dagster is a [Kedro](https://kedro.readthedocs.io/) plugin that enables seamless integration between [Kedro](https://kedro.readthedocs.io/), a framework for creating reproducible and maintainable data science code, and [Dagster](https://dagster.io/), a data orchestrator for data (and machine learning) pipelines. This plugin enables the use of Dagster's orchestration capabilities to deploy, automate, and monitor Kedro pipelines effectively.

<figure markdown>
![Lineage graph of assets involved in the specified jobs](images/getting-started/asset_graph_dark.png#only-dark)
![Lineage graph of assets involved in the specified jobs](images/getting-started/asset_graph_light.png#only-light)
<figcaption>Example of a Dagster Asset Lineage Graph generated from a Kedro project.</figcaption>
</figure>

## Table of Contents

### [Introduction](pages/intro.md)

  *Overview of Kedro-Dagster, its purpose, and key concepts.*

- [What Is Kedro?](pages/intro.md#what-is-kedro)
- [What Is Dagster?](pages/intro.md#what-is-dagster)
- [Why Kedro‑Dagster?](pages/intro.md#why-kedrodagster)
- [Key features](pages/intro.md#key-features)
- [Limitations & Considerations](pages/intro.md#limitations-and-considerations)
- [Contributing & Community](pages/intro.md#contributing-and-community)

### [Getting Started](pages/getting-started.md)

  *Step-by-step guide to installing and setting up Kedro-Dagster in your project.*

- [1. Create a Kedro Project (Optional)](pages/getting-started.md#1-create-a-kedro-project-optional)
- [2. Installation](pages/getting-started.md#2-installation)
- [3. Initialize Dagster Integration](pages/getting-started.md#3-initialize-dagster-integration)
- [4. Configure Jobs, Executors, and Schedules](pages/getting-started.md#4-configure-jobs-executors-and-schedules)
- [5. Browse the Dagster UI](pages/getting-started.md#5-browse-the-dagster-ui)

### [Example](pages/example.md)

  *A practical example demonstrating how to use Kedro-Dagster in a real project.*

- [Project Overview](pages/example.md#project-overview)
- [Quick Start](pages/example.md#quick-start)

### [Technical Documentation](pages/technical.md)

  *In-depth documentation on the design, architecture, and core concepts of the plugin.*

- [Project Configuration](pages/technical.md#project-configuration)
- [Kedro-Dagster Concept Mapping](pages/technical.md#kedro-dagster-concept-mapping)
- [Compatibility issues between Kedro and Dagster](pages/technical.md#compatibility-issues-between-kedro-and-dagster)

### [API Reference](pages/api.md)

  *Detailed reference for the Kedro-Dagster API, including classes, functions, and configuration options.*

- [Command Line Interface](pages/api.md#command-line-interface)
- [Configuration](pages/api.md#configuration)
- [Translation Modules](pages/api.md#translation-modules)
- [Datasets](pages/api.md#datasets)
- [Utilities](pages/api.md#utilities)

## License

Kedro-Dagster is open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
You are free to use, modify, and distribute this software under the terms of this license.
