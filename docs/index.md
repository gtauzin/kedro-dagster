![](images/logo_dark.png#only-dark){width=800}
![](images/logo_light.png#only-light){width=800}

# Welcome to Kedro-Dagster's documentation

Kedro-Dagster is a [Kedro](https://kedro.readthedocs.io/) plugin that enables seamless integration between [Kedro](https://kedro.readthedocs.io/), a framework for creating reproducible and maintainable data science code, and [Dagster](https://dagster.io/), a data orchestrator for data (and machine learning) pipelines. This plugin enables the use of Dagster's orchestration capabilities to deploy, automate, and monitor Kedro pipelines effectively.

<figure markdown>
![Lineage graph of assets involved in the specified jobs](images/getting-started/asset_graph_dark.png#only-dark)
![Lineage graph of assets involved in the specified jobs](images/getting-started/asset_graph_light.png#only-light)
<figcaption>Example of an Asset Lineage Graph.</figcaption>
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

- [Installation](pages/getting-started.md#1-installation)
- [Create a Kedro Project (Optional)](pages/getting-started.md#2-create-a-kedro-project-optional)
- [Initialize Dagster Integration](pages/getting-started.md#3-initialize-dagster-integration)
- [Configure Jobs, Executors, and Schedules](pages/getting-started.md#4-configure-jobs-executors-and-schedules)
- [Run the Dagster UI](pages/getting-started.md#5-run-the-dagster-ui)

### [Example](pages/example.md)

  *A practical example demonstrating how to use Kedro-Dagster in a real project.*

### [Technical Documentation](pages/technical.md)

  *In-depth documentation on the design, architecture, and core concepts of the plugin.*

### [API Reference](pages/api.md)

  *Detailed reference for the Kedro-Dagster API, including classes, functions, and configuration options.*

- [Command Line Interface](pages/api.md#command-line-interface)
- [Configuration](pages/api.md#configuration)
- [Translation Modules](pages/api.md#translation-modules)
- [Utilities](pages/api.md#utilities)

## License

Kedro-Dagster is open source and licensed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
You are free to use, modify, and distribute this software under the terms of this license.
