<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/gtauzin/kedro-dagster/main/.github/logo-light.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/gtauzin/kedro-dagster/main/.github/logo-dark.png">
    <img src="https://raw.githubusercontent.com/gtauzin/kedro-dagster/main/.github/logo-light.png" alt="Kedro-Dagster">
  </picture>
</p>

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)
[![Python Version](https://img.shields.io/pypi/pyversions/kedro-dagster)](https://pypi.org/project/kedro-dagster/)
[![License](https://img.shields.io/github/license/gtauzin/kedro-dagster)](https://github.com/gtauzin/kedro-dagster/blob/main/LICENSE)
[![PyPI Version](https://img.shields.io/pypi/v/kedro-dagster)](https://pypi.org/project/kedro-dagster/)
[![Run tests and checks](https://github.com/gtauzin/kedro-dagster/actions/workflows/check.yml/badge.svg)](https://github.com/gtauzin/kedro-dagster/actions/workflows/check.yml)
[![Slack Organisation](https://img.shields.io/badge/slack-chat-blueviolet.svg?label=Kedro%20Slack&logo=slack)](https://slack.kedro.org)

## What is Kedro-Dagster?

The Kedro-Dagster plugin enables seamless integration between [Kedro](https://kedro.readthedocs.io/), a framework for creating reproducible and maintainable data science code, and [Dagster](https://dagster.io/), a data orchestrator for machine learning and data pipelines. This plugin makes use of Dagster's orchestration capabilities to automate and monitor Kedro pipelines effectively.


## What are the features of Kedro-Dagster?

- **Dataset Translation**: Converts Kedro datasets into Dagster assets and IO managers, facilitating smooth data handling between the two frameworks.
- **Pipeline Translation**: Transforms Kedro pipelines into Dagster jobs, enabling their execution and scheduling.
- **Configuration-Driven Execution and Automation**: Utilizes Kedro's configuration to specify job executors and define schedules, allowing for flexible and dynamic pipeline management.
- **Hook Support**: Preserves Kedro hooks within the Dagster context, ensuring that custom behaviors and plugins are maintained during pipeline execution.
- **Logger Integration**: Integrates Kedro's logging with Dagster's logging system, providing unified and comprehensive logging across both platforms.

## How to install Kedro-Dagster?

Install the Kedro-Dagster plugin using pip:

```bash
pip install kedro-dagster
```

## How to get started with Kedro-Dagster?

1. **Initialize the Plugin in Your Kedro Project**:

   Navigate to your Kedro project directory and install the plugin:

   ```bash
   pip install kedro-dagster
   ```

2. **Generate Dagster Definitions and Configuration**:

   Use the following command to generate a `definitions.py` file, where all translated Kedro objects are available as Dagster objects, and a `dagster.yml` configuration file:

   ```bash
   kedro dagster init --env <ENV_NAME>
   ```

3. **Configure Jobs, Executors, and Schedules**:

   Define your job executors and schedules in the `dagster.yml` configuration file located in your Kedro project's `conf/<ENV_NAME>` directory. This file allows you to filter Kedro pipelines and assign specific executors and schedules to them.

   ```yaml
   # conf/base/dagster.yml
   schedules:
     my_job_schedule:
       cron_schedule: "0 0 * * *"
   executors:
     my_executor:
        retries: 3
   jobs:
     my_job:
       pipeline_name: __default__

       executor: my_executor
       schedule: my_job_schedule

   ```

4. **Launch the Dagster UI**:

   Start the Dagster UI to monitor and manage your pipelines using the following command:

   ```bash
   kedro dagster dev
   ```

## How do I use Kedro?

The [Kedro-Dagster documentation](https://gtauzin.github.io/kedro-dagster/) will be available soon, stay tuned!

## Can I contribute?

Yes! We welcome all kinds of contributions. Check out our [guide to contributing to Kedro](https://github.com/kedro-org/kedro/wiki/Contribute-to-Kedro).

## Where can I learn more?

There is a growing community around the Kedro project. We encourage you to ask and answer technical questions on the Kedro [Slack](https://slack.kedro.org/) and bookmark the [Linen archive of past discussions](https://linen-slack.kedro.org/).

## License

This project is licensed under the terms of the [Apache 2.0 License](https://github.com/gtauzin/kedro-dagster/blob/main/LICENSE).

## Acknowledgements

This plugin is inspired by existing Kedro plugins such as [kedro-kubeflow](https://github.com/getindata/kedro-kubeflow) and [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow).
