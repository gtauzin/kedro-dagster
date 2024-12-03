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


## What is Kedro-Dagster?

The Kedro-Dagster plugin enables seamless integration between [Kedro](https://kedro.readthedocs.io/), a framework for creating reproducible and maintainable data science code, and [Dagster](https://dagster.io/), a data orchestrator for machine learning and data pipelines. This plugin makes use of Dagster's orchestration capabilities to automate and monitor Kedro pipelines effectively.


## What are the features of Kedro-Dagster?

- **Dataset Translation**: Converts Kedro datasets into Dagster assets and IO managers, facilitating smooth data handling between the two frameworks.
- **Pipeline Translation**: Transforms Kedro pipelines into Dagster jobs, enabling the execution of Kedro workflows within the Dagster environment.
- **Configuration-Driven Execution and Automation**: Utilizes Kedro's configuration to specify job executors and define schedules, allowing for flexible and dynamic pipeline management.
- **Hook Support**: Preserves Kedro hooks within the Dagster context, ensuring that custom behaviors and extensions are maintained during pipeline execution.
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

3. **Configure Job Executors and Schedules**:

   Define your job executors and schedules in the `dagster.yml` configuration file located in your Kedro project's `conf/<ENV_NAME>` directory. This file allows you to filter Kedro pipelines and assign specific executors and schedules to them.

   ```yaml
   # conf/base/dagster.yml
   schedules:
     my_job_schedule:
       cron_schedule: "0 0 * * *"
   executors:
     my_executor:
        retries: 3
   ```

4. **Launch the Dagster UI**:

   Start the Dagster UI to monitor and manage your pipelines using the following command:

   ```bash
   kedro dagster dev
   ```

## License

This project is licensed under the terms of the [Apache 2.0 License](https://github.com/gtauzin/kedro-dagster/blob/main/LICENSE).

## Acknowledgements

This plugin is inspired by existing Kedro plugins such as [kedro-kubeflow](https://github.com/getindata/kedro-kubeflow) and [kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow).
