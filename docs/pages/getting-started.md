# Getting Started

This guide walks you through setting up and running a Kedro project with Dagster orchestration using the Kedro-Dagster plugin. The examples below use the Kedro `spaceflights-pandas` starter project, but you can use any Kedro project.

## 1. Installation

In your Kedro project directory, install the plugin:

```bash
pip install kedro-dagster
```

## 2. Create a Kedro Project (Optional)

*Skip this step if you already have a Kedro project you want to deploy with Dagster.*

If you don't already have a Kedro project, you can create one using a starter template:

```bash
kedro new --starter=spaceflights-pandas
```

Follow the prompts to set up your project. Once, it is done, install the dependencies of your project:

```bash
cd spaceflights-pandas
pip install -r requirements.txt
```

## 3. Initialize Dagster Integration

Use [`kedro dagster init`](api.md#kedro-dagster-init) to initialize Kedro-Dagster:

```bash
kedro dagster init --env local
```

This creates:

- `src/definitions.py`: Dagster entrypoint file that exposes all translated Kedro objects as Dagster objects.
- `conf/local/dagster.yml`: Dagster configuration file for the `local` Kedro environment.

## 4. Configure Jobs, Executors, and Schedules

The Kedro-Dagster configuration file `dagster.yml` includes the following sections:

- **dev**: TODO
- **schedules**: Used to set up cron schedules for jobs.
- **executors**: Used to specify the compute targets of jobs are run (in-process, multiprocess, k8s, etc).
- **jobs**: Used to describe jobs through the filtering of Kedro pipelines.

We can edit the automatically generated `conf/local/dagster.yml` to customize jobs, executors, and schedules:

```yaml
schedules:
  daily_schedule:
    cron_schedule: "0 0 * * *" # Create a daily schedule

executors:
  sequential_executor:
    in_process:

multiprocess_executor:
    multiprocess:
      max_concurrent: 2

jobs:
  my_job: # Job name
    pipeline: # Pipeline specification
      pipeline_name: __default__

    executor: my_executor
    schedule: daily_schedule

```

See the [Technical Documentation](technical.md) for more on customizing the Dagster configuration file.

## 5. Run the Dagster UI

Use [`kedro dagster dev`](api.md#kedro-dagster-dev) to start the Dagster development server:

```bash
kedro dagster dev --env local
```

The Dagster UI will be available at [http://127.0.0.1:3000](http://127.0.0.1:3000) by default.
You can trigger jobs, monitor runs, and inspect assets and their lineage from the UI.

## Next Steps

- See the [Kedro-Dagster example repository](https://github.com/gtauzin/kedro-dagster-example) for a more advanced example, or visit the [Example](example.md) section.
- Explore the [Technical Documentation](technical.md) for advanced configuration and customization.
- See the [API Reference](api.md) for details on available classes and functions.
