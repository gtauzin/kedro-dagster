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

``` title="definitions.py"
--8<-- "src/kedro_dagster/templates/definitions.py"
```

There's no need to modify the Dagster definitions file to get started. Checkout the [Technical Documentation](technical.md) to find out how to customize it.

- `conf/local/dagster.yml`: Dagster configuration file for the `local` Kedro environment.

``` title="definitions.py"
--8<-- "src/kedro_dagster/templates/dagster.yml"
```


## 4. Configure Jobs, Executors, and Schedules

The Kedro-Dagster configuration file `dagster.yml` includes the following sections:

- **dev**: TODO
- **schedules**: Used to set up cron schedules for jobs.
- **executors**: Used to specify the compute targets of jobs are run (in-process, multiprocess, k8s, etc).
- **jobs**: Used to describe jobs through the filtering of Kedro pipelines.

We can edit the automatically generated `conf/local/dagster.yml` to customize jobs, executors, and schedules:

```yaml
schedules:
  daily: # Schedule name
    cron_schedule: "0 0 * * *" # Schedule parameters

executors: # Executor name
  sequential: # Executor parameters
    in_process:

  multiprocess:
    multiprocess:
      max_concurrent: 2

jobs:
  default: # Job name
    pipeline: # Pipeline filter parameters
      pipeline_name: __default__
    executor: sequential

  parallel_data_processing:
    pipeline:
      pipeline_name: data_processing
      node_names:
      - preprocess_companies_node
      - preprocess_shuttles_node
    schedule: daily
    executor: multiprocess

  data_science:
    pipeline:
      pipeline_name: data_processing
      node_names:
      - preprocess_companies_node
      - preprocess_shuttles_node
    schedule: daily
    executor: sequential

```

See the [Technical Documentation](technical.md) for more on customizing the Dagster configuration file.

## 5. Run the Dagster UI

Use [`kedro dagster dev`](api.md#kedro-dagster-dev) to start the Dagster development server:

```bash
kedro dagster dev --env local
```

The Dagster UI will be available at [http://127.0.0.1:3000](http://127.0.0.1:3000) by default.

You can inspect assets jobs and resources, trigger, automatize jobs, and monitor runs from the UI.

### Assets

Moving to the "Assets" tab, you'll find the list of assets generated from the Kedro datasets involved in the filtered pipelines specified in `dasgter.yml`.

<figure markdown>
![List of assets involved in the specified jobs](../images/getting-started/asset_list_dark.png#only-dark){data-gallery="assets-dark"}
![List of assets involved in the specified jobs](../images/getting-started/asset_list_light.png#only-light){data-gallery="assets-light"}
<figcaption>Asset List.</figcaption>
</figure>

<figure markdown>
![Lineage graph of assets involved in the specified jobs](../images/getting-started/asset_graph_dark.png#only-dark){data-gallery="assets-dark"}
![Lineage graph of assets involved in the specified jobs](../images/getting-started/asset_graph_light.png#only-light){data-gallery="assets-light"}
<figcaption>Asset Lineage Graph.</figcaption>
</figure>

### Resources

<figure markdown>
![List of the resources involved in the specified jobs](../images/getting-started/resource_list_dark.png#only-dark){data-gallery="resources-dark"}
![List of the resources involved in the specified jobs](../images/getting-started/resource_list_light.png#only-light){data-gallery="resources-light"}
<figcaption>Resource list.</figcaption>
</figure>

### Jobs

<figure markdown>
![List of the specified jobs](../images/getting-started/job_list_dark.png#only-dark){data-gallery="jobs-dark"}
![List of the specified jobs](../images/getting-started/job_list_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job List.</figcaption>
</figure>

<figure markdown>
![Graph describing the "parallel_data_processing" job](../images/getting-started/job_graph_dark.png#only-dark){data-gallery="jobs-dark"}
![Graph describing the "parallel_data_processing" job](../images/getting-started/job_graph_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Graph.</figcaption>
</figure>

<figure markdown>
![Launchpad for the "parallel_data_processing" job](../images/getting-started/job_launchpad_dark.png#only-dark){data-gallery="jobs-dark"}
![Launchpad for the "parallel_data_processing" job](../images/getting-started/job_launchpad_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Laumchpad.</figcaption>
</figure>

<figure markdown>
![Running the "parallel_data_processing" job](../images/getting-started/job_running_dark.png#only-dark){data-gallery="jobs-dark"}
![Running the "parallel_data_processing" job](../images/getting-started/job_running_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Run Timeline.</figcaption>
</figure>

### Automation

<figure markdown>
![List of the schedules and sensors involved in the specified jobs](../images/getting-started/automation_list_dark.png#only-dark){data-gallery="automation-dark"}
![List of the schedules and sensors involved in the specified jobs](../images/getting-started/automation_list_light.png#only-light){data-gallery="automation-light"}
<figcaption>Resource List.</figcaption>
</figure>

---

## Next Steps

- See the [Kedro-Dagster example repository](https://github.com/gtauzin/kedro-dagster-example) for a more advanced example, or visit the [Example](example.md) section.
- Explore the [Technical Documentation](technical.md) for advanced configuration and customization.
- See the [API Reference](api.md) for details on available classes and functions.
