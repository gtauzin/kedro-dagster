# Getting Started

This guide walks you through setting up and running a Kedro project with Dagster orchestration using the Kedro‑Dagster plugin. The examples below use the Kedro `spaceflights-pandas` starter project, but you can use any Kedro project.

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

Follow the prompts to set up your project. Once it is done, install the dependencies of your project:

```bash
cd spaceflights-pandas
pip install -r requirements.txt
```

## 3. Initialize Dagster Integration

Use [`kedro dagster init`](api.md#kedro-dagster-init) to initialize Kedro‑Dagster:

```bash
kedro dagster init --env local
```

This creates:

- `src/definitions.py`: Dagster entrypoint file that exposes all translated Kedro objects as Dagster objects.

``` title="definitions.py"
--8<-- "src/kedro_dagster/templates/definitions.py"
```

There's no need to modify the Dagster definitions file to get started. Check out the [Technical Documentation](technical.md) to find out how to customize it.

- `conf/local/dagster.yml`: Dagster configuration file for the `local` Kedro environment.

``` title="dagster.yml"
--8<-- "src/kedro_dagster/templates/dagster.yml"
```

## 4. Configure Jobs, Executors, and Schedules

The Kedro‑Dagster configuration file `dagster.yml` includes the following sections:

- **schedules:** Used to set up cron schedules for jobs.
- **executors:** Used to specify the compute targets for jobs (in-process, multiprocess, k8s, etc).
- **jobs:** Used to describe jobs through the filtering of Kedro pipelines.

You can edit the automatically generated `conf/local/dagster.yml` to customize jobs, executors, and schedules:

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

## 5. Browse the Dagster UI

Use [`kedro dagster dev`](api.md#kedro-dagster-dev) to start the Dagster development server:

```bash
kedro dagster dev --env local
```

!! NOTE
The `dagster.yml` file also include a **dev** section, containing default parameters for the [`kedro dagster dev`](api.md#kedro-dagster-dev) command. Check out

The Dagster UI will be available at [http://127.0.0.1:3000](http://127.0.0.1:3000) by default.

You can inspect assets, jobs, and resources, trigger or automate jobs, and monitor runs from the UI.

### Assets

Moving to the "Assets" tab leads to the list of assets generated from the Kedro datasets involved in the filtered pipelines specified in `dagster.yml`.

<figure markdown>
![List of assets involved in the specified jobs](../images/getting-started/asset_list_dark.png#only-dark){data-gallery="assets-dark"}
![List of assets involved in the specified jobs](../images/getting-started/asset_list_light.png#only-light){data-gallery="assets-light"}
<figcaption>Asset List.</figcaption>
</figure>

Each asset is prefixed by the Kedro environment that was passed to the [`KedroProjectTranslator`](api.md#kedroprojecttranslator) in [`definitions.py`](api.md#definitionspy). If the Kedro dataset was generated from a [dataset factory](), the namespace that prefixed its name will also appear as a prefix, allowing easy browsing of assets per environment and per namespace.

!! NOTE
`base` environment

Clicking on the "Asset lineage" link at the top right of the window leads to the Dagster asset lineage graph, where you can observe the dependencies between assets and check their status and description.

!! NOTE
description in metadata

<figure markdown>
![Lineage graph of assets involved in the specified jobs](../images/getting-started/asset_graph_dark.png#only-dark){data-gallery="assets-dark"}
![Lineage graph of assets involved in the specified jobs](../images/getting-started/asset_graph_light.png#only-light){data-gallery="assets-light"}
<figcaption>Asset Lineage Graph.</figcaption>
</figure>

### Resources

Kedro‑Dagster defines one Dagster IO Manager per Kedro Dataset to handle their saving/loading. As with assets, they are defined per Kedro environment and their name is prefixed accordingly.

#### TODO naming convention section in technical doc

<figure markdown>
![List of the resources involved in the specified jobs](../images/getting-started/resource_list_dark.png#only-dark){data-gallery="resources-dark"}
![List of the resources involved in the specified jobs](../images/getting-started/resource_list_light.png#only-light){data-gallery="resources-light"}
<figcaption>Resource list.</figcaption>
</figure>

### Automation

Moving to the "Automation" tab, you can see a list of the defined schedules and sensors.

<figure markdown>
![List of the schedules and sensors involved in the specified jobs](../images/getting-started/automation_list_dark.png#only-dark){data-gallery="automation-dark"}
![List of the schedules and sensors involved in the specified jobs](../images/getting-started/automation_list_light.png#only-light){data-gallery="automation-light"}
<figcaption>Schedule and Sensor List.</figcaption>
</figure>

### Jobs

To see the different jobs defined in `dagster.yml`, click on the "Jobs" tab.

<figure markdown>
![List of the specified jobs](../images/getting-started/job_list_dark.png#only-dark){data-gallery="jobs-dark"}
![List of the specified jobs](../images/getting-started/job_list_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job List.</figcaption>
</figure>

Clicking on the "parallel_data_processing" job brings you to a graph representation of the corresponding Dagster-translated Kedro pipeline. `before_pipeline_run` and `after_pipeline_run` are included as the first and final nodes of the job graph.

<figure markdown>
![Graph describing the "parallel_data_processing" job](../images/getting-started/job_graph_dark.png#only-dark){data-gallery="jobs-dark"}
![Graph describing the "parallel_data_processing" job](../images/getting-started/job_graph_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Graph.</figcaption>
</figure>

The job can be run by clicking on the "Launchpad" sub-tab. The Kedro pipeline, its parameters (mapped to Dagster Config), and the Kedro datasets (mapped to IO managers) can be modified before launching a run.

<figure markdown>
![Launchpad for the "parallel_data_processing" job](../images/getting-started/job_launchpad_dark.png#only-dark){data-gallery="jobs-dark"}
![Launchpad for the "parallel_data_processing" job](../images/getting-started/job_launchpad_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Launchpad.</figcaption>
</figure>

<figure markdown>
![Running the "parallel_data_processing" job](../images/getting-started/job_running_dark.png#only-dark){data-gallery="jobs-dark"}
![Running the "parallel_data_processing" job](../images/getting-started/job_running_light.png#only-light){data-gallery="jobs-light"}
<figcaption>Job Run Timeline.</figcaption>
</figure>

---

## Next Steps

- Visit the [Example](example.md) section for a more advanced example.
- Explore the [Technical Documentation](technical.md) for advanced configuration and customization.
- See the [API Reference](api.md) for details on available classes and functions.
