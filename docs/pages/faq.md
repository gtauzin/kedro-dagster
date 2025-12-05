# Frequently Asked Questions

This guide answers common questions and helps you diagnose and resolve issues when using Kedro-Dagster.

## Getting Started

### Do I need to modify my Kedro code?

**No!** Kedro-Dagster works with existing Kedro projects without code changes.

The only modifications needed are:

1. **Add Kedro-Dagster to dependencies**:

    ```bash
    pip install kedro-dagster
    ```

2. **Initialize the plugin**:

    ```bash
    kedro dagster init --env local
    ```

3. **(Optional) Use Kedro-Dagster logging in node functions**:

    ```python
    def my_node(data):
        from kedro_dagster.logging import getLogger
        logger = getLogger(__name__)
        logger.info("This appears in Dagster UI")
        # ... rest of node code
    ```

Everything else (catalog, pipelines, parameters, hooks) works as-is.

---

### Can I use both Kedro and Dagster CLI?

**Yes!** You can mix both:

```bash
# Kedro commands work normally
kedro run --env local
kedro run --pipeline=data_processing

# Dagster commands via Kedro wrapper
kedro dagster dev --env local
kedro dagster list defs --env local

# Or use Dagster CLI directly (requires setting KEDRO_ENV)
export KEDRO_ENV=local
dg dev
```

The Kedro-Dagster CLI wrapper (`kedro dagster <command>`) automatically sets the Kedro environment for you.

---

### Is Kedro-Dagster production-ready?

**Yes**, with caveats:

- **Production strengths**:

    - Actively maintained and used in real projects
    - Comprehensive test coverage
    - Supports stable Kedro and Dagster versions (Kedro >= 0.19, Dagster >= 1.10)

- **Production considerations**:

    - **Newer project**: Less mature than Kedro or Dagster alone
    - **Experimental features**: Some features like Dagster partitioning support are marked experimental
    - **Limited executors tested**: Most testing on in-process and multiprocess (open an issue if any executor has problems)
    - **Pin versions**: Both Kedro and Dagster release frequently

---

## Configuration

### How do I use environment-specific configurations?

Kedro-Dagster respects Kedro environments. Create different configurations per environment:

```
conf/
  ├── local/
  │   ├── catalog.yml
  │   ├── dagster.yml  # Local executor, no schedule
  │   └── parameters.yml
  ├── staging/
  │   ├── catalog.yml
  │   ├── dagster.yml  # Multiprocess, test schedule
  │   └── parameters.yml
  └── prod/
      ├── catalog.yml
      ├── dagster.yml  # K8s executor, production schedule
      └── parameters.yml
```

Run with `kedro dagster dev --env <ENV>` to load that environment's config.

---

### How do I configure multiple jobs from one pipeline?

Use different pipeline filters in `dagster.yml`:

```yaml
jobs:
  preprocessing_only:
    pipeline:
      pipeline_name: data_processing
      to_nodes:
        - preprocess_companies_node
        - preprocess_shuttles_node
    executor: multiprocess

  full_data_processing:
    pipeline:
      pipeline_name: data_processing
    executor: sequential
```

Each job is a different slice of the same Kedro pipeline.

---

### How do I use different executors per job?

Define executors in `dagster.yml` and assign them:

```yaml
executors:
  local:
    in_process:

  parallel:
    multiprocess:
      max_concurrent: 4

  kubernetes:
    k8s:
      # K8s config...

jobs:
  dev_job:
    pipeline:
      pipeline_name: __default__
    executor: local

  prod_job:
    pipeline:
      pipeline_name: __default__
    executor: kubernetes
```

---

### How do I schedule jobs?

Define schedules and assign them to jobs in `dagster.yml`:

```yaml
schedules:
  daily_midnight:
    cron_schedule: "0 0 * * *"

  hourly:
    cron_schedule: "0 * * * *"

jobs:
  nightly_etl:
    pipeline:
      pipeline_name: data_processing
    schedule: daily_midnight

  realtime_updates:
    pipeline:
      pipeline_name: feature_refresh
    schedule: hourly
```

---

### Can I customize the generated definitions.py?

**Yes!** The `definitions.py` file is auto-generated initially but can be edited.

**Common customizations**:

1. **Add custom resources**:

    ```python
    from dagster import resource, Definitions

    @resource
    def my_custom_resource(context):
        return MyServiceClient()

    # Add to definitions
    defs = Definitions(
        assets=...,
        resources={**resources, "my_resource": my_custom_resource}
    )
    ```

2. **Add custom sensors**:

    ```python
    from dagster import sensor

    @sensor(job=my_job)
    def my_sensor(context):
        # Custom sensor logic
        pass

    # Add to definitions
    defs = Definitions(
        assets=...,
            sensors=list(dagster_code_location.named_sensors.values() + [my_sensor]),
    )

    ```

---

## Kedro Integration

### What happens to my Kedro hooks?

**All Kedro hooks are preserved and called at the appropriate times!**

Kedro-Dagster ensures hooks fire during Dagster execution:

- **Context and catalog hooks**: (`after_context_created`, `after_catalog_created`): Called before pipeline execution to mimic a Kedro run started with `kedro run`
- **Pipeline hooks** (`before_pipeline_run`, `after_pipeline_run`): Executed as dedicated ops in the job
- **Dataset hooks** (`before_dataset_loaded`, `after_dataset_loaded`, `before_dataset_saved`, `after_dataset_saved`): Called in Dagster IO managers
- **Node hooks** (`before_node_run`, `after_node_run`, `on_node_error`): Wrapped in Dagster ops
- **Pipeline error hooks** (`on_pipeline_error`): Triggered via Dagster sensors in case of run failures

This means integrations like `kedro-mlflow` work automatically without Dagster-specific code.

---

### How does MLflow integration work?

If you have `kedro-mlflow` installed and configured:

1. **Kedro-MLflow hooks fire automatically** during Dagster runs
2. **MLflow metadata appears in Dagster logs** (experiment ID, run ID, tracking URI)
3. **No Dagster-specific MLflow code needed**: it just works!
4. **Call MLflow functions in nodes as usual**

---

## Common Issues

### Why don't I see my logs in Dagster UI?

Make sure you're using `kedro_dagster.logging.getLogger` **inside your node functions**:

```python
def process_data(data):
    # Import inside the function!
    from kedro_dagster.logging import getLogger
    logger = getLogger(__name__)

    logger.info("This will appear in Dagster UI")
    return processed_data
```

Module-level loggers won't capture Dagster context. See [Logging documentation](guide.md#logging) for details.

---

### Why are my asset names formatted differently in Dagster?

Kedro-Dagster converts dots (`.`) to double underscores (`__`) in asset names to comply with Dagster's naming requirements.

**Kedro**: `namespace.my_dataset`
**Dagster**: `local__namespace__my_dataset`

This is automatic and reversible—Kedro-Dagster maps them back internally. Prefer using underscores and dots in Kedro dataset names to avoid issues with other special characters.

See [Naming Conventions](guide.md#naming-conventions) for details.

---

### Why did my job fail with "NotImplementedError"?

You're likely using an unsupported partition type:

- **Supported**: `StaticPartitionsDefinition`, `StaticPartitionMapping`, `IdentityPartitionMapping`
- **Not supported**: `TimeWindowPartitionsDefinition`, `DynamicPartitionsDefinition`, `MultiPartitionsDefinition`

**Solution**: Use `StaticPartitionsDefinition` with explicit partition keys, or use Dagster schedules for time-based execution.

See [Partitions documentation](guide.md#integrating-dagster-partitions) for examples.

---

### UI not reflecting configuration changes?

**Solution**:

1. Stop the Dagster development server
2. Restart with `kedro dagster dev --env <ENV>`
3. Hard refresh your browser (Ctrl+Shift+R or Cmd+Shift+R)

Some configuration changes require a full server restart and aren't hot-reloaded.

---

## Debugging Guide

When encountering an issue, follow this systematic approach:

### 1. Check Logs

- **Terminal output**: Look for error messages and stack traces where you ran `kedro dagster dev`
- **Dagster UI logs**: Navigate to the failed run → Click the failed op/asset → Check "Logs" tab

### 2. Verify Configuration

```bash
# Check YAML syntax
python -c "import yaml; yaml.safe_load(open('conf/local/dagster.yml'))"

# Verify Kedro project loads
kedro run --dry-run --env local

# See what Dagster sees
kedro dagster list defs --env local
```

### 3. Test in Isolation

```bash
# Bypass Dagster to isolate Kedro issues
kedro run --env local

# Test single node
kedro run --node=<node_name> --env local
```

Or use the Dagster UI to materialize a single asset and check if the issue is pipeline-wide or asset-specific.

### 4. Search GitHub Issues

Search [Kedro-Dagster Issues](https://github.com/gtauzin/kedro-dagster/issues) for similar problems.

If not found, [open a new issue](https://github.com/gtauzin/kedro-dagster/issues/new) with:

- Versions: `kedro --version`, `dagster --version`, `pip show kedro-dagster`
- Minimal reproducible example
- Error message and stack trace
- Configuration files (sanitized)

---

## Advanced Topics

### How do I contribute to Kedro-Dagster?

We welcome contributions! See [CONTRIBUTING.md](https://github.com/gtauzin/kedro-dagster/blob/main/CONTRIBUTING.md) for:

- Setting up development environment
- Running tests
- Code style guidelines
- Submitting pull requests

For major features, [open an issue](https://github.com/gtauzin/kedro-dagster/issues/new) first to discuss design.

---

## Still Need Help?

- **Documentation**: [Full Documentation](../index.md)
- **Community**: [Kedro Slack](https://slack.kedro.org/)
- **Discussions**: [GitHub Discussions](https://github.com/gtauzin/kedro-dagster/discussions)
- **Bug Reports**: [GitHub Issues](https://github.com/gtauzin/kedro-dagster/issues)

When asking for help, include:

- Kedro, Dagster, and Kedro-Dagster versions
- Minimal reproducible example
- Complete error messages
- What you've already tried
