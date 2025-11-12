"""Configuration definitions for Kedro-Dagster jobs.

These pydantic models describe the shape of the `dagster.yml` entries used to
translate Kedro pipelines into Dagster jobs, including pipeline filtering and
executor/schedule selection.
"""

from pydantic import BaseModel

from kedro_dagster.utils import KEDRO_VERSION

from .automation import ScheduleOptions
from .execution import ExecutorOptions
from .logging import LoggerOptions


class PipelineOptions(BaseModel):
    """Options for filtering and configuring Kedro pipelines within a Dagster job.

    Attributes:
        pipeline_name (str): Name of the Kedro pipeline to run. Defaults to `__default__`.
        from_nodes (list[str] | None): List of node names to start execution from.
        to_nodes (list[str] | None): List of node names to end execution at.
        node_names (list[str] | None): List of specific node names to include in the pipeline.
        from_inputs (list[str] | None): List of dataset names to use as entry points.
        to_outputs (list[str] | None): List of dataset names to use as exit points.
        node_namespace(s) (list[str] | None): Namespace(s) to filter nodes by. For Kedro >= 1.0, the
            filter key is "node_namespaces" (plural) and must be a list of strings; for older
            versions, it is "node_namespace" (singular string).
        tags (list[str] | None): List of tags to filter nodes by.

    Example:

    ```yaml
    jobs:
      sales_etl:
        pipeline:
          pipeline_name: etl
          node_namespaces: ["sales", "shared"]  # or node_namespace: "sales" for Kedro < 1.0
          tags: ["daily", "priority"]
          from_nodes: ["extract_raw_sales"]
          to_nodes: ["publish_clean_sales"]
          from_inputs: ["raw_sales"]
          to_outputs: ["clean_sales"]
    ```
    """

    pipeline_name: str = "__default__"
    from_nodes: list[str] | None = None
    to_nodes: list[str] | None = None
    node_names: list[str] | None = None
    from_inputs: list[str] | None = None
    to_outputs: list[str] | None = None
    # Kedro 1.x renamed the namespace filter kwarg to `node_namespaces` (plural).
    # Expose the appropriate field name based on the installed Kedro version while
    # keeping the rest of the configuration stable.
    if KEDRO_VERSION[0] >= 1:
        node_namespaces: list[str] | None = None
    else:  # pragma: no cover
        node_namespace: str | None = None
    tags: list[str] | None = None

    class Config:
        """Pydantic configuration enforcing strict fields."""

        extra = "forbid"


class JobOptions(BaseModel):
    """Configuration options for a Dagster job.

    Attributes:
        pipeline (PipelineOptions): PipelineOptions specifying which pipeline and nodes to run.
        executor (ExecutorOptions | str | None): ExecutorOptions instance or string key referencing an executor.
        schedule (ScheduleOptions | str | None): ScheduleOptions instance or string key referencing a schedule.
        loggers (list[LoggerOptions | str] | None): List of logger configurations (inline LoggerOptions)
            or list of logger names (strings) to attach to the job. Can mix both types.

    Example:

    ```yaml
    jobs:
      my_data_processing:
        pipeline:
          pipeline_name: data_processing
          node_namespaces: [price_predictor]
          tags: [test1]
        executor: multiprocessing   # references an executor name defined under executors:
        schedule: daily_schedule    # optional, references a schedule name defined under schedules:
        loggers: [console, file_logger]
    ```
    """

    pipeline: PipelineOptions
    executor: ExecutorOptions | str | None = None
    schedule: ScheduleOptions | str | None = None
    loggers: list[LoggerOptions | str] | None = None

    class Config:
        """Pydantic configuration enforcing strict fields."""

        extra = "forbid"
