"""Configuration definitions for Kedro-Dagster jobs.

These pydantic models describe the shape of the `dagster.yml` entries used to
translate Kedro pipelines into Dagster jobs, including pipeline filtering and
executor/schedule selection.
"""

from pydantic import BaseModel

from kedro_dagster.utils import _kedro_version

from .automation import ScheduleOptions
from .execution import ExecutorOptions


class PipelineOptions(BaseModel):
    """Options for filtering and configuring Kedro pipelines within a Dagster job.

    Attributes:
        pipeline_name (str | None): Name of the Kedro pipeline to run.
        from_nodes (list[str] | None): List of node names to start execution from.
        to_nodes (list[str] | None): List of node names to end execution at.
        node_names (list[str] | None): List of specific node names to include in the pipeline.
        from_inputs (list[str] | None): List of dataset names to use as entry points.
        to_outputs (list[str] | None): List of dataset names to use as exit points.
        node_namespace(s) (str | None): Namespace to filter nodes by. For Kedro >= 1.0, the
            filter key is "node_namespaces" (plural); for older versions, it is "node_namespace".
        tags (list[str] | None): List of tags to filter nodes by.
    """

    pipeline_name: str | None = None
    from_nodes: list[str] | None = None
    to_nodes: list[str] | None = None
    node_names: list[str] | None = None
    from_inputs: list[str] | None = None
    to_outputs: list[str] | None = None
    # Kedro 1.x renamed the namespace filter kwarg to `node_namespaces` (plural).
    # Expose the appropriate field name based on the installed Kedro version while
    # keeping the rest of the configuration stable.
    if _kedro_version()[0] >= 1:
        node_namespaces: str | None = None
    else:
        node_namespace: str | None = None
    tags: list[str] | None = None

    class Config:
        """Pydantic configuration enforcing strict fields."""

        extra = "forbid"

    # Backward-compat attribute for tests and callers that reference
    # `node_namespace` even under Kedro >= 1.0 where the field is pluralized.
    if _kedro_version()[0] >= 1:

        @property
        def node_namespace(self) -> str | None:  # pragma: no cover - thin alias
            return getattr(self, "node_namespaces", None)


class JobOptions(BaseModel):
    """Configuration options for a Dagster job.

    Attributes:
        pipeline (PipelineOptions): PipelineOptions specifying which pipeline and nodes to run.
        executor (ExecutorOptions | str | None): ExecutorOptions instance or string key referencing an executor.
        schedule (ScheduleOptions | str | None): ScheduleOptions instance or string key referencing a schedule.
    """

    pipeline: PipelineOptions
    executor: ExecutorOptions | str | None = None
    schedule: ScheduleOptions | str | None = None

    class Config:
        extra = "forbid"
