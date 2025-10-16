"""Translation of Kedro pipelines to Dagster jobs."""

from typing import TYPE_CHECKING, Any

import re
import dagster as dg
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline

from kedro_dagster.kedro import KedroRunTranslator
from kedro_dagster.utils import (
    format_dataset_name,
    format_node_name,
    _is_param_name,
    get_asset_key_from_dataset_name,
    get_filter_params_dict,
    is_mlflow_enabled,
)

if TYPE_CHECKING:
    from kedro.framework.context import KedroContext


def _serialize_partition_key(pk: Any) -> str:
    """Serialize a partition key into a Dagster-safe suffix (^[A-Za-z0-9_]+$)."""
    try:
        if isinstance(pk, dg.MultiPartitionKey):
            s = "|".join(f"{k}={v}" for k, v in sorted(pk.keys_by_dimension.items()))
        else:
            s = str(pk)
    except Exception:
        s = str(pk)
    # Replace any non-alphanumeric/underscore with underscore and trim
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    s = s.strip("_") or "all"
    return s


class PipelineTranslator:
    """Translator for Kedro pipelines to Dagster jobs.

    Args:
        dagster_config (dict[str, Any]): The configuration of the Dagster job.
        context (KedroContext): The Kedro context.
        project_path (str): The path to the Kedro project.
        env (str): The Kedro environment.
        session_id (str): The Kedro session ID.
        named_assets (dict[str, AssetsDefinition]): The named assets.
        named_ops (dict[str, OpDefinition]): The named ops.
        named_resources (dict[str, ResourceDefinition]): The named resources.
        named_executors (dict[str, ExecutorDefinition]): The named executors.
    """

    def __init__(
        self,
        dagster_config: dict[str, Any],
        context: "KedroContext",
        project_path: str,
        env: str,
        session_id: str,
        named_assets: dict[str, dg.AssetsDefinition],
        asset_partitions: dict[str, Any],
        named_ops: dict[str, dg.OpDefinition],
        named_resources: dict[str, dg.ResourceDefinition],
        named_executors: dict[str, dg.ExecutorDefinition],
        enable_mlflow: bool,
    ):
        self._dagster_config = dagster_config
        self._context = context
        self._project_path = project_path
        self._env = env
        self._session_id = session_id
        self._catalog = context.catalog
        self._hook_manager = context._hook_manager
        self._named_assets = named_assets
        self._asset_partitions = asset_partitions
        self._named_ops = named_ops
        self._named_resources = named_resources
        self._named_executors = named_executors
        self._enable_mlflow = enable_mlflow

    def _get_partition_mapping(self, upstream_asset: str, downstream_asset: str):
        """Return a callable mapping upstream_key -> list[downstream_key] if provided, else None.

        The mapping is expected in translator._asset_partitions[upstream_asset]["mappings"][downstream_asset].
        """
        ap = getattr(self, "_asset_partitions", {}) or {}
        spec = ap.get(upstream_asset) or {}
        mappings = spec.get("mappings") or {}
        mapping = mappings.get(downstream_asset)
        if callable(mapping):
            return mapping
        # Future: support Dagster PartitionMapping objects by adapter wrappers.
        return None

    def _enumerate_partition_keys_for_asset(self, asset_name: str) -> list[Any]:
        """Enumerate partition keys for an asset, supports Multi and regular partitions."""
        pdef = None
        if asset_name in self._asset_partitions:
            pdef = self._asset_partitions[asset_name].get("partitions_def")

        if not pdef:
            return []
        if isinstance(pdef, dg.MultiPartitionsDefinition) and hasattr(pdef, "get_multi_partition_keys"):
            try:
                return list(pdef.get_multi_partition_keys())  # type: ignore[attr-defined]
            except Exception:
                pass
        try:
            return list(getattr(pdef, "get_partition_keys")())
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"Cannot enumerate partition keys for asset '{asset_name}': {exc}")

    def _node_execution_keys(self, node) -> list[Any]:
        """Decide execution partition keys for a node.

        Strategy:
        - If any output is partitioned: use that partition space (validate if multiple outputs share identical key sets).
        - Else if any input is partitioned: use that partition space (validate if multiple inputs share identical key sets).
        - Else: unpartitioned -> [].
        """
        out_key_sets: list[list[Any]] = []
        for ds in node.outputs:
            asset_name = format_dataset_name(ds)
            keys = self._enumerate_partition_keys_for_asset(asset_name)
            if keys:
                out_key_sets.append(keys)

        if out_key_sets:
            base = set(map(_serialize_partition_key, out_key_sets[0]))
            for ks in out_key_sets[1:]:
                if set(map(_serialize_partition_key, ks)) != base:
                    raise ValueError(
                        f"Node '{node.name}' has multiple partitioned outputs with incompatible keyspaces"
                    )
            return out_key_sets[0]

        in_key_sets: list[list[Any]] = []
        for ds in node.inputs:
            asset_name = format_dataset_name(ds)
            keys = self._enumerate_partition_keys_for_asset(asset_name)
            if keys:
                in_key_sets.append(keys)

        if in_key_sets:
            base = set(map(_serialize_partition_key, in_key_sets[0]))
            for ks in in_key_sets[1:]:
                if set(map(_serialize_partition_key, ks)) != base:
                    raise ValueError(
                        f"Node '{node.name}' has multiple partitioned inputs with incompatible keyspaces"
                    )
            return in_key_sets[0]

        return []


    def _create_pipeline_hook_ops(self, job_name: str, pipeline: Pipeline) -> tuple[dg.OpDefinition, dg.OpDefinition]:
        """Create the pipeline hook ops for before and after pipeline run.

        Args:
            job_name (str): The name of the job.
            pipeline (Pipeline): The Kedro pipeline.

        Returns:
            tuple[OpDefinition, OpDefinition]: The before and after pipeline run hook ops.

        """
        required_resource_keys = {"kedro_run"}
        if self._enable_mlflow and is_mlflow_enabled():
            required_resource_keys.add("mlflow")

        @dg.op(
            name=f"before_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed before the `{job_name}` pipeline run.",
            out={"before_pipeline_run_hook_output": dg.Out(dagster_type=dg.Nothing)},
            required_resource_keys=required_resource_keys,
        )
        def before_pipeline_run_hook(context: dg.OpExecutionContext):
            kedro_run_resource = context.resources.kedro_run
            kedro_run_resource.after_context_created_hook()

            self._hook_manager.hook.before_pipeline_run(
                run_params=kedro_run_resource.run_params,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        after_pipeline_run_hook_ins = {
            f"{format_node_name(node.name)}_after_pipeline_run_hook_input": dg.In(dagster_type=dg.Nothing)
            for node in pipeline.nodes
        }
        for dataset_name in pipeline.all_outputs():
            asset_name = format_dataset_name(dataset_name)
            if not _is_param_name(dataset_name):
                asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                after_pipeline_run_hook_ins[asset_name] = dg.In(
                    asset_key=dg.AssetKey(asset_key),
                )

        @dg.op(
            name=f"after_pipeline_run_hook_{job_name}",
            description=f"Hook to be executed after the `{job_name}` pipeline run.",
            ins=after_pipeline_run_hook_ins,
            required_resource_keys=required_resource_keys,
        )
        def after_pipeline_run_hook(context: dg.OpExecutionContext, **materialized_assets) -> dg.Nothing:  # type: ignore[no-untyped-def]
            kedro_run_resource = context.resources.kedro_run

            run_results = {}
            for dataset_name in pipeline.outputs():
                asset_name = format_dataset_name(dataset_name)
                run_results[dataset_name] = materialized_assets[asset_name]

            self._hook_manager.hook.after_pipeline_run(
                run_params=kedro_run_resource.run_params,
                run_result=run_results,
                pipeline=pipeline,
                catalog=self._catalog,
            )

        return before_pipeline_run_hook, after_pipeline_run_hook

    def translate_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_name: str,
        filter_params: dict[str, Any],
        job_name: str,
        executor_def: dg.ExecutorDefinition | None = None,
        logger_defs: dict[str, dg.LoggerDefinition] | None = None,
    ) -> dg.JobDefinition:
        """Translate a Kedro pipeline into a Dagster job.

        Args:
            pipeline (Pipeline): The Kedro pipeline.
            pipeline_name (str): The name of the Kedro pipeline.
            filter_params (dict[str, Any]): Filter parameters for the pipeline.
            job_name (str): The name of the job.
            executor_def (ExecutorDefinition): The executor definition.
            logger_defs (dict[str, LoggerDefinition] | None): The logger definitions.

        Returns:
            JobDefinition: A Dagster job definition.
        """
        (
            before_pipeline_run_hook,
            after_pipeline_run_hook,
        ) = self._create_pipeline_hook_ops(job_name, pipeline)

        @dg.graph(
            name=f"{self._env}__{job_name}",
            description=f"Job derived from pipeline associated to `{job_name}` in env `{self._env}`.",
            out=None,
        )
        def pipeline_graph() -> None:
            before_pipeline_run_hook_output = before_pipeline_run_hook()

            # Collect initial external assets (broadcastable to partitions)
            materialized_input_assets: dict[str, Any] = {}
            for dataset_name in pipeline.inputs():
                asset_name = format_dataset_name(dataset_name)
                if not _is_param_name(dataset_name):
                    # External assets first
                    if asset_name in self._named_assets:
                        materialized_input_assets[asset_name] = self._named_assets[asset_name]
                    else:
                        asset_key = get_asset_key_from_dataset_name(dataset_name, self._env)
                        materialized_input_assets[asset_name] = dg.AssetSpec(key=asset_key).with_io_manager_key(
                            f"{self._env}__{asset_name}_io_manager"
                        )

            # Per-asset partitioned outputs cache: asset -> {serialized_pk: handle}
            partitioned_outputs: dict[str, dict[str, Any]] = {}

            # Last seen outputs (for after hook wiring) — for partitioned assets, last clone wins
            materialized_output_assets: dict[str, Any] = {}

            for layer in pipeline.grouped_nodes:
                for node in layer:
                    op_name = format_node_name(node.name) + "_graph"
                    base_op = self._named_ops[op_name]

                    exec_keys = self._node_execution_keys(node)

                    # Helper to assemble input kwargs for a given partition key (or None)
                    def _build_inputs_for_partition(pkey_obj: Any | None, pkey_serial: str | None) -> dict[str, Any]:
                        inputs_kwargs: dict[str, Any] = {}
                        for input_dataset_name in node.inputs:
                            input_asset_name = format_dataset_name(input_dataset_name)
                            if _is_param_name(input_dataset_name):
                                continue
                            # Preference order: partitioned_outputs (from previous nodes) -> initial external
                            if input_asset_name in partitioned_outputs and pkey_serial is not None:
                                up_map = partitioned_outputs[input_asset_name]
                                # Identity mapping by default: same serial
                                handle = up_map.get(pkey_serial)
                                if handle is None:
                                    # Try mapping from upstream to this downstream partition key
                                    mapping_fn = self._get_partition_mapping(input_asset_name, input_asset_name)
                                    if mapping_fn is not None and pkey_obj is not None:
                                        # find upstream partitions mapping to this downstream key
                                        upstream_keys = self._enumerate_partition_keys_for_asset(input_asset_name)
                                        matched_serials: list[str] = []
                                        for uk in upstream_keys:
                                            mapped_down = mapping_fn(uk) or []
                                            if any(_serialize_partition_key(dk) == pkey_serial for dk in mapped_down):
                                                matched_serials.append(_serialize_partition_key(uk))
                                        if len(matched_serials) > 1:
                                            raise ValueError(
                                                f"Implicit fan-in not supported for input '{input_asset_name}' of node '{node.name}'."
                                            )
                                        if matched_serials:
                                            handle = up_map.get(matched_serials[0])
                                if handle is not None:
                                    inputs_kwargs[input_asset_name] = handle
                                    continue
                            # Fallback to broadcast external/unpartitioned
                            if input_asset_name in materialized_input_assets:
                                inputs_kwargs[input_asset_name] = materialized_input_assets[input_asset_name]

                        return inputs_kwargs

                    if not exec_keys:
                        # Unpartitioned single invocation
                        inputs_kwargs = _build_inputs_for_partition(None, None)
                        res = base_op(
                            before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                            **inputs_kwargs,
                        )
                        # Capture outputs
                        if len(node.outputs) == 0:
                            materialized_output_assets_op = {res.output_name: res}
                        else:
                            materialized_output_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res  # type: ignore[assignment]
                            }
                        # Update caches
                        for out_ds in node.outputs:
                            out_asset_name = format_dataset_name(out_ds)
                            handle = materialized_output_assets_op.get(out_asset_name)
                            if handle is not None:
                                materialized_input_assets[out_asset_name] = handle
                                materialized_output_assets[out_asset_name] = handle
                        # Also keep the hook Nothing output (overwrites last)
                        for name, handle in materialized_output_assets_op.items():
                            if name.endswith("_after_pipeline_run_hook_input"):
                                materialized_output_assets[name] = handle
                        continue

                    # Partitioned: clone per partition key
                    for pk in exec_keys:
                        pks = _serialize_partition_key(pk)
                        alias_name = f"{format_node_name(node.name)}__{pks}"
                        op_invocation = base_op.alias(alias_name).with_config({"__partition_key": str(pk)})
                        inputs_kwargs = _build_inputs_for_partition(pk, pks)
                        res = op_invocation(
                            before_pipeline_run_hook_output=before_pipeline_run_hook_output,
                            **inputs_kwargs,
                        )
                        if len(node.outputs) == 0:
                            materialized_output_assets_op = {res.output_name: res}
                        else:
                            materialized_output_assets_op = {
                                out_handle.output_name: out_handle for out_handle in res  # type: ignore[assignment]
                            }
                        # Update partitioned cache for each produced asset
                        for out_ds in node.outputs:
                            out_asset_name = format_dataset_name(out_ds)
                            handle = materialized_output_assets_op.get(out_asset_name)

                            if handle is not None:
                                partitioned_outputs.setdefault(out_asset_name, {})[pks] = handle
                                # For after hook, last clone wins
                                materialized_output_assets[out_asset_name] = handle
                        # Also keep the hook Nothing output for this clone (last clone wins)
                        for name, handle in materialized_output_assets_op.items():
                            if name.endswith("_after_pipeline_run_hook_input"):
                                materialized_output_assets[name] = handle

            # Wire after hook with latest outputs/controls
            after_pipeline_run_hook(**materialized_output_assets)

        # Overrides the kedro_run resource with the one created for the job
        kedro_run_translator = KedroRunTranslator(
            context=self._context,
            project_path=self._project_path,
            env=self._env,
            session_id=self._session_id,
        )
        kedro_run_resource = kedro_run_translator.to_dagster(
            pipeline_name=pipeline_name,
            filter_params=filter_params,
        )
        resource_defs = {"kedro_run": kedro_run_resource}

        for dataset_name in pipeline.all_inputs() | pipeline.all_outputs():
            asset_name = format_dataset_name(dataset_name)
            if f"{self._env}__{asset_name}_io_manager" in self._named_resources:
                resource_defs[f"{self._env}__{asset_name}_io_manager"] = self._named_resources[
                    f"{self._env}__{asset_name}_io_manager"
                ]

        if self._enable_mlflow and is_mlflow_enabled():
            resource_defs |= {"mlflow": self._named_resources["mlflow"]}

        job = pipeline_graph.to_job(
            name=f"{self._env}__{job_name}",
            resource_defs=resource_defs,
            executor_def=executor_def,
            logger_defs=logger_defs,
        )

        return job

    def to_dagster(self) -> dict[str, dg.JobDefinition]:
        """Translate the Kedro pipelines into Dagster jobs.

        Returns:
            dict[str, JobDefinition]: The translated Dagster jobs.
        """
        named_jobs = {}
        for job_name, job_config in self._dagster_config.jobs.items():  # type: ignore[attr-defined]
            pipeline_config = job_config.pipeline.model_dump()

            pipeline_name = pipeline_config.get("pipeline_name", "__default__")
            filter_params = get_filter_params_dict(pipeline_config)
            pipeline = pipelines.get(pipeline_name).filter(**filter_params)

            executor_config = job_config.executor
            if executor_config in self._named_executors:
                executor_def = self._named_executors[executor_config]
            else:
                raise ValueError(f"Executor `{executor_config}` not found.")

            job = self.translate_pipeline(
                pipeline=pipeline,
                pipeline_name=pipeline_name,
                filter_params=filter_params,
                job_name=job_name,
                executor_def=executor_def,
            )

            named_jobs[job_name] = job

        return named_jobs
