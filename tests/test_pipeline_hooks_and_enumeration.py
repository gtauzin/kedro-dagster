# mypy: ignore-errors

from __future__ import annotations

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline, node

from kedro_dagster.nodes import NodeTranslator
from kedro_dagster.pipelines import PipelineTranslator


class DummyContext:
    def __init__(self, catalog: DataCatalog):
        self.catalog = catalog

        # create a minimal hook manager-like object
        class _HookMgr:
            def __init__(self):
                class _Hook:
                    def before_pipeline_run(self, **kwargs):
                        return None

                    def after_pipeline_run(self, **kwargs):
                        return None

                self.hook = _Hook()

        self._hook_manager = _HookMgr()


def _make_pipeline_translator(named_resources: dict | None = None) -> PipelineTranslator:
    catalog = DataCatalog()
    return PipelineTranslator(
        dagster_config={},
        context=DummyContext(catalog),
        project_path="/tmp/project",
        env="base",
        session_id="sess",
        named_assets={},
        asset_partitions={},
        named_op_factories={},
        named_resources=named_resources or {},
        named_executors={},
        enable_mlflow=False,
    )


def test_enumerate_partition_keys_none_returns_empty_list():
    t = _make_pipeline_translator()
    assert t._enumerate_partition_keys(None) == []


def test_before_after_pipeline_hooks_require_mlflow_conditionally():
    # without mlflow
    t1 = _make_pipeline_translator()
    op1 = t1._create_before_pipeline_run_hook("job", Pipeline([]))
    assert "mlflow" not in op1.required_resource_keys

    after1 = t1._create_after_pipeline_run_hook_op("job", Pipeline([]), ["x_after_pipeline_run_hook_input"])
    assert "mlflow" not in after1.required_resource_keys
    # input should be declared
    assert "x_after_pipeline_run_hook_input" in after1.ins

    # with mlflow
    t2 = _make_pipeline_translator({"mlflow": object()})
    op2 = t2._create_before_pipeline_run_hook("job", Pipeline([]))
    assert "mlflow" in op2.required_resource_keys
    after2 = t2._create_after_pipeline_run_hook_op("job", Pipeline([]), [])
    assert "mlflow" in after2.required_resource_keys


def test_node_op_declares_after_hook_output_and_mlflow_requirement():
    # minimal NodeTranslator using a dummy pipeline and no catalog IO managers
    catalog = DataCatalog()
    nt_without_mlflow = NodeTranslator(
        pipelines=[Pipeline([])],
        catalog=catalog,
        hook_manager=DummyContext(catalog)._hook_manager,
        session_id="sess",
        asset_partitions={},
        named_resources={},
        env="base",
    )

    nt_with_mlflow = NodeTranslator(
        pipelines=[Pipeline([])],
        catalog=catalog,
        hook_manager=DummyContext(catalog)._hook_manager,
        session_id="sess",
        asset_partitions={},
        named_resources={"mlflow": object()},
        env="base",
    )

    n = node(func=lambda inputs: {"out": 1}, inputs=["inp"], outputs=["out"], name="N")
    op1 = nt_without_mlflow.create_op(n)
    # last-layer adds a synthetic Nothing output
    assert any(name.endswith("_after_pipeline_run_hook_input") for name in op1.outs)
    assert "mlflow" not in op1.required_resource_keys

    op2 = nt_with_mlflow.create_op(n)
    assert "mlflow" in op2.required_resource_keys
