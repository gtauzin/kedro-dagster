# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from pytest import fixture

from .scenarios.helpers import dagster_executors_config, make_jobs_config
from .scenarios.kedro_projects import (
    options_exec_filebacked,
    options_hooks_filebacked,
    options_multiple_inputs,
    options_multiple_outputs_dict,
    options_multiple_outputs_tuple,
    options_no_dagster_config,
    options_no_outputs_node,
    options_nothing_assets,
    options_partitioned_identity_mapping,
    options_partitioned_intermediate_output2,
    options_partitioned_static_mapping,
)
from .scenarios.project_factory import KedroProjectOptions, build_kedro_project_scenario


@fixture(scope="session")
def temp_directory(tmpdir_factory):
    # Use tmpdir_factory to create a temporary directory with session scope
    return tmpdir_factory.mktemp("session_temp_dir")


@fixture(scope="session")
def project_scenario_factory(temp_directory) -> Callable[[KedroProjectOptions], Path]:
    """Return a callable that builds Kedro project variants in tmp dirs.

    Usage:
        project_path = project_scenario_factory(KedroProjectOptions(env="base", catalog={...}))
    """

    def _factory(kedro_project_options: KedroProjectOptions, project_name: str | None = None) -> Path:
        return build_kedro_project_scenario(
            temp_directory=temp_directory, options=kedro_project_options, project_name=project_name
        )

    return _factory


#
# Convenience fixtures: one Kedro project per scenario, each with a unique project_name
# Default env is "base" for all.
#


@fixture(scope="session")
def kedro_project_no_dagster_config(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_no_dagster_config(env="base"), project_name="kedro-project-no-dagster-config"
    )


@fixture(scope="function")
def kedro_project_exec_filebacked(project_scenario_factory) -> Path:
    return project_scenario_factory(options_exec_filebacked(env="base"), project_name="kedro-project-exec-filebacked")


@fixture(scope="function")
def kedro_project_partitioned_intermediate_output2(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_intermediate_output2(env="base"),
        project_name="kedro-project-partitioned-intermediate-output2",
    )


@fixture(scope="function")
def kedro_project_partitioned_identity_mapping_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_identity_mapping(env="base"),
        project_name="kedro-project-partitioned-identity-mapping",
    ), "base"


@fixture(scope="function")
def kedro_project_partitioned_identity_mapping_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_identity_mapping(env="local"),
        project_name="kedro-project-partitioned-identity-mapping-local",
    ), "local"


# Per-env wrappers for identity mapping returning (Path, env)


@fixture(scope="function")
def kedro_project_partitioned_static_mapping(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_static_mapping(env="base"),
        project_name="kedro-project-partitioned-static-mapping",
    )


@fixture(scope="function")
def kedro_project_multiple_inputs(project_scenario_factory) -> Path:
    return project_scenario_factory(options_multiple_inputs(env="base"), project_name="kedro-project-multiple-inputs")


@fixture(scope="function")
def kedro_project_multiple_outputs_tuple(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_tuple(env="base"), project_name="kedro-project-multiple-outputs-tuple"
    )


@fixture(scope="function")
def kedro_project_multiple_outputs_dict(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_dict(env="base"), project_name="kedro-project-multiple-outputs-dict"
    )


@fixture(scope="function")
def kedro_project_no_outputs_node(project_scenario_factory) -> Path:
    return project_scenario_factory(options_no_outputs_node(env="base"), project_name="kedro-project-no-outputs-node")


@fixture(scope="function")
def kedro_project_nothing_assets(project_scenario_factory) -> Path:
    return project_scenario_factory(options_nothing_assets(env="base"), project_name="kedro-project-nothing-assets")


@fixture(scope="function")
def kedro_project_hooks_filebacked_base(project_scenario_factory, tmp_path: Path) -> Path:
    # Prepare input file and directories for file-backed scenario
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("value\n1\n", encoding="utf-8")
    primary_dir = tmp_path / "data_primary"
    output_dir = tmp_path / "data_output"
    primary_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    opts = options_hooks_filebacked(
        env="base", input_csv=str(input_csv), primary_dir=str(primary_dir), output_dir=str(output_dir)
    )
    return project_scenario_factory(opts, project_name="kedro-project-hooks-filebacked"), "base"


# Per-env variants and wrappers returning (Path, env)


@fixture(scope="function")
def kedro_project_exec_filebacked_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_exec_filebacked(env="base"), project_name="kedro-project-exec-filebacked-base"
    ), "base"


@fixture(scope="function")
def kedro_project_exec_filebacked_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_exec_filebacked(env="local"), project_name="kedro-project-exec-filebacked-local"
    ), "local"


@fixture(scope="function")
def kedro_project_partitioned_intermediate_output2_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_intermediate_output2(env="base"),
        project_name="kedro-project-partitioned-intermediate-output2-base",
    ), "base"


@fixture(scope="function")
def kedro_project_partitioned_intermediate_output2_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_intermediate_output2(env="local"),
        project_name="kedro-project-partitioned-intermediate-output2-local",
    ), "local"


@fixture(scope="function")
def kedro_project_partitioned_static_mapping_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_static_mapping(env="base"),
        project_name="kedro-project-partitioned-static-mapping-base",
    ), "base"


@fixture(scope="function")
def kedro_project_partitioned_static_mapping_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_partitioned_static_mapping(env="local"),
        project_name="kedro-project-partitioned-static-mapping-local",
    ), "local"


@fixture(scope="function")
def kedro_project_multiple_inputs_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_inputs(env="base"), project_name="kedro-project-multiple-inputs-base"
    ), "base"


@fixture(scope="function")
def kedro_project_multiple_inputs_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_inputs(env="local"), project_name="kedro-project-multiple-inputs-local"
    ), "local"


@fixture(scope="function")
def kedro_project_multiple_outputs_tuple_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_tuple(env="base"), project_name="kedro-project-multiple-outputs-tuple-base"
    ), "base"


@fixture(scope="function")
def kedro_project_multiple_outputs_tuple_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_tuple(env="local"), project_name="kedro-project-multiple-outputs-tuple-local"
    ), "local"


@fixture(scope="function")
def kedro_project_multiple_outputs_dict_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_dict(env="base"), project_name="kedro-project-multiple-outputs-dict-base"
    ), "base"


@fixture(scope="function")
def kedro_project_multiple_outputs_dict_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_multiple_outputs_dict(env="local"), project_name="kedro-project-multiple-outputs-dict-local"
    ), "local"


@fixture(scope="function")
def kedro_project_no_outputs_node_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_no_outputs_node(env="base"), project_name="kedro-project-no-outputs-node-base"
    ), "base"


@fixture(scope="function")
def kedro_project_no_outputs_node_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_no_outputs_node(env="local"), project_name="kedro-project-no-outputs-node-local"
    ), "local"


@fixture(scope="function")
def kedro_project_nothing_assets_base(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_nothing_assets(env="base"), project_name="kedro-project-nothing-assets-base"
    ), "base"


@fixture(scope="function")
def kedro_project_nothing_assets_local(project_scenario_factory) -> Path:
    return project_scenario_factory(
        options_nothing_assets(env="local"), project_name="kedro-project-nothing-assets-local"
    ), "local"


@fixture(scope="function")
def kedro_project_hooks_filebacked_local(project_scenario_factory, tmp_path: Path) -> Path:
    input_csv = tmp_path / "input_local.csv"
    input_csv.write_text("value\n2\n", encoding="utf-8")
    primary_dir = tmp_path / "data_primary_local"
    output_dir = tmp_path / "data_output_local"
    primary_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    opts = options_hooks_filebacked(
        env="local", input_csv=str(input_csv), primary_dir=str(primary_dir), output_dir=str(output_dir)
    )
    return project_scenario_factory(opts, project_name="kedro-project-hooks-filebacked-local"), "local"


# Scenario variant: exec filebacked but output2 is MemoryDataset


@fixture(scope="function")
def kedro_project_exec_filebacked_output2_memory_base(project_scenario_factory) -> Path:
    opts = options_exec_filebacked(env="base")
    opts.catalog["output2_ds"] = {"type": "MemoryDataset"}
    return project_scenario_factory(opts, project_name="kedro-project-exec-filebacked-output2-memory-base"), "base"


@fixture(scope="function")
def kedro_project_exec_filebacked_output2_memory_local(project_scenario_factory) -> Path:
    opts = options_exec_filebacked(env="local")
    opts.catalog["output2_ds"] = {"type": "MemoryDataset"}
    return project_scenario_factory(opts, project_name="kedro-project-exec-filebacked-output2-memory-local"), "local"


# Scenario: Multiple executors dagster config


@fixture(scope="function")
def kedro_project_multi_executors_base(project_scenario_factory) -> Path:
    dagster_cfg = {
        "executors": dagster_executors_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="multiproc"),
    }
    return project_scenario_factory(
        KedroProjectOptions(env="base", dagster=dagster_cfg), project_name="kedro-project-multi-executors-base"
    ), "base"


@fixture(scope="function")
def kedro_project_multi_executors_local(project_scenario_factory) -> Path:
    dagster_cfg = {
        "executors": dagster_executors_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="multiproc"),
    }
    return project_scenario_factory(
        KedroProjectOptions(env="local", dagster=dagster_cfg), project_name="kedro-project-multi-executors-local"
    ), "local"
