# mypy: ignore-errors

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import kedro.framework.session.session as _kedro_session_mod
from kedro.framework import project as _kedro_project
from pytest import fixture

from .scenarios.kedro_projects import (
    dagster_executors_config,
    make_jobs_config,
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


# Avoid loading third-party Kedro plugin hooks via entry points during tests.
@fixture(autouse=True)
def _disable_kedro_plugin_entrypoints(monkeypatch):
    original = getattr(_kedro_session_mod, "_register_hooks_entry_points", None)

    def _wrapped_register(hook_manager, disabled_plugins):
        # Call the original registration first (if available) to load entry points
        if callable(original):
            original(hook_manager, disabled_plugins)

        # Determine which plugins are allowed for this test run.
        #  - Project settings variable ALLOWED_HOOK_PLUGINS defined in the generated project
        #  - If not present or empty: disable all third-party plugin hooks
        try:
            proj_allowed = getattr(_kedro_project.settings, "ALLOWED_HOOK_PLUGINS", ())
        except Exception:
            proj_allowed = ()

        allowed_set = {str(p).strip() for p in proj_allowed if str(p).strip()}

        # Unregister any plugin not explicitly allowed. If none are allowed,
        # unregister all loaded third-party plugins.
        try:
            for plugin, dist in hook_manager.list_plugin_distinfo():
                project_name = getattr(dist, "project_name", None)
                if not project_name:
                    # Skip if we cannot resolve a project name
                    hook_manager.unregister(plugin=plugin)
                    continue
                if not allowed_set or project_name not in allowed_set:
                    hook_manager.unregister(plugin=plugin)
        except Exception:
            for plugin, _ in hook_manager.list_plugin_distinfo():
                hook_manager.unregister(plugin=plugin)

    monkeypatch.setattr(
        _kedro_session_mod,
        "_register_hooks_entry_points",
        _wrapped_register,
        raising=False,
    )


@fixture(scope="session")
def project_scenario_factory(temp_directory) -> Callable[[KedroProjectOptions], KedroProjectOptions]:
    """Return a callable that builds Kedro project variants in tmp dirs.

    Usage:
        options = project_scenario_factory(KedroProjectOptions(env="base", catalog={...}))
    """

    def _factory(kedro_project_options: KedroProjectOptions, project_name: str | None = None) -> KedroProjectOptions:
        return build_kedro_project_scenario(
            temp_directory=temp_directory, options=kedro_project_options, project_name=project_name
        )

    return _factory


# Convenience fixtures: one Kedro project per scenario, each with a unique project_name


@fixture(scope="session")
def kedro_project_no_dagster_config_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_no_dagster_config(env="base"), project_name="kedro-project-no-dagster-config"
    )


@fixture(scope="function")
def kedro_project_exec_filebacked_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(options_exec_filebacked(env="base"), project_name="kedro-project-exec-filebacked")


@fixture(scope="function")
def kedro_project_exec_filebacked_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_exec_filebacked(env="local"), project_name="kedro-project-exec-filebacked-local"
    )


@fixture(scope="function")
def kedro_project_partitioned_intermediate_output2_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_intermediate_output2(env="base"),
        project_name="kedro-project-partitioned-intermediate-output2",
    )


@fixture(scope="function")
def kedro_project_partitioned_intermediate_output2_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_intermediate_output2(env="local"),
        project_name="kedro-project-partitioned-intermediate-output2-local",
    )


@fixture(scope="function")
def kedro_project_partitioned_identity_mapping_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_identity_mapping(env="base"),
        project_name="kedro-project-partitioned-identity-mapping",
    )


@fixture(scope="function")
def kedro_project_partitioned_identity_mapping_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_identity_mapping(env="local"),
        project_name="kedro-project-partitioned-identity-mapping-local",
    )


@fixture(scope="function")
def kedro_project_partitioned_static_mapping_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_static_mapping(env="base"),
        project_name="kedro-project-partitioned-static-mapping",
    )


@fixture(scope="function")
def kedro_project_partitioned_static_mapping_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_partitioned_static_mapping(env="local"),
        project_name="kedro-project-partitioned-static-mapping-local",
    )


@fixture(scope="function")
def kedro_project_no_outputs_node_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(options_no_outputs_node(env="base"), project_name="kedro-project-no-outputs-node")


@fixture(scope="function")
def kedro_project_no_outputs_node_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_no_outputs_node(env="local"), project_name="kedro-project-no-outputs-node-local"
    )


@fixture(scope="function")
def kedro_project_nothing_assets_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(options_nothing_assets(env="base"), project_name="kedro-project-nothing-assets")


@fixture(scope="function")
def kedro_project_nothing_assets_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_nothing_assets(env="local"), project_name="kedro-project-nothing-assets-local"
    )


@fixture(scope="function")
def kedro_project_hooks_filebacked_base(project_scenario_factory, tmp_path: Path) -> KedroProjectOptions:
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
    return project_scenario_factory(opts, project_name="kedro-project-hooks-filebacked")


@fixture(scope="function")
def kedro_project_hooks_filebacked_local(project_scenario_factory, tmp_path: Path) -> KedroProjectOptions:
    input_csv = tmp_path / "input_local.csv"
    input_csv.write_text("value\n2\n", encoding="utf-8")
    primary_dir = tmp_path / "data_primary_local"
    output_dir = tmp_path / "data_output_local"
    primary_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    opts = options_hooks_filebacked(
        env="local", input_csv=str(input_csv), primary_dir=str(primary_dir), output_dir=str(output_dir)
    )
    return project_scenario_factory(opts, project_name="kedro-project-hooks-filebacked-local")


@fixture(scope="function")
def kedro_project_multiple_inputs_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_inputs(env="base"), project_name="kedro-project-multiple-inputs-base"
    )


@fixture(scope="function")
def kedro_project_multiple_inputs_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_inputs(env="local"), project_name="kedro-project-multiple-inputs-local"
    )


@fixture(scope="function")
def kedro_project_multiple_outputs_tuple_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_outputs_tuple(env="base"), project_name="kedro-project-multiple-outputs-tuple-base"
    )


@fixture(scope="function")
def kedro_project_multiple_outputs_tuple_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_outputs_tuple(env="local"), project_name="kedro-project-multiple-outputs-tuple-local"
    )


@fixture(scope="function")
def kedro_project_multiple_outputs_dict_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_outputs_dict(env="base"), project_name="kedro-project-multiple-outputs-dict-base"
    )


@fixture(scope="function")
def kedro_project_multiple_outputs_dict_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_multiple_outputs_dict(env="local"), project_name="kedro-project-multiple-outputs-dict-local"
    )


@fixture(scope="function")
def kedro_project_exec_filebacked_output2_memory_base(project_scenario_factory) -> KedroProjectOptions:
    opts = options_exec_filebacked(env="base")
    opts.catalog["output2_ds"] = {"type": "MemoryDataset"}
    return project_scenario_factory(opts, project_name="kedro-project-exec-filebacked-output2-memory-base")


@fixture(scope="function")
def kedro_project_exec_filebacked_output2_memory_local(project_scenario_factory) -> KedroProjectOptions:
    opts = options_exec_filebacked(env="local")
    opts.catalog["output2_ds"] = {"type": "MemoryDataset"}
    return project_scenario_factory(opts, project_name="kedro-project-exec-filebacked-output2-memory-local")


@fixture(scope="function")
def kedro_project_multi_executors_base(project_scenario_factory) -> KedroProjectOptions:
    dagster_cfg = {
        "executors": dagster_executors_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="multiproc"),
    }
    return project_scenario_factory(
        KedroProjectOptions(env="base", dagster=dagster_cfg), project_name="kedro-project-multi-executors-base"
    )


@fixture(scope="function")
def kedro_project_multi_executors_local(project_scenario_factory) -> KedroProjectOptions:
    dagster_cfg = {
        "executors": dagster_executors_config(),
        "jobs": make_jobs_config(pipeline_name="__default__", executor="multiproc"),
    }
    return project_scenario_factory(
        KedroProjectOptions(env="local", dagster=dagster_cfg), project_name="kedro-project-multi-executors-local"
    )


@fixture(scope="function")
def kedro_project_scenario_env(request, project_scenario_factory) -> KedroProjectOptions:
    scenario_key, env = request.param
    # Map keys to option builders
    builder_map = {
        "exec_filebacked": options_exec_filebacked,
        "partitioned_intermediate_output2": options_partitioned_intermediate_output2,
        "partitioned_static_mapping": options_partitioned_static_mapping,
        "multiple_inputs": options_multiple_inputs,
        "multiple_outputs_tuple": options_multiple_outputs_tuple,
        "multiple_outputs_dict": options_multiple_outputs_dict,
        "no_outputs_node": options_no_outputs_node,
        "nothing_assets": options_nothing_assets,
    }
    opts = builder_map[scenario_key](env)
    project_name = f"kedro-project-{scenario_key.replace('_', '-')}-{env}"
    return project_scenario_factory(opts, project_name=project_name)


@fixture(scope="function")
def kedro_project_multi_in_out_env(request, project_scenario_factory) -> KedroProjectOptions:
    scenario_key, env = request.param
    builder_map = {
        "multiple_inputs": options_multiple_inputs,
        "multiple_outputs_tuple": options_multiple_outputs_tuple,
        "multiple_outputs_dict": options_multiple_outputs_dict,
    }
    if scenario_key not in builder_map:
        raise ValueError("Invalid multi-in/out scenario key")
    opts = builder_map[scenario_key](env)
    project_name = f"kedro-project-{scenario_key.replace('_', '-')}-{env}"
    return project_scenario_factory(opts, project_name=project_name)
