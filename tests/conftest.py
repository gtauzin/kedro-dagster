# mypy: ignore-errors

from __future__ import annotations

import importlib.metadata as _ilmd
from collections.abc import Callable
from pathlib import Path

import kedro.framework.session.session as _kedro_session_mod
from kedro.framework import project as _kedro_project
from pytest import fixture

from .scenarios.kedro_projects import (
    dagster_executors_config,
    make_jobs_config,
    options_exec_filebacked,
    options_group_name_metadata,
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
    options_spaceflights_quickstart,
)
from .scenarios.project_factory import KedroProjectOptions, build_kedro_project_scenario


@fixture(scope="session")
def temp_directory(tmpdir_factory):
    # Use tmpdir_factory to create a temporary directory with session scope
    return tmpdir_factory.mktemp("session_temp_dir")


# Avoid loading third-party Kedro plugin hooks via entry points during tests.
@fixture(autouse=True)
def _disable_kedro_plugin_entrypoints(monkeypatch):
    # Replace Kedro's entry-point-based hook registration with a filtered
    # version for tests so that system-installed plugins don't interfere.
    def _wrapped_register(*args, **kwargs):
        hook_manager = args[0] if args else kwargs.get("hook_manager")

        # Read the project setting that explicitly lists allowed plugin
        # distribution names. Treat missing/empty as "no plugins allowed".
        proj_allowed = getattr(_kedro_project.settings, "ALLOWED_HOOK_PLUGINS", ())
        allowed_set = {str(p).strip() for p in proj_allowed if str(p).strip()}

        if not allowed_set:
            return hook_manager

        # A loader that only loads entry points coming from allowed
        # distributions. This mirrors the shape of the loader Kedro expects
        # but filters by distribution name.
        def _filtered_loader(group: str):
            # Obtain entry points in a way compatible with both old and new
            # importlib.metadata APIs. We catch exceptions to avoid failing
            # the test suite if metadata introspection fails.
            try:
                all_entry_points = _ilmd.entry_points()
                if hasattr(all_entry_points, "select"):
                    # Newer API: select by group
                    entry_points = list(all_entry_points.select(group=group))
                else:
                    # Older API: mapping-like access
                    entry_points = list(all_entry_points.get(group, []))
            except Exception:
                # On any error, treat as if there are no entry points.
                entry_points = []

            # Iterate entry points and load/register only those whose
            # distribution name is explicitly allowed. Each entry point is
            # guarded so one bad plugin doesn't break test setup.
            for entry_point in entry_points:
                try:
                    dist_name = getattr(getattr(entry_point, "dist", None), "name", None)
                    if dist_name and dist_name in allowed_set:
                        plugin = entry_point.load()
                        hook_manager.register(plugin, name=getattr(entry_point, "name", None))
                except Exception:
                    # Ignore failures for individual entry points.
                    continue

            return hook_manager

        hook_manager.load_setuptools_entrypoints = _filtered_loader

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


@fixture(scope="function")
def kedro_project_group_name_metadata_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_group_name_metadata(env="base"), project_name="kedro-project-group-name-metadata-base"
    )


@fixture(scope="function")
def kedro_project_group_name_metadata_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_group_name_metadata(env="local"), project_name="kedro-project-group-name-metadata-local"
    )


@fixture(scope="function")
def kedro_project_spaceflights_quickstart_base(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_spaceflights_quickstart(env="base"), project_name="kedro-project-spaceflights-quickstart-base"
    )


@fixture(scope="function")
def kedro_project_spaceflights_quickstart_local(project_scenario_factory) -> KedroProjectOptions:
    return project_scenario_factory(
        options_spaceflights_quickstart(env="local"), project_name="kedro-project-spaceflights-quickstart-local"
    )
