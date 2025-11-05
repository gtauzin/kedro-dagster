"""A collection of CLI commands for working with Kedro-Dagster."""

import os
import subprocess
from logging import getLogger
from pathlib import Path

import click
from dagster_dg_cli.cli import create_dg_cli
from kedro.framework.project import settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.utils import find_kedro_project, write_jinja_template

LOGGER = getLogger(__name__)
TEMPLATE_FOLDER_PATH = Path(__file__).parent / "templates"


@click.group(name="Kedro-Dagster")
def commands() -> None:
    pass


@commands.group(name="dagster")
def dagster_commands() -> None:
    """Run project with Dagster"""
    pass


@dagster_commands.command()
@click.option(
    "--env",
    "-e",
    default="base",
    help="The name of the kedro environment where the 'dagster.yml' should be created. Default to 'local'",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Update the template without any checks.",
)
@click.option(
    "--silent",
    "-s",
    is_flag=True,
    default=False,
    help="Should message be logged when files are modified?",
)
def init(env: str, force: bool, silent: bool) -> None:
    """Updates the template of a kedro project.

    Running this command is mandatory to use Kedro-Dagster.

    This adds:
     - "conf/base/dagster.yml": This is a configuration file
     used for the dagster run parametrization.
     - "src/<python_package>/definitions.py": This is the
     dagster file where all dagster definitions are set.
    """

    dagster_yml = "dagster.yml"
    project_path = find_kedro_project(Path.cwd()) or Path.cwd()
    project_metadata = bootstrap_project(project_path)
    package_name = project_metadata.package_name
    dagster_yml_path = project_path / settings.CONF_SOURCE / env / dagster_yml

    if dagster_yml_path.is_file() and not force:
        click.secho(
            click.style(
                f"A 'dagster.yml' already exists at '{dagster_yml_path}' You can use the ``--force`` option to override it.",
                fg="red",
            )
        )
    else:
        try:
            write_jinja_template(
                src=TEMPLATE_FOLDER_PATH / dagster_yml,
                is_cookiecutter=False,
                dst=dagster_yml_path,
                python_package=package_name,
            )
            if not silent:
                click.secho(
                    click.style(
                        f"'{settings.CONF_SOURCE}/{env}/{dagster_yml}' successfully updated.",
                        fg="green",
                    )
                )
        except FileNotFoundError:
            click.secho(
                click.style(
                    f"No env '{env}' found. Please check this folder exists inside '{settings.CONF_SOURCE}' folder.",
                    fg="red",
                )
            )

    definitions_py = "definitions.py"
    definitions_py_path = project_path / "src" / package_name / definitions_py

    if definitions_py_path.is_file() and not force:
        click.secho(
            click.style(
                f"A 'definitions.py' already exists at '{definitions_py_path}' You can use the ``--force`` option to override it.",
                fg="red",
            )
        )
    else:
        write_jinja_template(
            src=TEMPLATE_FOLDER_PATH / definitions_py,
            is_cookiecutter=False,
            dst=definitions_py_path,
            python_package=package_name,
        )
        if not silent:
            click.secho(
                click.style(
                    f"'src/{package_name}/{definitions_py}' successfully updated.",
                    fg="green",
                )
            )

    # Create/Update the project's dg.toml from template
    # - 'project_name' in the template refers to the Python root module (i.e., package name)
    # - 'package_name' in the template refers to the display project name
    dg_toml = "dg.toml"
    dg_toml_path = project_path / dg_toml

    if dg_toml_path.is_file() and not force:
        click.secho(
            click.style(
                f"A 'dg.toml' already exists at '{dg_toml_path}' You can use the ``--force`` option to override it.",
                fg="red",
            )
        )
    else:
        write_jinja_template(
            src=TEMPLATE_FOLDER_PATH / dg_toml,
            is_cookiecutter=False,
            dst=dg_toml_path,
            # Map template variables appropriately
            project_name=package_name,  # Python module name
            package_name=project_metadata.project_name,  # Display project name
        )
        if not silent:
            click.secho(
                click.style(
                    f"'{dg_toml}' successfully updated.",
                    fg="green",
                )
            )


def _register_dg_commands() -> None:
    """Dynamically register all 'dg' CLI commands under 'kedro dagster'.

    Each command gets an additional '--env/-e' option and forwards all other
    args/options to the underlying 'dg' command via a subprocess. The subprocess
    is executed within a Kedro session context to ensure project settings are
    correctly initialized. We also set a few environment variables so the child
    process can pick up the Kedro project and environment if needed.
    """

    # Discover the available dg commands from the official CLI entrypoint factory
    dg_root: click.Group = create_dg_cli()
    dg_command_names = list(dg_root.commands.keys())

    # Skip commands already defined in our group (e.g., 'init', 'dev')
    existing_command_names = ["init"]

    for cmd_name in dg_command_names:
        if cmd_name in existing_command_names:
            continue

        # Try to introspect command options to detect presence of --log-level/--log-format
        log_level_flags: set[str] = set()
        log_format_flags: set[str] = set()
        try:
            if isinstance(dg_root, click.Group):
                cmd_obj = dg_root.commands.get(cmd_name)
                if cmd_obj is not None:
                    for p in getattr(cmd_obj, "params", []):
                        if isinstance(p, click.Option):
                            opts = set(p.opts)
                            if "--log-level" in opts:
                                log_level_flags = opts
                            if "--log-format" in opts:
                                log_format_flags = opts
        except Exception:  # pragma: no cover - be defensive if structure changes
            log_level_flags = set()
            log_format_flags = set()

        def _factory(name: str, lvl_flags: set[str], fmt_flags: set[str]) -> click.Command:
            @dagster_commands.command(
                name=name,
                context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
            )
            @click.option(
                "--env",
                "-e",
                required=False,
                default="local",
                help="The Kedro environment to use",
            )
            @click.argument("args", nargs=-1, type=click.UNPROCESSED)
            def _cmd(
                env: str,
                args: tuple[str, ...],
                _name: str = name,
                _lvl_flags: set[str] = lvl_flags,
                _fmt_flags: set[str] = fmt_flags,
            ) -> None:
                """Wrapper around 'dg <name>' executed within a Kedro session."""

                project_path = find_kedro_project(Path.cwd()) or Path.cwd()
                # Ensure Kedro project is bootstrapped and a session is created
                bootstrap_project(project_path)
                with KedroSession.create(project_path=project_path, env=env) as session:
                    # Ensure the Kedro context is fully initialized
                    session.load_context()
                    default_flags = {
                        "log-level": "info",
                        "log-format": "colored",
                    }

                    env_vars = os.environ.copy()
                    # Set Kedro env vars so child process can pick them up if needed
                    env_vars["KEDRO_ENV"] = env
                    env_vars["KEDRO_PROJECT_PATH"] = str(project_path)

                    # Build forwarded args and inject defaults if needed
                    forwarded = list(args)

                    def _has_flag(flags: set[str]) -> bool:
                        if not flags:
                            return False
                        for f in flags:
                            for a in forwarded:
                                if a == f or (f.startswith("--") and a.startswith(f + "=")):
                                    return True
                        return False

                    # If the dg command supports --log-level and it's not provided, inject from config
                    if _lvl_flags and not _has_flag(_lvl_flags):
                        try:
                            lvl_value = default_flags["log-level"]
                        except Exception:
                            lvl_value = None
                        if lvl_value:
                            forwarded.extend(["--log-level", str(lvl_value)])

                    # If the dg command supports --log-format and it's not provided, inject from config
                    if _fmt_flags and not _has_flag(_fmt_flags):
                        try:
                            fmt_value = default_flags["log-format"]
                        except Exception:
                            fmt_value = None
                        if fmt_value:
                            forwarded.extend(["--log-format", str(fmt_value)])

                    # Execute the original 'dg' command, forwarding all extra args
                    subprocess.call(["dg", _name, *forwarded], cwd=str(project_path), env=env_vars)

            return _cmd

        _factory(cmd_name, log_level_flags, log_format_flags)


# Register dg commands at import time so they appear in 'kedro dagster --help'
_register_dg_commands()
