"""A collection of CLI commands for working with Kedro-Dagster."""

import os
import subprocess
from logging import getLogger
from pathlib import Path
from typing import Any, Literal

import click
from dagster_dg_cli.cli import create_dg_cli
from kedro.framework.project import settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from kedro_dagster.utils import DAGSTER_VERSION, find_kedro_project, write_jinja_template

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
    """Scaffold or refresh Dagster integration files for the current Kedro project.

    Creates or updates the Dagster configuration and entry points so the project
    can be run from Dagster. Existing files are preserved unless ``--force`` is used.
    The Python package name is inferred from the Kedro project metadata.

    Created/updated templates:
    * ``conf/<env>/dagster.yml`` – Dagster run parametrization for Kedro-Dagster.
    * ``src/<python_package>/definitions.py`` – Dagster ``Definitions`` entry-point.
    * ``dg.toml`` – Dagster ``dg`` CLI configuration (Dagster >= 1.10.6 only).

    Args:
        env (str): Kedro environment under ``conf`` where ``dagster.yml`` is written. Defaults to ``"base"``.
        force (bool): Overwrite existing files without prompting. Defaults to ``False``.
        silent (bool): Suppress success messages for a quieter output. Defaults to ``False``.

    Examples:
        Basic initialization in the base config environment:

        >>> kedro dagster init --env base

        Force overwrite existing integration files:

        >>> kedro dagster init -e base --force

        Run silently (no success messages):

        >>> kedro dagster init -e base --silent
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
    if DAGSTER_VERSION >= (1, 10, 6):
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


if DAGSTER_VERSION >= (1, 10, 6):

    class DgProxyCommand(click.Command):
        """A Click command that proxies to a `dg <name>` command while showing its options in help.

        This keeps the wrapper lightweight (env + passthrough ARGS) but augments the help output
        to include the underlying `dg` command's options so users see the full set of flags.
        """

        def __init__(self, *args: Any, underlying_cmd: click.Command | None = None, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._underlying_cmd = underlying_cmd

        def format_options(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
            # Render our wrapper's options and the single "Options:" header
            super().format_options(ctx, formatter)

            # Then append the underlying dg command's options if available
            if not isinstance(self._underlying_cmd, click.Command):
                return
            try:
                uctx = click.Context(self._underlying_cmd)
                rows: list[tuple[str, str]] = []
                for p in getattr(self._underlying_cmd, "params", []):
                    if isinstance(p, click.Parameter):
                        rec = p.get_help_record(uctx)
                        if rec:
                            rows.append(rec)
                if rows:
                    formatter.write_dl(rows)
            except Exception:  # pragma: no cover
                # If the underlying command structure changes, don't break help output
                pass

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

        # Skip commands we already expose explicitly in this group
        existing = set(getattr(dagster_commands, "commands", {}).keys())
        wrapped_command_names = [cmf for cmf in dg_command_names if cmf not in existing]

        for cmd_name in wrapped_command_names:
            cmd_obj = dg_root.commands[cmd_name]

            def _callback_factory(name: str) -> Any:
                def _callback(env: str, args: tuple[str, ...]) -> None:
                    """Wrapper around 'dg <name>' executed within a Kedro session."""

                    project_path = find_kedro_project(Path.cwd()) or Path.cwd()
                    # Ensure Kedro project is bootstrapped and a session is created
                    bootstrap_project(project_path)
                    with KedroSession.create(project_path=project_path, env=env) as session:
                        # Ensure the Kedro context is fully initialized
                        session.load_context()

                        env_vars = os.environ.copy()
                        # Set Kedro env vars so child process can pick them up if needed
                        env_vars["KEDRO_ENV"] = env

                        # Execute the original 'dg' command, forwarding all extra args
                        subprocess.call(["dg", name, *args], cwd=str(project_path), env=env_vars)

                return _callback

            # Build a lightweight wrapper with env option and passthrough args
            params: list[click.Parameter] = [
                click.Option(["--env", "-e"], required=False, default="local", help="The Kedro environment to use"),
                click.Argument(["args"], nargs=-1, type=click.UNPROCESSED),
            ]
            # Prefer the underlying command's help/description if available
            help_text = (getattr(cmd_obj, "help", None) or "").strip()
            help_text = f" Kedro-Dagster wrapper around 'dg {cmd_name}'. " + help_text
            cmd = DgProxyCommand(
                name=cmd_name,
                params=params,
                callback=_callback_factory(cmd_name),
                help=help_text,
                context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
                underlying_cmd=cmd_obj,
            )
            dagster_commands.add_command(cmd)

    # Register dg commands at import time so they appear in 'kedro dagster --help'
    _register_dg_commands()

else:  # pragma: no cover

    @dagster_commands.command()
    @click.option(
        "--env",
        "-e",
        required=False,
        default="local",
        help="The Kedro environment within conf folder we want to retrieve",
    )
    @click.option(
        "--log-level",
        required=False,
        help="The level of the event tracked by the loggers",
    )
    @click.option(
        "--log-format",
        required=False,
        help="The format of the logs",
    )
    @click.option(
        "--port",
        "-p",
        required=False,
        help="The port to listen on",
    )
    @click.option(
        "--host",
        "-h",
        required=False,
        help="The network address to listen on",
    )
    @click.option(
        "--live-data-poll-rate",
        required=False,
        help="The rate at which to poll for new data",
    )
    def dev(
        env: str,
        log_level: Literal["debug", "info", "warning", "error", "critical"],
        log_format: Literal["color", "json", "default"],
        port: str,
        host: str,
        live_data_poll_rate: str,
    ) -> None:
        """Launch the Dagster developer UI for this Kedro project (Dagster < 1.10.6).

        Bootstraps the Kedro project, resolves the Dagster ``python_file`` from the
        Kedro-Dagster configuration, and invokes ``dagster dev`` with the provided options.
        Use this for local development to iterate on assets, jobs, schedules, sensors.

        Args:
            env (str): Kedro configuration environment to load (e.g., ``"local"``, ``"base"``, ``"prod"``).
            log_level (Literal["debug", "info", "warning", "error", "critical"]): Log verbosity for Dagster.
            log_format (Literal["color", "json", "default"]): Output format for logs.
            port (str): HTTP port to bind the Dagster web UI.
            host (str): Interface or IP to bind (e.g., ``"127.0.0.1"`` or ``"0.0.0.0"``).
            live_data_poll_rate (str): Polling interval in seconds when live data is enabled.

        Examples:
            Start the UI with the local environment on default port:

            >>> kedro dagster dev -e local

            Use JSON logs and custom port:

            >>> kedro dagster dev -e local --log-format json --log-level info --port 3000

        """

        project_path = find_kedro_project(Path.cwd()) or Path.cwd()
        project_metadata = bootstrap_project(project_path)
        package_name = project_metadata.package_name
        definitions_py = "definitions.py"
        definitions_py_path = project_path / "src" / package_name / definitions_py

        with KedroSession.create(
            project_path=project_path,
            env=env,
        ) as session:
            # Ensure the Kedro context is fully initialized
            session.load_context()

            env_vars = os.environ.copy()
            # Set Kedro env vars so child process can pick them up if needed
            env_vars["KEDRO_ENV"] = env

            # call dagster dev with specific options
            subprocess.call(
                [
                    "dagster",
                    "dev",
                    "--python-file",
                    definitions_py_path,
                    "--log-level",
                    log_level,
                    "--log-format",
                    log_format,
                    "--host",
                    host,
                    "--port",
                    port,
                    "--live-data-poll-rate",
                    live_data_poll_rate,
                ],
                cwd=str(project_path),
                env=env_vars,
            )
