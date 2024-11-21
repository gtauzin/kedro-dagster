"""A collection of CLI commands for working with Kedro-Dagster."""

import subprocess
from logging import getLogger
from pathlib import Path
from typing import Literal

import click
from kedro.framework.project import settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.utils import _find_kedro_project

from kedro_dagster.config import get_dagster_config
from kedro_dagster.utils import write_jinja_template

LOGGER = getLogger(__name__)
TEMPLATE_FOLDER_PATH = Path(__file__).parent / "templates"


@click.group(name="Kedro-Dagster")
def commands():
    pass


@commands.group(name="dagster")
def dagster_commands():
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
def init(env: str, force: bool, silent: bool):
    """Updates the template of a kedro project.

    Running this command is mandatory to use kedro-dagster.

    This adds:
     - "conf/base/dagster.yml": This is a configuration file
     used for the dagster run parametrization.
     - "src/<python_package>/definitions.py": This is the
     dagster file where all dagster definitions are set.
    """

    dagster_yml = "dagster.yml"
    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
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


@dagster_commands.command()
@click.option(
    "--env",
    "-e",
    required=False,
    default="local",
    help="The environment within conf folder we want to retrieve",
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
):
    """Opens the dagster dev user interface with the
    project-specific settings of `dagster.yml`.
    """

    project_path = _find_kedro_project(Path.cwd()) or Path.cwd()
    bootstrap_project(project_path)

    with KedroSession.create(
        project_path=project_path,
        env=env,
    ) as session:
        context = session.load_context()
        dagster_config = get_dagster_config(context)
        python_file = dagster_config.dev.python_file
        log_level = log_level or dagster_config.dev.log_level
        log_format = log_format or dagster_config.dev.log_format
        host = host or dagster_config.dev.host
        port = port or dagster_config.dev.port
        live_data_poll_rate = live_data_poll_rate or dagster_config.dev.live_data_poll_rate

        # call dagster dev with specific options
        subprocess.call([
            "dagster",
            "dev",
            "--python-file",
            python_file,
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
        ])
