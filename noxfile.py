"""Nox sessions."""

import nox

# Require Nox version 2024.3.2 or newer to support the 'default_venv_backend' option
nox.needs_version = ">=2024.3.2"

# Set 'uv' as the default backend for creating virtual environments
nox.options.default_venv_backend = "uv|virtualenv"

# Default sessions to run when nox is called without arguments
nox.options.sessions = ["fix", "tests_coverage", "serve_docs"]


# --------------------------------------------------------------------------------------
# Compatibility matrix for Kedro and Dagster
# Update these lists to expand or narrow the test matrix.
# We prefer spec ranges over exact pins so latest patch for each line is exercised.
# Examples:
#   "kedro>=0.19,<1.0" installs latest 0.19.x
#   "dagster>=1.10,<1.11" installs latest 1.10.x
# --------------------------------------------------------------------------------------
KEDRO_SPECS = [
    "kedro>=0.19,<1.0",
    "kedro>=1.0,<1.1",
]

# Keep dagster and dagster-webserver on the same minor line where possible
DAGSTER_SPECS = [
    "dagster>=1.10,<1.11",
    "dagster>=1.11,<1.12",
    "dagster>=1.12,<1.13",
]


# Test sessions for different Python versions
@nox.session(python=["3.13"], venv_backend="uv")
def tests_coverage(session: nox.Session) -> None:
    """Run the tests with pytest under the specified Python version."""
    session.env["COVERAGE_FILE"] = f".coverage.{session.python}"
    session.env["COVERAGE_PROCESS_START"] = "pyproject.toml"

    # Install dependencies
    session.run_install(
        "uv",
        "sync",
        "--extra",
        "mlflow",
        "--no-default-groups",
        "--group",
        "tests",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )

    # Clears all .coverage* files
    session.run("coverage", "erase")

    # Run unit tests under coverage
    session.run(
        "coverage",
        "run",
        "--parallel-mode",
        "--source=src/kedro_dagster",
        "-m",
        "pytest",
        "tests",
        *session.posargs,
    )

    # Combine coverage data from parallel runs
    session.run("coverage", "combine")

    # HTML report, ignoring parse errors and without contexts
    session.run("coverage", "html", "--ignore-errors", "-d", session.create_tmp())

    # XML report for CI
    session.run("coverage", "xml", "-o", f"coverage.{session.python}.xml")


@nox.session(python=["3.10", "3.11", "3.12", "3.13"], venv_backend="uv")
@nox.parametrize("kedro_spec", KEDRO_SPECS)
@nox.parametrize("dagster_spec", DAGSTER_SPECS)
@nox.parametrize("with_mlflow", [False, True])
def tests_versions(session: nox.Session, dagster_spec: str, kedro_spec: str, with_mlflow: bool) -> None:
    """Run the test suite across a matrix of Kedro and Dagster versions.

    This installs the project with test dependencies, then overrides Kedro, Dagster,
    and Dagster Webserver to the specified constraints using uv.
    """
    # Install base deps (tests group + optional mlflow extra)
    sync_args = [
        "uv",
        "sync",
        "--no-default-groups",
        "--group",
        "tests",
    ]
    if with_mlflow:
        sync_args += ["--extra", "mlflow"]
    session.run_install(*sync_args, env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location})

    # Install specific Kedro / Dagster lines for this run
    # Keep dagster-webserver aligned with dagster line
    # TODO
    # webserver_spec = dagster_spec.replace("dagster", "dagster-webserver", 1)
    session.run_install(
        "uv",
        "pip",
        "install",
        kedro_spec,
        dagster_spec,
        # webserver_spec,
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )

    # Unit tests directly (no coverage for version tests)
    session.run(
        "pytest",
        "tests",
        *session.posargs,
    )


@nox.session(venv_backend="uv")
def fix(session: nox.Session) -> None:
    """Format the code base to adhere to our styles, and complain about what we cannot do automatically."""
    # Install dependencies
    session.run_install(
        "uv",
        "sync",
        "--no-default-groups",
        "--group",
        "fix",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Run pre-commit
    session.run("pre-commit", "run", "--all-files", "--show-diff-on-failure", *session.posargs, external=True)


@nox.session(venv_backend="uv")
def build_docs(session: nox.Session) -> None:
    """Run a development server for working on documentation."""
    # Install dependencies
    session.run_install(
        "uv",
        "sync",
        "--no-default-groups",
        "--group",
        "docs",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Build the docs
    session.run("mkdocs", "build", "--clean", external=True)


@nox.session(venv_backend="uv")
def serve_docs(session: nox.Session) -> None:
    """Run a development server for working on documentation."""
    # Install dependencies
    session.run_install(
        "uv",
        "sync",
        "--no-default-groups",
        "--group",
        "docs",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Build and serve the docs
    session.run("mkdocs", "build", "--clean", external=True)
    session.log("###### Starting local server. Press Control+C to stop server ######")
    session.run("mkdocs", "serve", "-a", "localhost:8080", external=True)


@nox.session(venv_backend="uv")
def deploy_docs(session: nox.Session) -> None:
    """Build fresh docs and deploy them."""
    # Install dependencies
    session.run_install(
        "uv",
        "sync",
        "--no-default-groups",
        "--group",
        "docs",
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )
    # Deploy docs to GitHub pages
    session.run("mkdocs", "gh-deploy", "--clean", external=True)
