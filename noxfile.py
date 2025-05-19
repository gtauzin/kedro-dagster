"""Nox sessions."""

import nox

# Require Nox version 2024.3.2 or newer to support the 'default_venv_backend' option
nox.needs_version = ">=2024.3.2"

# Set 'uv' as the default backend for creating virtual environments
nox.options.default_venv_backend = "uv|virtualenv"

# Default sessions to run when nox is called without arguments
nox.options.sessions = ["fix", "tests", "serve_docs"]


# Test sessions for different Python versions
@nox.session(python=["3.10", "3.11", "3.12"], venv_backend="uv")
def tests(session: nox.Session) -> None:
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

    # Run behavior tests (run behave directly, not under coverage)
    session.run(
        "behave",
        "-vv",
        "features",
    )

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
