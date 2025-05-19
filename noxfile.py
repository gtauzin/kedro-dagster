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

    # Run behavior tests
    session.run("behave", "-vv", "features", external=True)

    # Run unit tests
    session.run(
        "pytest",
        "--cov=src/kedro_dagster",
        f"--cov-report=html:{session.create_tmp()}/htmlcov",
        f"--cov-report=xml:coverage.{session.python}.xml",
        f"--junitxml=junit.{session.python}.xml",
        "-n",
        "auto",
        "tests",
        *session.posargs,
        external=True,
    )

    # Run diff-cover
    diff_against = session.env.get("DIFF_AGAINST", "origin/main")
    session.run("diff-cover", "--compare-branch", diff_against, f"coverage.{session.python}.xml", external=True)


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
