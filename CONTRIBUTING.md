# Introduction

Thank you for considering contributing to Kedro-Dagster! We welcome contributions in the form of pull requests, issues or code reviews. You can add to code, or simply send us spelling and grammar fixes or extra tests. Contribute anything that you think improves the community for us all!

The following sections describe our vision and the contribution process.

## Code of conduct

The Kedro team pledges to foster and maintain a welcoming and friendly community in all of our spaces. All members of our community are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md), and we will do our best to enforce those principles and build a happy environment where everyone is treated with respect and dignity.

## Get started

We use [GitHub Issues](https://github.com/gtauzin/kedro-dagster/issues) to keep track of known bugs. We keep a close eye on them and try to make it clear when we have an internal fix in progress. Before reporting a new issue, please do your best to ensure your problem hasn't already been reported. If so, it's often better to just leave a comment on an existing issue, rather than create a new one. Old issues also can often include helpful tips and solutions to common problems.

If you are looking for help with your code, please consider posting a question on the [Kedro Slack organisation](https://slack.kedro.org/). You can post your questions to the `#questions` or the `#plugins-integrations` channel. Past questions and discussions from the Kedro Slack organisation are accessible on [Linen](https://linen-slack.kedro.org/). In the interest of community engagement we also believe that help is much more valuable if it's shared publicly, so that more people can benefit from it.

If you have already checked the [existing issues](https://github.com/gtauzin/kedro-dagster/issues) on GitHub and are still convinced that you have found odd or erroneous behaviour then please file a [new issue](https://github.com/gtauzin/kedro-dagster/issues/new/choose). We have a template that helps you provide the necessary information we'll need in order to address your query.

## Feature requests

### Suggest a new feature

If you have new ideas for Kedro-Dagster functionality then please open a [GitHub issue](https://github.com/gtauzin/kedro-dagster/issues) with the label `enhancement`. Please describe in your own words the feature you would like to see, why you need it, and how it should work.

### Contribute a new feature

If you're unsure where to begin contributing to Kedro-Dagster, please start by looking through the `good first issue` and `help wanted` on [GitHub](https://github.com/gtauzin/kedro-dagster/issues).

Typically, small contributions to `kedro-dagster` are more preferable due to an easier review process, but we accept any new features if they prove to be essential for the functioning of the plugin or if we believe that they are used by most projects.

## Your first contribution

Working on your first pull request? You can learn how from these resources:

* [First timers only](https://www.firsttimersonly.com/)
* [How to contribute to an open source project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github)

### Guidelines

* Aim for cross-platform compatibility on Windows, macOS and Linux
* We use [uv](https://docs.astral.sh/uv/) for project and virtual environment management
* We use [SemVer](https://semver.org/) for versioning

Our code is designed to be compatible with Python 3.6 onwards and our style guidelines are (in cascading order):

* [PEP 8 conventions](https://www.python.org/dev/peps/pep-0008/) for all Python code
* [Google docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) for code comments
* [PEP 484 type hints](https://www.python.org/dev/peps/pep-0484/) for all user-facing functions / class methods e.g.

```python
def count_truthy(elements: List[Any]) -> int:
    return sum(1 for elem in elements if elem)
```

> *Note:* We only accept contributions under the [Apache 2.0](https://opensource.org/licenses/Apache-2.0) license and you should have permission to share the submitted code.

### Branching conventions

We use a branching model that helps us keep track of branches in a logical, consistent way. All branches should have the hyphen-separated convention of: `<type-of-change>/<short-description-of-change>` e.g. `feature/awesome-new-feature`

| Types of changes | Description                                                                 |
| ---------------- | --------------------------------------------------------------------------- |
| `docs`           | Changes to the documentation of the plugin                                  |
| `feature`        | Non-breaking change which adds functionality                                |
| `fix`            | Non-breaking change which fixes an issue                                    |
| `tests`          | Changes to project unit (`tests/`) and / or integration (`features/`) tests |

## Plugin contribution process

 1. Fork the project
 2. Develop your contribution in a new branch.
 3. Make sure all your commits are signed off by using `-s` flag with `git commit`.
 4. Open a PR against the `main` branch and sure that the PR title follows the [Conventional Commits specs](https://www.conventionalcommits.org/en/v1.0.0/).
 5. Make sure the CI builds are green (have a look at the section [Testing](#testing) below)
 6. Ensure the documentation changes render properly (see section [Documentation](#documentation))
 7. Update the PR according to the reviewer's comments

## Documentation

* The main documentation is in the `docs/` directory and is built with [MkDocs](https://www.mkdocs.org/). To build or serve the documentation locally, use:

  ```bash
  uvx nox -s build_docs
  ```

  or

  ```bash
  uv run mkdocs serve
  ```

* API reference and technical documentation are auto-generated from the code and docstrings. Please keep docstrings up to date when contributing code.
* When adding new features or making changes, update the relevant documentation pages in `docs/pages/` and ensure the navigation in `mkdocs.yml` is correct.

## Testing

* **Unit tests** are in the `tests/` directory and use `pytest`. Run them with:

  ```bash
  uv run pytest tests
  ```

* **Behavior (end-to-end) tests** are in the `features/` directory and use `behave`. Run them with:

  ```bash
  uv run behave features
  ```

* **Linting and formatting** is enforced with `ruff` and can be run with:

  ```bash
  uvx ruff check src/kedro-dagster
  ```

* **All checks and tests** can be run using [nox](https://nox.thea.codes/):

  ```bash
  uvx nox -s fix
  uvx nox -s tests
  ```

  This will run linting, formatting, unit tests, behavior tests, and coverage checks across supported Python versions.

The CI/CD pipeline will run all of these checks automatically on pull requests. Please ensure your code passes locally before submitting a PR.

## Docstring conventions

We use Google-style docstrings with inline types in parentheses for all public modules, classes, methods, and functions in `kedro_dagster`.

* Summary line first; keep lines to ~100 characters.
* Include an Args section whenever there are parameters. Format each as:
  * `name (type): description.`
* Include a Returns section when a value is returned. Format as:
  * `type: description.` If multiple values are returned, describe the structure (e.g., `tuple[type_a, type_b]: ...`).
* Prefer PEP 585 built-in generics: `list[str]`, `dict[str, Any]`, `tuple[int, str]`.
* Avoid leading articles in parameter descriptions. Prefer `Kedro catalog` over `The Kedro catalog`.
* State defaults in the description when helpful, and use `| None` for optional types.

Example:

"""Create a Dagster op wrapping a Kedro node.

Args:
    node (Node): Kedro node to wrap.
    is_in_first_layer (bool): Whether node is in the first topological layer.

Returns:
    OpDefinition: Configured Dagster op.
"""
