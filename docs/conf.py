# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
from importlib.metadata import version as get_version

project = "kedro-dagster"
copyright = "2024"
author = "G. Tauzin"
release = get_version("kedro_dagster")
version = ".".join(release.split(".")[:3])

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx_design",
    "sphinx-prompt",
    "sphinx_gallery.gen_gallery",
    "numpydoc",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_templates", "Thumbs.db", ".DS_Store"]

# The reST default role (used for this markup: `text`) to use for all
# documents.
default_role = "literal"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_style = "css/kedro_dagster.css"
html_logo = "_static/img/logo.jpg"
# html_favicon = "_static/img/favicon.ico"
html_css_files = [
    "css/kedro_dagster.css",
]
html_sidebars = {
    "quick_start": [],
    "user_guide": [],
    "auto_examples/index": [],
}

html_theme_options = {
    "external_links": [],
    "use_edit_page_button": True,
    "show_toc_level": 1,
}

html_context = {
    "github_user": "gtauzin",
    "github_repo": "kedro-dagster",
    "github_version": "main",
    "doc_path": "doc",
}
linkcheck_ignore = [r"https://github.com/gtauzin/kedro-dagster"]

# -- Options for autodoc ------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "inherited-members": True,
}

# generate autosummary even if no references
autosummary_generate = True

# -- Options for numpydoc -----------------------------------------------------

# this is needed for some reason...
# see https://github.com/numpy/numpydoc/issues/69
numpydoc_show_class_members = False

# -- Options for intersphinx --------------------------------------------------

intersphinx_mapping = {
    "python": (f"https://docs.python.org/{sys.version_info.major}", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/reference", None),
    "scikit-learn": ("https://scikit-learn.org/stable", None),
    "matplotlib": ("https://matplotlib.org/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "joblib": ("https://joblib.readthedocs.io/en/latest/", None),
}

# -- Options for sphinx-gallery -----------------------------------------------

# Generate the plot for the gallery
sphinx_gallery_conf = {
    "plot_gallery": "True",
    "doc_module": "kedro-dagster",
    "backreferences_dir": os.path.join("generated"),
    "examples_dirs": "../examples",
    "gallery_dirs": "auto_examples",
    "reference_url": {},
}
