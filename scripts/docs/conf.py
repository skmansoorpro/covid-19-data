# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))
import os

# Needed for Read The Docs documentation setup
ENV_VARS = {
    "OWID_COVID_PROJECT_DIR": {
        "current": os.environ.get("OWID_COVID_PROJECT_DIR"),
        "fallback": (project_dir := os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))),
    },
    "OWID_COVID_PROJECT_DIR": {
        "current": os.environ.get("OWID_COVID_CONFIG"),
        "fallback": os.path.join(project_dir, "scripts", "config.yaml"),
    },
}
for var_name, var_values in ENV_VARS.items():
    if var_values["current"] is None:
        print(f"Setting env variable {var_name}")
        os.environ[var_name] = var_values["fallback"]
print("----------------------------------------")
# -- Project information -----------------------------------------------------

project = "cowidev"
copyright = "2022, Our World in Data"
author = "Our World in Data"

# The full version, including alpha/beta/rc tags
release = "0.0.1.dev0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx_click", "myst_parser"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
