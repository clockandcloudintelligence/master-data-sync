import os
import sys

# Add the parent directory to sys.path so we can import our modules
sys.path.insert(0, os.path.abspath('../raw_materials'))
sys.path.insert(0, os.path.abspath('../logistics'))

# Confirm paths for debugging
print("Current sys.path:")
for path in sys.path:
    print(path)

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'raw-materials-data-sync'
copyright = '2024, Syed Farooq Hassan'
author = 'Syed Farooq Hassan'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',  # Optional, for type hints in docs
]

templates_path = ['_templates']
exclude_patterns = []



autodoc_default_options = {
    'member-order': 'groupwise',  # Orders members by type (grouping class attributes separately)
    'undoc-members': True,
    'exclude-members': '__weakref__',
    'no-index': True,  # Adds :no-index: to all members automatically if you prefer
}



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

