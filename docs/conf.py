import os
import sys

import crate_theme.sphinx


project = u'Crate DB'
copyright = u'2013, Crate Technology GmbH'

# The suffix of source filenames.
source_suffix = '.txt'
# The master toctree document.
master_doc = 'index'

nitpicky = True

html_show_sourcelink = False


exclude_trees = ['pyenv', 'tmp', 'out', 'crate-python/tmp']

extensions = ['sphinx.ext.autodoc']

html_theme = 'bootstrap'
html_theme_path = crate_theme.sphinx.get_html_theme_path()

html_theme_options = {
    'bootswatch_theme': "crate",
}