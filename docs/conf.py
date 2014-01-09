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
html_favicon = 'favicon.png'

exclude_trees = ['pyenv', 'tmp', 'out', 'crate-python/tmp']

extensions = ['sphinx.ext.autodoc']

html_theme = 'bootstrap'
html_theme_path = crate_theme.sphinx.get_html_theme_path()

html_theme_options = {
    # HTML navbar class (Default: "navbar") to attach to <div> element.
    # For black navbar, do "navbar navbar-inverse"
    'navbar_class': "navbar navbar-inverse",

    # Fix navigation bar to top of page?
    # Values: "true" (default) or "false"
    'navbar_fixed_top': "true",

    # Bootstrap theme.
    #
    # Themes:
    # * crate-docs
    # * crate-admin
    # * crate-web
    'bootstrap_theme': "crate-docs",
}