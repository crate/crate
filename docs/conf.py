import os
import sys

import crate_theme.sphinx

project = u'Crate Data'
copyright = u'2014, CRATE Technology GmbH'

# The suffix of source filenames.
source_suffix = '.txt'
# The master toctree document.
master_doc = 'index'

nitpicky = True

html_show_sourcelink = False

exclude_trees = ['pyenv', 'tmp', 'out', 'parts', 'clients', 'eggs']

extensions = ['sphinx.ext.autodoc']

html_theme = 'docs_admin'
html_theme_path = crate_theme.sphinx.get_html_theme_path()

html_favicon = 'out/html/_static/img/favicon.ico'

html_sidebars = {'**': ['crate-sidebar.html', 'sourcelink.html', 'searchbox.html']}

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
    'bootstrap_theme': "crate-docs-admin",
}
