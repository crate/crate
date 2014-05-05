import crate.rtd_theme

project = u'Crate Data'
copyright = u'2014, CRATE Technology GmbH'

# The suffix of source filenames.
source_suffix = '.txt'
exclude_patterns = ['requirements.txt']
# The master toctree document.
master_doc = 'index'

nitpicky = True

html_show_sourcelink = False

exclude_trees = ['pyenv', 'tmp', 'out', 'parts', 'clients', 'eggs']

extensions = ['sphinx.ext.autodoc']

html_theme = 'crate'
html_theme_path = crate.rtd_theme.get_html_theme_path()

html_favicon = 'out/html/_static/img/favicon.ico'

html_sidebars = {'**': ['crate-sidebar.html', 'sourcelink.html']}

logo = 'img/logo_crate.png'

html_theme_options = {
    # HTML navbar class (Default: "navbar") to attach to <div> element.
    # For black navbar, do "navbar navbar-inverse"
    'navbar_class': 'navbar navbar-inverse',

    # Fix navigation bar to top of page?
    # Values: "true" (default) or "false"
    'navbar_fixed_top': 'true',

    'globaltoc_includehidden': 'true',
}
