import os
import sys

sys.path.append(os.path.abspath('.'))
sys.path.append(os.path.abspath('_themes'))


project = u'Crate DB'
copyright = u'2013, Crate Technology GmbH'

# The suffix of source filenames.
source_suffix = '.txt'
# The master toctree document.
master_doc = 'index'

html_theme = 'crate'
html_theme_path = ['_themes']
html_static_path = ['_static']
nitpicky = True

html_show_sourcelink = False


exclude_trees = ['pyenv', 'tmp', 'out', 'crate-python/tmp']

extensions = ['sphinx.ext.autodoc']