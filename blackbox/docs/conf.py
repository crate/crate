from crate.theme.rtd.conf.crate_reference import *

exclude_patterns = ['out/**', 'tmp/**', 'eggs/**', 'requirements.txt', 'README.rst']

extensions = ['crate.sphinx.csv', 'sphinx_sitemap']

# Enable version chooser.
html_context.update({
    "display_version": True,
})
