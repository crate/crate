from crate.theme.rtd.conf.crate_server import *

site_url = 'https://crate.io/docs/crate/reference/en/latest/'

exclude_patterns = ['out/**', 'tmp/**', 'eggs/**', 'requirements.txt']

extensions = ['crate.sphinx.csv', 'sphinx_sitemap']
# crate.theme sets html_favicon to favicon.png which causes a warning because it should be a .ico
# and in addition there is no favicon.png in this project so it can't find the file
html_favicon = None
