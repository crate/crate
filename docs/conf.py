from crate.theme.rtd.conf.crate_reference import *

exclude_patterns = ['out/**', 'tmp/**', 'eggs/**', 'requirements.txt', 'README.rst']

extensions.append('crate.sphinx.csv')

linkcheck_ignore = [
    'https://www.iso.org/obp/ui/.*'  # Breaks accessibility via JS ¯\_(ツ)_/¯
]
linkcheck_retries = 3
