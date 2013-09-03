import unittest
import doctest
import subprocess
import zc.customdoctests
import subprocess
from crate.testing.layer import CrateLayer
import os
import requests

here = os.path.dirname(__file__)

def project_path(*parts):
    return os.path.join(os.path.dirname(docs_path()), *parts)

def docs_path(*parts):
    return os.path.join(os.path.dirname(os.path.dirname(here)), *parts)

def crate_path(*parts):
    return docs_path('tmp', 'crate', *parts)

def transform(s):
    return (r'import subprocess;print subprocess.check_output(r"""%s""",stderr=subprocess.STDOUT,shell=True)' % s) + '\n'

parser = zc.customdoctests.DocTestParser(
    ps1='sh\$', comment_prefix='#', transform=transform)



empty_layer = CrateLayer('crate',
                         crate_home=crate_path(),
                         crate_exec=crate_path('bin', 'crate'),
                         port=9200,
    )


def setUpLocations(test):
    requests.put('http://localhost:9200/locations', '''
    index :
     number_of_shards : 2
     number_of_replicas : 0
    ''')
    requests.put('http://localhost:9200/locations/default/_mapping',
        file(project_path('sql/src/test/resources/essetup/mappings',
                          'test_a.json')))

    requests.post('http://localhost:9200/_bulk?refresh=true',
        file(project_path('sql/src/test/resources/essetup/data',
                          'test_a.json')))

    print project_path('sql/src/test/resources/essetup/data', 'test_a.json')


def test_suite():
    suite = unittest.TestSuite()
    for fn in ('hello.txt', 'blob.txt'):
        s = doctest.DocFileSuite('../../' +  fn, parser=parser,
                             optionflags=doctest.NORMALIZE_WHITESPACE |
                             doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    for fn in ('sql.txt',):
        s = doctest.DocFileSuite('../../' +  fn, parser=parser,
                                 setUp=setUpLocations,
                             optionflags=doctest.NORMALIZE_WHITESPACE |
                             doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    return suite
