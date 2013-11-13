import unittest
import doctest
import zc.customdoctests
from crate.testing.layer import CrateLayer
import os
import requests
import shutil

from crate.client.crash import CrateCmd
cmd = CrateCmd()

here = os.path.dirname(__file__)


def project_path(*parts):
    return os.path.join(os.path.dirname(docs_path()), *parts)


def docs_path(*parts):
    return os.path.join(os.path.dirname(os.path.dirname(here)), *parts)


def crate_path(*parts):
    return docs_path('tmp', 'crate', *parts)


def bash_transform(s):
    return (
        r'import subprocess;'
        r'print(subprocess.check_output(r"""%s""",stderr=subprocess.STDOUT,shell=True))' % s) + '\n'


def crash_transform(s):
    return ('cmd.onecmd("""{0}""");'.format(s.strip()))


bash_parser = zc.customdoctests.DocTestParser(
    ps1='sh\$', comment_prefix='#', transform=bash_transform)

crash_parser = zc.customdoctests.DocTestParser(
    ps1='cr>', comment_prefix='#', transform=crash_transform)


empty_layer = CrateLayer('crate',
                         crate_home=crate_path(),
                         crate_exec=crate_path('bin', 'crate'),
                         port=9200,)


def setUpLocations(test):
    test.globs['cmd'] = cmd
    requests.put('http://localhost:9200/locations', '''
    index :
     number_of_shards : 2
     number_of_replicas : 0
    ''')
    requests.put('http://localhost:9200/locations/default/_mapping',
                 open(project_path('sql/src/test/resources/essetup/mappings',
                                   'test_a.json')))

    requests.post('http://localhost:9200/_bulk?refresh=true',
                  open(project_path('sql/src/test/resources/essetup/data',
                                    'test_a.json')))

    print(project_path('sql/src/test/resources/essetup/data', 'test_a.json'))

def setUpQuotes(test):
    test.globs['cmd'] = cmd
    requests.put('http://localhost:9200/quotes', '''
    index :
     number_of_shards : 2
     number_of_replicas : 0
    ''')
    requests.put('http://localhost:9200/quotes/default/_mapping',
                 open(project_path('sql/src/test/resources/essetup/mappings',
                                   'test_b.json')))

    crate_wd = empty_layer.wdPath()
    cluster_name = "Testing9200"
    import_dir = os.path.join(crate_wd, cluster_name, "nodes", "0", "import_data")
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(project_path('sql/src/test/resources/essetup/data/copy', 'test_copy_from.json'),
                os.path.join(import_dir, "quotes.json"))


def setUpLocationsAndQuotes(test):
    setUpLocations(test)
    setUpQuotes(test)

def setUp(test):
    test.globs['cmd'] = cmd

def tearDownDropQuotes(test):
    cmd.onecmd("drop table quotes")

def test_suite():
    suite = unittest.TestSuite()
    for fn in ('hello.txt', 'blob.txt'):
        s = doctest.DocFileSuite('../../' + fn,
                                 parser=bash_parser,
                                 tearDown=tearDownDropQuotes,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                 doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    for fn in ('sql/rest.txt',):
        s = doctest.DocFileSuite('../../' + fn,
                                 parser=bash_parser,
                                 setUp=setUpLocations,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                 doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    for fn in ('sql/dml.txt', 'sql/occ.txt', 'sql/ddl.txt', 'sql/information_schema.txt'):
        s = doctest.DocFileSuite('../../' + fn, parser=crash_parser,
                                 setUp=setUpLocationsAndQuotes,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                 doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    return suite
