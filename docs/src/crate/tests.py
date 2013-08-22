import unittest
import doctest
import subprocess
import zc.customdoctests
import subprocess
from crate.testing.layer import CrateLayer
import os

here = os.path.dirname(__file__)


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



def test_suite():
    suite = unittest.TestSuite()
    for fn in ('hello.txt', 'blob.txt'):
        s = doctest.DocFileSuite('../../' +  fn, parser=parser,
                             optionflags=doctest.NORMALIZE_WHITESPACE |
                             doctest.ELLIPSIS)
        s.layer = empty_layer
        suite.addTest(s)
    return suite
