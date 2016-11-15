import unittest
import os
import faulthandler
import glob
from functools import partial
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path, project_root
from crate.testing.layer import CrateLayer
from sqllogictest import run_file

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()

tests_path = os.path.abspath(os.path.join(
    project_root, 'blackbox', 'sqllogictest', 'testfiles', 'test'))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
whitelist = set([
    'select1.test',
    'random/select/slt_good_0.test',
])


class TestMaker(type):

    def __new__(cls, name, bases, attrs):
        glob_pattern = os.path.join(tests_path, '**/*.test')
        for filename in glob.glob(glob_pattern, recursive=True):
            filepath = os.path.join(tests_path, filename)
            relpath = os.path.relpath(filepath, start=tests_path)
            if relpath not in whitelist:
                continue
            attrs['test_' + filename] = partial(
                run_file,
                fh=open(filepath, 'r', encoding='utf-8'),
                hosts='localhost:' + str(CRATE_HTTP_PORT),
                verbose=False,
                failfast=True
            )
        return type.__new__(cls, name, bases, attrs)


class SqlLogicTest(unittest.TestCase, metaclass=TestMaker):
    pass


def test_suite():
    suite = unittest.TestSuite(unittest.makeSuite(SqlLogicTest))
    crate_layer = CrateLayer(
        'crate-sqllogic',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT,
        settings={
            'stats.enabled': True
        }
    )
    suite.layer = crate_layer
    return suite
