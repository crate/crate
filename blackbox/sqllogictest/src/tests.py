import unittest
import os
from functools import partial
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path, project_root
from crate.testing.layer import CrateLayer
from sqllogictest import run_file

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()

tests_path = os.path.abspath(os.path.join(
    project_root, 'blackbox', 'sqllogictest', 'src', 'tests'))


class TestMaker(type):

    def __new__(cls, name, bases, attrs):
        for filename in os.listdir(tests_path):
            filepath = os.path.join(tests_path, filename)
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
        transport_port=CRATE_TRANSPORT_PORT
    )
    suite.layer = crate_layer
    return suite
