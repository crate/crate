import os
import re
import faulthandler
import logging
import unittest
import pathlib
from functools import partial
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path, project_root
from crate.testing.layer import CrateLayer
from sqllogictest import run_file

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()
CRATE_PSQL_PORT = GLOBAL_PORT_POOL.get()

tests_path = pathlib.Path(os.path.abspath(os.path.join(
    project_root, 'blackbox', 'sqllogictest', 'testfiles', 'test')))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
FILE_WHITELIST = [re.compile(o) for o in [
    'select[1-4].test',
    'random/select/slt_good_\d+.test',
    'random/groupby/slt_good_\d+.test',
]]


class TestMaker(type):

    def __new__(cls, name, bases, attrs):
        for filename in tests_path.glob('**/*.test'):
            filepath = tests_path / filename
            relpath = str(filepath.relative_to(tests_path))
            if not any(p.match(str(relpath)) for p in FILE_WHITELIST):
                continue
            attrs['test_' + relpath] = partial(
                run_file,
                filename=str(filepath),
                host='localhost',
                port=str(CRATE_PSQL_PORT),
                log_level=logging.WARNING,
                log_file='sqllogic.log',
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
            'stats.enabled': True,
            'psql.port': CRATE_PSQL_PORT
        }
    )
    suite.layer = crate_layer
    return suite
