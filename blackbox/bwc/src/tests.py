"""
Module that contains basic bwc tests
"""

import unittest
import tempfile
import shutil
import time
import os
import glob
from typing import NamedTuple
from io import BytesIO
from testutils.paths import project_root
from crate.client import connect

# Uses CrateNode from cr8 instead of crate.testing.layer CrateLayer
# Because CrateLayer doesn't support customizing path.data behaviour and
# also doesn't support launching nodes version < 1.0.0
from cr8.run_crate import CrateNode, get_crate


class VersionDef(NamedTuple):
    version: str
    upgrade_segments: bool


CURRENT_TARBALL = next(iter(
    glob.glob(os.path.join(project_root, 'app', 'build', 'distributions', '*.tar.gz'))))
VERSIONS = (
    VersionDef('0.57.x', False),
    VersionDef('1.0.x', False),
    VersionDef('1.1.x', True),
    VersionDef('2.0.x', False),
    VersionDef(CURRENT_TARBALL, False)
)
TABLE_WITH_MOST_TYPES = '''
create table t1 (
    id int primary key,
    name string,
    f float,
    d double,
    point geo_point,
    shape geo_shape,
    ip_address ip,
    ts timestamp
) clustered into 2 shards with (number_of_replicas = 0)
'''

INSERT = '''
INSERT INTO t1
    (id, name, f, d, point, shape, ip_address, ts)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?)
'''

# Use statements that use different code paths to retrieve the values
SELECT_STATEMENTS = (
    'select * from t1',
    'select distinct id, name, f, d, ip_address, ts from t1',
    'select * from t1 where id = 1'
)


DUMMY_ROW = (
    1,
    'Arthur',
    238.4,
    838.42,
    [29.3, 21.1],
    'polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))',
    '127.0.0.1',
    int(time.time() * 1000)
)


def wait_for_active_shards(cursor):
    waited = 0
    duration = 0.01
    while waited < 20:
        cursor.execute("select count(*) from sys.shards where state = 'STARTED'")
        if int(cursor.fetchone()[0]) == 4:
            return
        time.sleep(duration)
        waited += duration
        duration *= 2
    raise TimeoutError("Shards didn't become active in time")


def run_selects(c, blob_container, digest):
    for stmt in SELECT_STATEMENTS:
        print('  ', stmt)
        c.execute(stmt)
    print('   Getting blob: ', digest)
    blob_container.get(digest)


class BwcTest(unittest.TestCase):

    def setUp(self):
        self._path_data = tempfile.mkdtemp()
        print(f'data path: {self._path_data}')
        self._on_stop = []

        def new_node(version):
            n = CrateNode(
                crate_dir=get_crate(version),
                settings={
                    'path.data': self._path_data,
                    'cluster.name': 'crate-bwc-tests'
                },
                keep_data=True
            )
            self._on_stop.append(n.stop)
            return n
        self._new_node = new_node

    def tearDown(self):
        print(f'Removing: {self._path_data}')
        shutil.rmtree(self._path_data, ignore_errors=True)
        self._process_on_stop()

    def _process_on_stop(self):
        for to_stop in self._on_stop:
            to_stop()
        self._on_stop.clear()

    def test_upgrade_from_054_to_latest(self):
        """ Test upgrade path from 0.54 to latest

        Creates a blob and regular table in 0.54 and inserts a record, then
        goes through all VERSIONS - each time verifying that a few simple
        selects work.
        """
        print(f'# Starting: 0.54.x')
        node = self._new_node('0.54.x')
        node.start()
        with connect(node.http_url) as conn:
            c = conn.cursor()
            c.execute(TABLE_WITH_MOST_TYPES)
            c.execute(INSERT, DUMMY_ROW)
            c.execute('refresh table t1')
            c.execute('create blob table b1 clustered into 2 shards with (number_of_replicas = 0)')
            container = conn.get_blob_container('b1')
            digest = container.put(BytesIO(b'sample data'))
            run_selects(c, container, digest)
        self._process_on_stop()
        for version, upgrade_segments in VERSIONS:
            print(f'# Starting: {version}')
            node = self._new_node(version)
            node.start()
            with connect(node.http_url) as conn:
                cursor = conn.cursor()
                wait_for_active_shards(cursor)
                if upgrade_segments:
                    cursor.execute('optimize table t1 with (upgrade_segments = true)')
                    cursor.execute('optimize table blob.b1 with (upgrade_segments = true)')
                blobs = conn.get_blob_container('b1')
                run_selects(cursor, blobs, digest)
            self._process_on_stop()


def test_suite():
    return unittest.makeSuite(BwcTest)
