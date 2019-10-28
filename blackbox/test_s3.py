#!/usr/bin/env python3

import os
import platform
import shutil
import tempfile
import threading
import unittest
import socket
from pathlib import Path
from crate.client import connect
from cr8.run_crate import CrateNode, wait_until
from subprocess import Popen, PIPE, DEVNULL
from testutils.paths import crate_path
from urllib.request import urlretrieve


crate_node = CrateNode(
    crate_dir=crate_path(),
    settings={
        'transport.tcp.port': 0,
        'psql.port': 0,
    },
    env={
        'CRATE_HEAP_SIZE': '256M'
    },
    version=(4, 0, 0)
)


def _is_up(host: str, port: int) -> bool:
    try:
        conn = socket.create_connection((host, port))
        conn.close()
        return True
    except (socket.gaierror, ConnectionRefusedError):
        return False


class MinioServer:

    MINIO_URLS = {
        'Linux-x86_64': 'https://dl.min.io/server/minio/release/linux-amd64/minio',
        'Darwin-x86_64': 'https://dl.min.io/server/minio/release/darwin-amd64/minio'
    }

    CACHE_ROOT = Path(os.environ.get('XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache')))
    CACHE_DIR = CACHE_ROOT / 'crate-tests'

    def __init__(self):
        self.minio_path = self._get_minio()
        self.data_dir = data_dir = Path(tempfile.mkdtemp())
        # Create base_path
        os.makedirs(data_dir / 'backups')
        self.process = None

    def _get_minio(self):
        minio_dir = MinioServer.CACHE_DIR / 'minio'
        minio_path = minio_dir / 'minio'
        if not os.path.exists(minio_path):
            os.makedirs(minio_dir, exist_ok=True)
            minio_url = MinioServer.MINIO_URLS[f'{platform.system()}-{platform.machine()}']
            urlretrieve(minio_url, minio_path)
        minio_path.chmod(0o755)
        return minio_path

    def run(self):
        cmd = [self.minio_path, 'server', str(self.data_dir)]
        env = os.environ.copy()
        env['MINIO_ACCESS_KEY'] = 'minio'
        env['MINIO_SECRET_KEY'] = 'miniostorage'
        self.process = Popen(
            cmd,
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            env=env,
            universal_newlines=True
        )

    def close(self):
        if self.process:
            self.process.terminate()
            self.process.communicate(timeout=10)
            self.process = None
        shutil.rmtree(self.data_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class S3SnapshotIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate_node.start()

    @classmethod
    def tearDownClass(cls):
        crate_node.stop()

    def test_copy_to_s3_copy_from_s3_roundtrip(self):
        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            with connect(crate_node.http_url) as conn:
                c = conn.cursor()
                c.execute('CREATE TABLE t1 (x int)')
                c.execute('INSERT INTO t1 (x) VALUES (1), (2), (3)')
                c.execute('REFRESH TABLE t1')
                c.execute("""
                    CREATE REPOSITORY r1 TYPE S3
                    WITH (access_key = 'minio', secret_key = 'miniostorage', bucket='backups', endpoint = '127.0.0.1:9000', protocol = 'http')
                """)
                c.execute('CREATE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)')
                c.execute('DROP TABLE t1')
                c.execute('RESTORE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)')
                c.execute('SELECT COUNT(*) FROM t1')
                rowcount = c.fetchone()[0]
                self.assertEqual(rowcount, 3)
