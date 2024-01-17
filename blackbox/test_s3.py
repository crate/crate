#!/usr/bin/env python3

# Licensed to Crate.io GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.


import os
import platform
import shutil
import tempfile
import threading
import unittest
import socket
import json
from pathlib import Path
from crate.client import connect
from cr8.run_crate import CrateNode, wait_until
from subprocess import Popen, PIPE, DEVNULL
from testutils.paths import crate_path
from urllib.request import urlretrieve
from minio import Minio


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
        'Linux-aarch64': 'https://dl.min.io/server/minio/release/linux-arm64/minio',
        'Darwin-x86_64': 'https://dl.min.io/server/minio/release/darwin-amd64/minio',
        'Darwin-arm64': 'https://dl.min.io/server/minio/release/darwin-arm64/minio'
    }

    MINIO_ACCESS_KEY = 'minio'
    MINIO_SECRET_KEY = 'miniostorage/'

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
        env['MINIO_ACCESS_KEY'] = MinioServer.MINIO_ACCESS_KEY
        env['MINIO_SECRET_KEY'] = MinioServer.MINIO_SECRET_KEY
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
        client = Minio('127.0.0.1:9000',
                   access_key = MinioServer.MINIO_ACCESS_KEY,
                   secret_key = MinioServer.MINIO_SECRET_KEY,
                   secure = False)

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
                    WITH (access_key = 'minio', secret_key = 'miniostorage/', bucket='backups', endpoint = '127.0.0.1:9000', protocol = 'http')
                """)

                # Make sure access_key and secret_key are masked in sys.repositories
                c.execute('''SELECT settings['access_key'], settings['secret_key'] from sys.repositories where name = 'r1' ''')
                settings = c.fetchone()
                self.assertEqual(settings[0], '[xxxxx]')
                self.assertEqual(settings[1], '[xxxxx]')

                c.execute('CREATE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)')
                c.execute('DROP TABLE t1')
                c.execute('RESTORE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)')
                c.execute('SELECT COUNT(*) FROM t1')
                rowcount = c.fetchone()[0]
                self.assertEqual(rowcount, 3)
                c.execute('DROP SNAPSHOT r1.s1')
                c.execute('''SELECT COUNT(*) FROM sys.snapshots WHERE name = 's1' ''')
                rowcount = c.fetchone()[0]
                self.assertEqual(rowcount, 0)

                [self.assertEqual(n.object_name.endswith('.dat'), False) for n in client.list_objects('backups')]


class S3CopyIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate_node.start()

    @classmethod
    def tearDownClass(cls):
        crate_node.stop()

    def test_basic_copy_to_and_copy_from(self):
        client = Minio('127.0.0.1:9000',
                       access_key = MinioServer.MINIO_ACCESS_KEY,
                       secret_key = MinioServer.MINIO_SECRET_KEY,
                       region = "myRegion",
                       secure = False)

        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            client.make_bucket("my-bucket")

            with connect(crate_node.http_url) as conn:
                c = conn.cursor()
                c.execute('CREATE TABLE t1 (x int)')
                c.execute('INSERT INTO t1 (x) VALUES (1), (2), (3)')
                c.execute('REFRESH TABLE t1')

                c.execute("""
                    COPY t1 TO DIRECTORY 's3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key' WITH (protocol = 'http')
                """)
                c.execute('CREATE TABLE r1 (x int)')
                c.execute("""
                    COPY r1 FROM 's3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/k*y/*' WITH (protocol = 'http')
                """)
                c.execute('REFRESH TABLE r1')

                c.execute('SELECT x FROM r1 order by x')
                result = c.fetchall()
                self.assertEqual(result, [[1],[2],[3]])

                c.execute("""
                    COPY r1 FROM ['s3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key/t1_0_.json',
                    's3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key/t1_1_.json',
                    's3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key/t1_2_.json',
                    's3://minio:miniostorage%2F@127.0.0.1:9000/my-bucket/key/t1_3_.json'] WITH (protocol = 'http')
                """)
                c.execute('REFRESH TABLE r1')

                c.execute('SELECT x FROM r1 order by x')
                result = c.fetchall()
                self.assertEqual(result, [[1],[1],[2],[2],[3],[3]])

                c.execute('DROP TABLE t1')
                c.execute('DROP TABLE r1')

    def test_anonymous_read_write(self):
        client = Minio('127.0.0.1:9000',
                       access_key = MinioServer.MINIO_ACCESS_KEY,
                       secret_key = MinioServer.MINIO_SECRET_KEY,
                       region = "myRegion",
                       secure = False)

        with MinioServer() as minio:
            t = threading.Thread(target=minio.run)
            t.daemon = True
            t.start()
            wait_until(lambda: _is_up('127.0.0.1', 9000))

            client.make_bucket("my.bucket")

            # https://docs.min.io/docs/python-client-api-reference.html#set_bucket_policy
            # Example anonymous read-write bucket policy.
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": [
                            "s3:GetBucketLocation",
                            "s3:ListBucket",
                            "s3:ListBucketMultipartUploads",
                        ],
                        "Resource": "arn:aws:s3:::my.bucket",
                    },
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListMultipartUploadParts",
                            "s3:AbortMultipartUpload",
                        ],
                        "Resource": "arn:aws:s3:::my.bucket/*",
                    },
                ],
            }
            client.set_bucket_policy("my.bucket", json.dumps(policy))

            with connect(crate_node.http_url) as conn:
                c = conn.cursor()
                c.execute('CREATE TABLE t1 (x int)')
                c.execute('INSERT INTO t1 (x) VALUES (1), (2), (3)')
                c.execute('REFRESH TABLE t1')

                c.execute("""
                    COPY t1 TO DIRECTORY 's3://127.0.0.1:9000/my.bucket/' WITH (protocol = 'http')
                """)
                c.execute("""
                    COPY t1 FROM 's3://127.0.0.1:9000/my.bucket/*' WITH (protocol = 'http')
                """)
                c.execute('REFRESH TABLE t1')

                c.execute('SELECT x FROM t1 order by x')
                result = c.fetchall()
                self.assertEqual(result, [[1],[1],[2],[2],[3],[3]]) # two sets because copied to/from the same table

                c.execute('DROP TABLE t1')
