# -*- coding: utf-8; -*-
#
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

import sys
import unittest
import os
import subprocess
import glob
import shutil
import time
import tarfile
import logging
from testutils.ports import bind_port
from testutils.paths import crate_path, project_root
from cr8.run_crate import CrateNode
from crate.client import connect
from urllib.error import HTTPError
from urllib.request import urlretrieve

HADOOP_VERSION = '2.10.1'
HADOOP_SOURCE = ('http://www-eu.apache.org/dist/hadoop/common/'
                 'hadoop-{version}/hadoop-{version}.tar.gz'.format(version=HADOOP_VERSION))
CACHE_DIR = os.environ.get(
    'XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache', 'crate-tests'))


NN_PORT = bind_port()


hdfs_repo_libs_path = os.path.join(
    project_root,
    'plugins',
    'es-repository-hdfs',
    'build',
    'extraLibs')

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


def add_hadoop_libs(hdfs_repo_libs_path, path_to_dist):
    hdfs_plugin_location = os.path.join(path_to_dist, 'plugins', 'es-repository-hdfs')
    for filename in glob.glob(os.path.join(hdfs_repo_libs_path, '*.jar')):
        # full_file_name = os.path.join(hdfs_repo_libs_path, filename)
        if (os.path.isfile(filename)):
            shutil.copy(filename, hdfs_plugin_location)


def wait_for_minicluster(log, timeout=60):
    start = time.time()
    while True:
        line = log.readline().decode('utf-8').strip()
        elapsed = time.time() - start
        sys.stderr.write(line + '\n')
        if line.endswith('Cluster is active'):
            return True
        elif elapsed > timeout:
            return False


class HadoopLayer:

    def __init__(self):
        self.hadoop_path = hadoop_path = self._get_hadoop()
        self.hadoop_bin = os.path.join(hadoop_path, 'bin', 'hadoop')
        self.hadoop_mapreduce_client = os.path.join(
            hadoop_path, 'share', 'hadoop', 'mapreduce',
            'hadoop-mapreduce-client-jobclient-{version}-tests.jar'.format(version=HADOOP_VERSION))
        self.yarn_server_jar = os.path.join(
            hadoop_path, 'share', 'hadoop', 'yarn', 'test',
            'hadoop-yarn-server-tests-{version}-tests.jar'.format(version=HADOOP_VERSION))

    def _get_hadoop(self):
        hadoop_path = os.path.join(CACHE_DIR, 'hadoop')
        hadoop_tar = os.path.join(
            hadoop_path,
            'hadoop-{version}.tar.gz'.format(version=HADOOP_VERSION))
        if not os.path.exists(hadoop_tar):
            os.makedirs(hadoop_path, exist_ok=True)
            try:
                urlretrieve(HADOOP_SOURCE, hadoop_tar)
            except HTTPError:
                print(f'Could not download {HADOOP_SOURCE}')
                raise
            with tarfile.open(hadoop_tar) as tf:
                tf.extractall(path=hadoop_path)
        return os.path.join(hadoop_path,
                            'hadoop-{version}'.format(version=HADOOP_VERSION))

    def start(self):
        cmd = [
            self.hadoop_bin, 'jar',
            self.hadoop_mapreduce_client, 'minicluster',
            '-nnport', str(NN_PORT), '-nomr', '-format',
            '-D', 'dfs.replication=1',
            '-D', 'dfs.client.use.datanode.hostname=true',
            '-D', 'dfs.datanode.use.datanode.hostname=true',
        ]
        JAVA_HOME = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk/')
        self.p = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(self.hadoop_bin),
            env={
                'JAVA_HOME': JAVA_HOME,
                'HADOOP_CLASSPATH': self.yarn_server_jar
            },
            stderr=subprocess.PIPE
        )
        if wait_for_minicluster(self.p.stderr):
            print('>>> Hadoop cluster is active')
        else:
            print('>>> Could not start Hadoop cluster in time')

    def stop(self):
        if self.p:
            self.p.terminate()
            self.p.communicate(timeout=10)
            self.p.stderr.close()
            self.p = None


class HdfsCrateLayer(CrateNode):
    def start(self):
        add_hadoop_libs(hdfs_repo_libs_path, crate_path())
        super().start()


crate = HdfsCrateLayer(
    crate_dir=crate_path(),
    settings={
        'psql.port': 0,
        'transport.tcp.port': 0
    },
    env={
        'CRATE_HEAP_SIZE': '256M',
        **os.environ.copy()
    },
    version=(4, 0, 0)
)
hadoop = HadoopLayer()


class HdfsIntegrationTest(unittest.TestCase):

    def setUp(self):
        hadoop.start()
        crate.start()

    def tearDown(self):
        hadoop.stop()
        crate.stop()

    def test_create_hdfs_repository(self):
        with connect(f'{crate.http_url}') as conn:
            c = conn.cursor()
            stmt = '''create repository "test-repo" type hdfs with (uri = ?, path = '/data')'''
            # okay if it doesn't raise a exception
            c.execute(stmt, ('hdfs://127.0.0.1:{nnport}'.format(nnport=NN_PORT),))
