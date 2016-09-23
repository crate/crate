# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
import socket
import subprocess
import tarfile
import time
import unittest
import zipfile
from crate.client import connect
from crate.testing.layer import CrateLayer
from testutils.paths import crate_path, project_root
from testutils.ports import GLOBAL_PORT_POOL
from urllib.request import urlretrieve

HADOOP_VERSION = '2.7.1'
HADOOP_SOURCE = ('http://www-eu.apache.org/dist/hadoop/common/'
                 'hadoop-{version}/hadoop-{version}.tar.gz'.format(version=HADOOP_VERSION))
CACHE_DIR = os.environ.get(
    'XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache', 'crate-tests'))

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()
NN_PORT = '49000'

hdfs_repo_zip = os.path.join(
    project_root,
    'es-repository-hdfs',
    'build',
    'distributions',
    'es-repository-hdfs-hadoop2.zip')


def add_hadoop_libs(hdfs_zip_path, path_to_dist):
    hdfs_plugin_location = os.path.join(
        path_to_dist, 'plugins', 'elasticsearch-repository-hdfs')
    with zipfile.ZipFile(hdfs_zip_path) as hdfs_zip:
        hadoop_libs = [i for i in hdfs_zip.namelist()
                       if i.startswith('hadoop-libs')]
        hdfs_zip.extractall(path=hdfs_plugin_location, members=hadoop_libs)


def is_up(host, port):
    """test if a host is up"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ex = s.connect_ex((host, port))
    if ex == 0:
        s.close()
        return True
    return False


class HadoopLayer(object):
    __name__ = 'hadoop'
    __bases__ = ()

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
            urlretrieve(HADOOP_SOURCE, hadoop_tar)
            with tarfile.open(hadoop_tar) as tf:
                tf.extractall(path=hadoop_path)
        return os.path.join(hadoop_path,
                            'hadoop-{version}'.format(version=HADOOP_VERSION))

    def setUp(self):
        cmd = ['bash',
               self.hadoop_bin, 'jar',
               self.hadoop_mapreduce_client, 'minicluster',
               '-nnport', NN_PORT, '-nomr', '-D', 'dfs.replication=0', 'dfs.client.use.datanode.hostname=true']

        JAVA_HOME = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-8-openjdk/')
        self.p = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(self.hadoop_bin),
            env={
                'JAVA_HOME': JAVA_HOME,
                'HADOOP_CLASSPATH': self.yarn_server_jar
            })
        elapsed = 0.0
        while not is_up('localhost', int(NN_PORT)) and elapsed < 5:
            time.sleep(0.1)
            elapsed += 0.1

    def tearDown(self):
        self.p.kill()


class HdfsCrateLayer(CrateLayer):
    def setUp(self):
        add_hadoop_libs(hdfs_repo_zip, crate_path())
        super(HdfsCrateLayer, self).setUp()


class HadoopAndCrateLayer(object):
    def __init__(self, crate_layer, hadoop_layer):
        self.__bases__ = (crate_layer, hadoop_layer)
        self.__name__ = 'hadoop_and_crate'

    def setUp(self):
        pass

    def tearDown(self):
        pass


class HdfsIntegrationTest(unittest.TestCase):
    def test_create_hdfs_repository(self):
        conn = connect('localhost:{}'.format(CRATE_HTTP_PORT))
        c = conn.cursor()
        stmt = '''create repository "test-repo" type hdfs with (uri = ?, path = '/data')'''
        # okay if it doesn't raise a exception
        c.execute(stmt, ('hdfs://127.0.0.1:{nnport}'.format(nnport=NN_PORT),))


def test_suite():
    suite = unittest.TestSuite(unittest.makeSuite(HdfsIntegrationTest))
    crate_layer = HdfsCrateLayer(
        'crate',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT
    )
    hadoop_layer = HadoopLayer()
    suite.layer = HadoopAndCrateLayer(crate_layer, hadoop_layer)
    return suite
