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

import os
import re
import unittest
from crate.client import connect
from testutils.ports import bind_port
from testutils.paths import crate_path
from cr8.run_crate import CrateNode
from subprocess import PIPE, Popen
from urllib.request import urlretrieve

JMX_PORT = bind_port()
JMX_OPTS = '''
     -Dcom.sun.management.jmxremote
     -Dcom.sun.management.jmxremote.port={}
     -Dcom.sun.management.jmxremote.ssl=false
     -Dcom.sun.management.jmxremote.authenticate=false
     -Dio.netty.leakDetection.level=paranoid
'''


env = os.environ.copy()
env['CRATE_JAVA_OPTS'] = JMX_OPTS.format(JMX_PORT)
env['CRATE_HEAP_SIZE'] = '256M'
enterprise_crate = CrateNode(
    crate_dir=crate_path(),
    settings={
        'transport.tcp.port': 0,
        'psql.port': 0,
        'node.name': 'crate-enterprise',
    },
    env=env,
    version=(4, 0, 0)
)


class JmxClient:

    SJK_JAR_URL = "https://repo1.maven.org/maven2/org/gridkit/jvmtool/sjk/0.21/sjk-0.21.jar"

    CACHE_DIR = os.environ.get(
        'XDG_CACHE_HOME',
        os.path.join(os.path.expanduser('~'), '.cache', 'crate-tests')
    )

    def __init__(self, jmx_port):
        self.jmx_port = jmx_port
        self.jmx_path = self._get_jmx()

    def _get_jmx(self):
        jar_name = 'sjk.jar'
        jmx_path = os.path.join(JmxClient.CACHE_DIR, 'jmx')
        jar_path = os.path.join(jmx_path, jar_name)
        if not os.path.exists(jar_path):
            os.makedirs(jmx_path, exist_ok=True)
            urlretrieve(JmxClient.SJK_JAR_URL, jar_path)
        return jar_path

    def query_jmx(self, bean, attribute):
        env = os.environ.copy()
        env.setdefault('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk')
        with Popen(
            [
                'java',
                '--add-exports', 'java.rmi/sun.rmi.server=ALL-UNNAMED',
                '--add-exports', 'java.rmi/sun.rmi.transport=ALL-UNNAMED',
                '--add-exports', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
                '-jar', self.jmx_path,
                'mx',
                '-s', f'localhost:{self.jmx_port}',
                '-mg',
                '-b', bean,
                '-f', attribute
            ],
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            env=env,
            universal_newlines=True
        ) as p:
            stdout, stderr = p.communicate()
        restart_msg = 'Restarting java with unlocked package access\n'
        if stderr.startswith(restart_msg):
            stderr = stderr[len(restart_msg):]
        # Bean name is printed in the first line. Remove it
        stdout = stdout[len(bean) + 1:]
        stdout = stdout.replace(attribute, '').strip()
        return (stdout, stderr)


class JmxIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        enterprise_crate.start()

    @classmethod
    def tearDownClass(cls):
        enterprise_crate.stop()

    def test_mbean_select_total_count(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(enterprise_crate.http_url) as conn:
            c = conn.cursor()
            c.execute("select 1")
            stdout, stderr = jmx_client.query_jmx(
                'io.crate.monitoring:type=QueryStats',
                'SelectQueryTotalCount'
            )
            self.assertEqual(stderr, '')
            self.assertGreater(int(stdout), 0)

    def test_mbean_select_ready(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeStatus',
            'Ready'
        )
        self.assertEqual(stderr, '')
        self.assertEqual(stdout.rstrip(), 'true')

    def test_mbean_node_name(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo',
            'NodeName'
        )
        self.assertEqual(stderr, '')
        self.assertEqual(stdout.rstrip(), 'crate-enterprise')

    def test_mbean_node_id(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo',
            'NodeId'
        )
        self.assertEqual(stderr, '')
        self.assertNotEqual(stdout.rstrip(), '', 'node id must not be empty')

    def test_mbean_shards(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(enterprise_crate.http_url) as conn:
            c = conn.cursor()
            c.execute('''create table test(id integer) clustered into 1 shards with (number_of_replicas=0)''')
            stdout, stderr = jmx_client.query_jmx(
                'io.crate.monitoring:type=NodeInfo',
                'ShardStats'
            )
            result = [line.strip() for line in stdout.split('\n') if line.strip()]
            result.sort()
            self.assertEqual(result[0], 'primaries:  1')
            self.assertEqual(result[1], 'replicas:   0')
            self.assertEqual(result[2], 'total:      1')
            self.assertEqual(result[3], 'unassigned: 0')
            self.assertEqual(stderr, '')

            stdout, stderr = jmx_client.query_jmx(
                'io.crate.monitoring:type=NodeInfo',
                'ShardInfo'
            )
            self.assertNotEqual(stdout.rstrip(), '', 'ShardInfo must not be empty')
            self.assertIn("partitionIdent", stdout)
            self.assertIn("routingState", stdout)
            self.assertIn("shardId", stdout)
            self.assertIn("size", stdout)
            self.assertIn("state", stdout)
            self.assertIn("schema", stdout)
            self.assertIn("table", stdout)
            self.assertEqual(stderr, '')
            c.execute('''drop table test''')

    def test_mbean_cluster_state_version(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo', 'ClusterStateVersion')
        self.assertGreater(int(stdout), 0)
        self.assertEqual(stderr, '')

    def test_number_of_open_connections(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(enterprise_crate.http_url) as _:
            stdout, stderr = jmx_client.query_jmx(
                'io.crate.monitoring:type=Connections', 'HttpOpen')
            self.assertGreater(int(stdout), 0)
            self.assertEqual(stderr, '')

    def test_search_pool(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=ThreadPools', 'Search')
        lines = [line.strip() for line in stdout.split('\n')]
        expected = [
            'active:          0',
            'completed:       1',
            'largestPoolSize: 1',
            'name:            search',
            'poolSize:        1',
            'queueSize:       0',
            'rejected:        0',
        ]
        self.assertSequenceEqual(expected, lines)
        self.assertEqual(stderr, '')

    def test_parent_breaker(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=CircuitBreakers', 'Parent')
        self.assert_valid_circuit_breaker_jmx_output('parent', stdout)
        self.assertEqual(stderr, '')

        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=CircuitBreakers', 'Query')
        self.assert_valid_circuit_breaker_jmx_output('query', stdout)
        self.assertEqual(stderr, '')

    def assert_valid_circuit_breaker_jmx_output(self, cb_name, output):
        limit = re.search(r'limit:\s+([0-9]+)', output)
        self.assertGreater(int(limit.group(1)), 0)

        self.assertRegex(output, rf'name:\s+{cb_name}')
        self.assertRegex(output, r'overhead:\s+(\d+\.?\d+)')
        self.assertRegex(output, r'trippedCount:\s+(\d+)')
        self.assertRegex(output, r'used:\s+(\d+)')
