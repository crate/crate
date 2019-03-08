# -*- coding: utf-8; -*-
#
# Licensed to Crate.io Inc. (Crate) under one or more contributor license
# agreements.  See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.  Crate licenses this file to
# you under the Apache License, Version 2.0 (the "License");  you may not
# use this file except in compliance with the License.  You may obtain a
# copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, to use any modules in this file marked as "Enterprise Features",
# Crate must have given you permission to enable and use such Enterprise
# Features and you must have a valid Enterprise or Subscription Agreement
# with Crate.  If you enable or use the Enterprise Features, you represent
# and warrant that you have a valid Enterprise or Subscription Agreement
# with Crate.  Your use of the Enterprise Features if governed by the terms
# and conditions of your Enterprise or Subscription Agreement with Crate.

import os
import re
import unittest
import time
import logging
from crate.client import connect
from testutils.ports import bind_port
from testutils.paths import crate_path
from crate.testing.layer import CrateLayer
from subprocess import PIPE, Popen
from urllib.request import urlretrieve

JMX_PORT = bind_port()
CRATE_HTTP_PORT = bind_port()
JMX_PORT_ENTERPRISE_DISABLED = bind_port()

JMX_OPTS = '''
     -Dcom.sun.management.jmxremote
     -Dcom.sun.management.jmxremote.port={}
     -Dcom.sun.management.jmxremote.ssl=false
     -Dcom.sun.management.jmxremote.authenticate=false
'''

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


env = os.environ.copy()
env['CRATE_JAVA_OPTS'] = JMX_OPTS.format(JMX_PORT)
enterprise_crate = CrateLayer(
    'crate-enterprise',
    crate_home=crate_path(),
    port=CRATE_HTTP_PORT,
    transport_port=0,
    env=env,
    settings={
        'license.enterprise': True,
    }
)

env = os.environ.copy()
env["CRATE_JAVA_OPTS"] = JMX_OPTS.format(JMX_PORT_ENTERPRISE_DISABLED)
community_crate = CrateLayer(
    'crate',
    crate_home=crate_path(),
    port=bind_port(),
    transport_port=0,
    env=env,
    settings={
        'license.enterprise': False,
    }
)


class JmxClient:

    SJK_JAR_URL = "https://repository.sonatype.org/service/local/artifact/maven/redirect?r=central-proxy&g=org.gridkit.jvmtool&a=sjk&v=LATEST"

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
        return (stdout[len(bean) + 1:], stderr)


class JmxIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        enterprise_crate.start()

    @classmethod
    def tearDownClass(cls):
        enterprise_crate.stop()

    def test_mbean_select_total_count(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(f'localhost:{CRATE_HTTP_PORT}') as conn:
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

    def test_mbean_cluster_state_version(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo', 'ClusterStateVersion')
        self.assertGreater(int(stdout), 0)
        self.assertEqual(stderr, '')

    def test_number_of_open_connections(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=Connections', 'HttpOpen')
        self.assertGreater(int(stdout), 0)
        self.assertEqual(stderr, '')

    def test_search_pool(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=ThreadPools', 'Search')
        self.assertEqual(
            '\n'.join((line.strip() for line in stdout.split('\n'))),
            '''\
active:          0
completed:       4
largestPoolSize: 4
name:            search
poolSize:        4
queueSize:       0
rejected:        0

''')
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


class NoEnterpriseJmxIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        community_crate.start()

    @classmethod
    def tearDownClass(cls):
        community_crate.stop()

    def test_enterprise_setting_disabled(self):
        jmx_client = JmxClient(JMX_PORT_ENTERPRISE_DISABLED)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=QueryStats',
            'SelectQueryTotalCount'
        )

        self.assertEqual(
            stderr,
            'MBean not found: io.crate.monitoring:type=QueryStats\n')
        self.assertEqual(stdout, '')

