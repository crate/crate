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
from jnius import autoclass

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
crate_node = CrateNode(
    crate_dir=crate_path(),
    settings={
        'transport.tcp.port': 0,
        'psql.port': 0,
        'node.name': 'crate-jmx-test',
    },
    env=env
)


JMXServiceURL = autoclass("javax.management.remote.JMXServiceURL")
JMXConnectorFactory = autoclass("javax.management.remote.JMXConnectorFactory")
MBeanServerConnection = autoclass("javax.management.MBeanServerConnection")
ObjectName = autoclass("javax.management.ObjectName")


class JmxClient:

    def __init__(self, jmx_port):
        self.url = JMXServiceURL(f"service:jmx:rmi:///jndi/rmi://localhost:{jmx_port}/jmxrmi")
        connector = JMXConnectorFactory.connect(self.url, None)
        self.conn = connector.getMBeanServerConnection()

    def query_jmx(self, bean, attribute):
        objectName = ObjectName(bean)
        result = self.conn.getAttribute(objectName, attribute)
        return result


class JmxIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate_node.start()

    @classmethod
    def tearDownClass(cls):
        crate_node.stop()

    def test_mbean_select_total_count(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(crate_node.http_url) as conn:
            c = conn.cursor()
            c.execute("select 1")
            result = jmx_client.query_jmx(
                'io.crate.monitoring:type=QueryStats',
                'SelectQueryTotalCount'
            )
            self.assertGreater(int(result), 0)

    def test_mbean_select_ready(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeStatus',
            'Ready'
        )
        self.assertEqual(result, 1)

    def test_mbean_node_name(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo',
            'NodeName'
        )
        self.assertEqual(result.rstrip(), 'crate-jmx-test')

    def test_mbean_node_id(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo',
            'NodeId'
        )
        self.assertNotEqual(result.rstrip(), '', 'node id must not be empty')

    def test_mbean_shards(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(crate_node.http_url) as conn:
            c = conn.cursor()
            c.execute('''create table test(id integer) clustered into 1 shards with (number_of_replicas=0)''')
            result = jmx_client.query_jmx(
                'io.crate.monitoring:type=NodeInfo',
                'ShardStats'
            )
            self.assertEqual(result.contentString(), "{primaries=1, replicas=0, total=1, unassigned=0}")

            result = jmx_client.query_jmx(
                'io.crate.monitoring:type=NodeInfo',
                'ShardInfo'
            )
            self.assertEqual(1, len(result))
            shardInfo = result[0]
            self.assertTrue(shardInfo.containsKey("primary"))
            self.assertTrue(shardInfo.containsKey("partitionIdent"))
            self.assertTrue(shardInfo.containsKey("routingState"))
            self.assertTrue(shardInfo.containsKey("shardId"))
            self.assertTrue(shardInfo.containsKey("size"))
            self.assertTrue(shardInfo.containsKey("state"))
            self.assertTrue(shardInfo.containsKey("schema"))
            self.assertTrue(shardInfo.containsKey("table"))
            c.execute('''drop table test''')

    def test_mbean_cluster_state_version(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=NodeInfo', 'ClusterStateVersion')
        self.assertGreaterEqual(int(result), 0)

    def test_number_of_open_connections(self):
        jmx_client = JmxClient(JMX_PORT)
        with connect(crate_node.http_url) as _:
            result = jmx_client.query_jmx(
                'io.crate.monitoring:type=Connections', 'HttpOpen')
            self.assertGreater(int(result), 0)

    def test_search_pool(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=ThreadPools', 'Search')
        expected = "{active=0, completed=0, largestPoolSize=0, name=search, poolSize=0, queueSize=0, rejected=0}"
        self.assertEqual(expected, result.contentString())

    def test_parent_breaker(self):
        jmx_client = JmxClient(JMX_PORT)
        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=CircuitBreakers', 'Parent')
        self.assertGreater(result.get("limit"), 0)
        self.assertGreater(result.get("used"), 0)
        self.assertEqual("parent", result.get("name"))
        self.assertEqual(0, result.get("trippedCount"))
        self.assertEqual(1.0, result.get("overhead"))

        result = jmx_client.query_jmx(
            'io.crate.monitoring:type=CircuitBreakers', 'Query')
        self.assertGreater(result.get("limit"), 0)
        self.assertEqual(result.get("used"), 0)
        self.assertEqual("query", result.get("name"))
        self.assertEqual(0, result.get("trippedCount"))
        self.assertEqual(1.0, result.get("overhead"))
