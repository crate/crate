# -*- coding: utf-8; -*-
#
# Licensed to Crate under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.  Crate licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial
# agreement.

import os
import time
import random
import unittest
from cr8.run_crate import CrateNode
from testutils.paths import crate_path
from testutils.ports import bind_range
from crate.client import connect

CRATE_CE = os.environ.get('CRATE_CE') == "1"

TRIAL_MAX_NODES = 3


class CommunityLicenseITest(unittest.TestCase):

    # The number must be higher than the `max_nodes` of an Enterprise Trial License
    NUM_SERVERS = TRIAL_MAX_NODES + 1

    CRATES = []
    HTTP_PORTS = []

    @classmethod
    def setUpClass(cls):
        # auto-discovery with unicast on the same host only works if all nodes are configured with the same port range
        transport_port_range = bind_range(range_size=cls.NUM_SERVERS)
        for i in range(cls.NUM_SERVERS):
            layer = CrateNode(
                crate_dir=crate_path(),
                version=(4, 0, 0),
                settings={
                    'cluster.name': cls.__class__.__name__,
                    'node.name': cls.node_name(i),
                    'psql.port': 0,
                    'transport.tcp.port': transport_port_range,
                },
                env={
                    'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
                    'CRATE_HEAP_SIZE': '256M'
                }
            )
            layer.start()
            cls.HTTP_PORTS.append(layer.addresses.http.port)
            cls.CRATES.append(layer)

        dsn = cls.random_dns()
        num_nodes = 0

        # wait until all nodes joined the cluster
        with connect(dsn) as conn:
            c = conn.cursor()
            while num_nodes < len(cls.CRATES):
                c.execute("select * from sys.nodes")
                num_nodes = len(c.fetchall())
                time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        for layer in cls.CRATES:
            layer.stop()

    @classmethod
    def node_name(cls, i):
        return "crate_{0}_{1}".format(cls.__class__.__name__, i)

    @classmethod
    def random_dns(cls):
        return "localhost:" + str(random.choice(cls.HTTP_PORTS))

    def test_community_edition_has_no_license(self):
        if not CRATE_CE:
            return

        with connect(self.random_dns()) as conn:
            c = conn.cursor()
            c.execute("select license, license['issued_to'] from sys.cluster")
            self.assertEqual([[None, None]], c.fetchall())

    def test_community_edition_has_no_max_nodes_limit(self):
        if not CRATE_CE:
            return

        with connect(self.random_dns()) as conn:
            c = conn.cursor()
            c.execute("create table t1 (id int)")
            self.assertEqual(1, c.rowcount)

