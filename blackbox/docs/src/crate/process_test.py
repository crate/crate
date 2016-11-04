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

import unittest
import os
import signal
import time
import random
from crate.testing.layer import CrateLayer
from crate.client.http import Client
from crate.client.exceptions import ProgrammingError
from testutils.paths import crate_path
from testutils.ports import GLOBAL_PORT_POOL


def decommission(process):
    os.kill(process.pid, signal.SIGUSR2)
    return process.wait(timeout=60)


def retry_sql(client, statement):
    """ retry statement on node not found in cluster state errros

    sys.shards queries might fail if a node that has shutdown is still
    in the cluster state
    """
    retry = 0
    last_error = None
    while retry < 50:
        try:
            return client.sql(statement)
        except ProgrammingError as e:
            if 'not found in cluster state' in e.message:
                time.sleep(0.1)
                last_error = e
                retry += 1
                continue
            raise e
    raise last_error


class CascadedLayer(object):

    def __init__(self, name, *bases):
        self.__name__ = name
        self.__bases__ = tuple(bases)

    def setUp(self):
        pass

    def teardown(self):
        pass


class GracefulStopCrateLayer(CrateLayer):

    MAX_RETRIES = 3

    def start(self, retry=0):
        if retry >= self.MAX_RETRIES:
            raise SystemError('Could not start Crate server. Max retries exceeded!')
        try:
            super(GracefulStopCrateLayer, self).start()
        except Exception:
            self.start(retry=retry + 1)

    def stop(self):
        """do not care if process already died"""
        try:
            super(GracefulStopCrateLayer, self).stop()
        except OSError:
            pass


class GracefulStopTest(unittest.TestCase):
    """
    abstract class for starting a cluster of crate instances
    and testing against them
    """
    DEFAULT_NUM_SERVERS = 1

    def __init__(self, *args, **kwargs):
        num_servers = kwargs.pop("num_servers", getattr(self, "NUM_SERVERS", self.DEFAULT_NUM_SERVERS))
        super(GracefulStopTest, self).__init__(*args, **kwargs)
        self.crates = []
        self.clients = []
        self.node_names = []
        for i in range(num_servers):
            layer = GracefulStopCrateLayer(
                self.node_name(i),
                crate_path(),
                host='localhost',
                port=GLOBAL_PORT_POOL.get(),
                transport_port=GLOBAL_PORT_POOL.get(),
                cluster_name=self.__class__.__name__)
            client = Client(layer.crate_servers)
            self.crates.append(layer)
            self.clients.append(client)
            self.node_names.append(self.node_name(i))
        self.layer = CascadedLayer(
            "{0}_{1}_crates".format(self.__class__.__name__, num_servers),
            *self.crates
        )

    def setUp(self):
        client = self.random_client()
        num_nodes = 0

        # wait until all nodes joined the cluster
        while num_nodes < len(self.crates):
            response = client.sql("select * from sys.nodes")
            num_nodes = response.get("rowcount", 0)
            time.sleep(.5)

    def random_client(self):
        return random.choice(self.clients)

    def node_name(self, i):
        return "crate_{0}_{1}".format(self.__class__.__name__, i)

    def set_settings(self, settings):
        client = self.random_client()
        for key, value in settings.items():
            client.sql("set global transient {}=?".format(key), (value,))


class TestProcessSignal(GracefulStopTest):
    """
    test signal handling of the crate process
    """
    NUM_SERVERS = 1

    def test_signal_usr2(self):
        """
        crate stops when receiving the USR2 signal - single node
        """
        crate = self.crates[0]
        process = crate.process

        exit_value = decommission(process)
        self.assertTrue(
            exit_value == 0,
            "crate stopped with return value {0}. expected: 0".format(exit_value)
        )


class TestGracefulStopPrimaries(GracefulStopTest):

    NUM_SERVERS = 2

    def setUp(self):
        super(TestGracefulStopPrimaries, self).setUp()
        client = self.clients[0]
        client.sql("create table t1 (id int, name string) "
                   "clustered into 4 shards "
                   "with (number_of_replicas=0)")
        client.sql("insert into t1 (id, name) values (?, ?), (?, ?)",
                   (1, "Ford", 2, "Trillian"))
        client.sql("refresh table t1")

    def test_graceful_stop_primaries(self):
        """
        test min_availability: primaries
        """
        client2 = self.clients[1]
        self.set_settings({"cluster.graceful_stop.min_availability": "primaries"})
        decommission(self.crates[0].process)
        stmt = "select table_name, id from sys.shards where state = 'UNASSIGNED'"
        response = retry_sql(client2, stmt)
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)

    def tearDown(self):
        client = self.clients[1]
        client.sql("drop table t1")


class TestGracefulStopFull(GracefulStopTest):

    NUM_SERVERS = 3

    def setUp(self):
        super(TestGracefulStopFull, self).setUp()
        client = self.clients[0]
        client.sql("create table t1 (id int, name string) "
                   "clustered into 4 shards "
                   "with (number_of_replicas=1)")
        client.sql("insert into t1 (id, name) values (?, ?), (?, ?)",
                   (1, "Ford", 2, "Trillian"))
        client.sql("refresh table t1")

    def test_graceful_stop_full(self):
        """
        min_availability: full moves all shards
        """
        crate1, crate2, crate3 = self.crates
        client1, client2, client3 = self.clients
        self.set_settings({"cluster.graceful_stop.min_availability": "full"})
        decommission(crate1.process)

        stmt = "select table_name, id from sys.shards where state = 'UNASSIGNED'"
        response = retry_sql(client2, stmt)
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)

    def tearDown(self):
        client = self.clients[2]
        client.sql("drop table t1")


class TestGracefulStopNone(GracefulStopTest):

    NUM_SERVERS = 2

    def setUp(self):
        super(TestGracefulStopNone, self).setUp()
        client = self.clients[0]

        client.sql("create table t1 (id int, name string) "
                   "clustered into 8 shards "
                   "with (number_of_replicas=0)")
        client.sql("refresh table t1")
        names = ("Ford", "Trillian", "Zaphod", "Jeltz")
        for i in range(16):
            client.sql("insert into t1 (id, name) "
                       "values (?, ?)",
                       (i, random.choice(names)))
        client.sql("refresh table t1")

    def test_graceful_stop_none(self):
        """
        test `min_availability: none` will stop the node immediately.
        Causes some shard to become unassigned (no replicas)
        """
        client2 = self.clients[1]

        self.set_settings({"cluster.graceful_stop.min_availability": "none"})
        decommission(self.crates[0].process)

        stmt = "select _node['id'] as node_id, id, state \
            from sys.shards where state='UNASSIGNED'"
        resp = retry_sql(client2, stmt)

        # since there were no replicas some shards must be missing
        unassigned_shards = resp.get("rowcount", -1)
        self.assertTrue(
            unassigned_shards > 0,
            "{0} unassigned shards, expected more than 0".format(unassigned_shards)
        )

    def tearDown(self):
        client = self.clients[1]
        client.sql("drop table t1")
