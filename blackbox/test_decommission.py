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

import unittest
import os
import time
import random
import string
import threading
from cr8.run_crate import CrateNode
from crate.client.http import Client
from crate.client.exceptions import ProgrammingError, ConnectionError
from testutils.paths import crate_path
from testutils.ports import bind_range


def decommission(client, node):
    try:
        return client.sql('alter cluster decommission ?', (node,))
    except ConnectionError:
        pass


def retry_sql(client, statement):
    """ retry statement on node not found in cluster state errors

    sys.shards queries might fail if a node that has shutdown is still
    in the cluster state
    """
    wait_time = 0
    sleep_duration = 0.01
    last_error = None
    while wait_time < 10.5:
        try:
            return client.sql(statement)
        except ProgrammingError as e:
            if ('not found in cluster state' in e.message or
                    'Node not connected' in e.message):
                time.sleep(sleep_duration)
                last_error = e
                wait_time += sleep_duration
                sleep_duration *= 2
                continue
            raise e
    raise last_error


def wait_for_cluster_size(client, expected_size, timeout_in_s=20):
    """ retry check (up to timeout_in_s) for expected cluster size

    Returns True if the cluster size reaches the expected one, False otherwise
    Can raise:  Value Error -- for a negative value of expected_size
                TimeoutError -- after timeout_in_s

    A node can be decommissioned with a delay, this can be
    used to check when a node is indeed decommissioned
    """
    if expected_size < 0:
        raise ValueError('expected_size cannot be negative')

    wait_time = 0
    sleep_duration = 1
    num_nodes = -1
    while num_nodes != expected_size:
        time.sleep(sleep_duration)
        wait_time += sleep_duration
        response = client.sql("select * from sys.nodes")
        num_nodes = response.get("rowcount", -1)
        if num_nodes == -1:
            return False
        if wait_time > timeout_in_s:
            raise TimeoutError('Timeout occurred ({}s) while waiting for cluster size to become {}'
                               .format(timeout_in_s, expected_size))
    return True


class GracefulStopCrateLayer(CrateNode):

    MAX_RETRIES = 3

    def start(self, retry=0):
        if retry >= self.MAX_RETRIES:
            raise SystemError('Could not start Crate server. Max retries exceeded!')
        try:
            super().start()
        except Exception:
            self.start(retry=retry + 1)

    def stop(self):
        """do not care if process already died"""
        try:
            super().stop()
        except OSError:
            pass


class GracefulStopTest(unittest.TestCase):
    """
    abstract class for starting a cluster of crate instances
    and testing against them
    """
    DEFAULT_NUM_SERVERS = 1

    def __init__(self, *args, **kwargs):
        self.num_servers = kwargs.pop("num_servers", getattr(self, "NUM_SERVERS", self.DEFAULT_NUM_SERVERS))
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.crates = []
        self.clients = []
        self.node_names = []
        # auto-discovery with unicast on the same host only works if all nodes are configured with the same port range
        transport_port_range = bind_range(range_size=self.num_servers)
        for i in range(self.num_servers):
            layer = GracefulStopCrateLayer(
                crate_dir=crate_path(),
                settings={
                    'cluster.name': self.__class__.__name__,
                    'node.name': self.node_name(i),
                    'transport.tcp.port': transport_port_range,
                },
                env={
                    **os.environ.copy(),
                    'CRATE_HEAP_SIZE': '256M'
                },
                version=(4, 0, 0)
            )
            layer.start()
            self.clients.append(Client(layer.http_url))
            self.crates.append(layer)
            self.node_names.append(self.node_name(i))

        client = self.random_client()
        num_nodes = 0

        # wait until all nodes joined the cluster
        while num_nodes < len(self.crates):
            response = client.sql("select * from sys.nodes")
            num_nodes = response.get("rowcount", 0)
            time.sleep(.5)

    def tearDown(self):
        for client in self.clients:
            client.close()
        for layer in self.crates:
            layer.stop()

    def random_client(self):
        return random.choice(self.clients)

    def node_name(self, i):
        return "crate_{0}_{1}".format(self.__class__.__name__, i)

    def set_settings(self, settings):
        client = self.random_client()
        for key, value in settings.items():
            client.sql("set global transient {}=?".format(key), (value,))


class TestGracefulStopPrimaries(GracefulStopTest):

    NUM_SERVERS = 3

    def setUp(self):
        super().setUp()
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
        decommission(self.clients[0], self.node_names[0])
        self.assertEqual(wait_for_cluster_size(client2, TestGracefulStopPrimaries.NUM_SERVERS - 1), True)

        stmt = "select table_name, id from sys.shards where state = 'UNASSIGNED'"
        response = retry_sql(client2, stmt)
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)

    def tearDown(self):
        client = self.clients[1]
        client.sql("drop table t1")
        super().tearDown()


class TestGracefulStopFull(GracefulStopTest):

    NUM_SERVERS = 3

    def setUp(self):
        super().setUp()
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
        node1, node2, node3 = self.node_names
        client1, client2, client3 = self.clients
        self.set_settings({"cluster.graceful_stop.min_availability": "full"})
        decommission(client2, node1)
        self.assertEqual(wait_for_cluster_size(client2, TestGracefulStopFull.NUM_SERVERS - 1), True)

        stmt = "select table_name, id from sys.shards where state = 'UNASSIGNED'"
        response = retry_sql(client2, stmt)
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)

    def tearDown(self):
        client = self.clients[2]
        client.sql("drop table t1")
        super().tearDown()


class TestGracefulStopNone(GracefulStopTest):

    NUM_SERVERS = 3

    def setUp(self):
        super().setUp()
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
        decommission(self.clients[0], self.node_names[0])
        self.assertEqual(wait_for_cluster_size(client2, TestGracefulStopNone.NUM_SERVERS - 1), True)

        stmt = "select node['id'] as node_id, id, state \
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
        super().tearDown()


class TestGracefulStopDuringQueryExecution(GracefulStopTest):

    NUM_SERVERS = 3

    def setUp(self):
        super().setUp()
        client = self.clients[0]

        client.sql('''
            CREATE TABLE t1 (id int primary key, name string)
            CLUSTERED INTO 4 SHARDS
            WITH (number_of_replicas = 1)
        ''')

        def bulk_params():
            for i in range(5000):
                chars = list(string.ascii_lowercase[:14])
                random.shuffle(chars)
                yield (i, ''.join(chars))
        bulk_params = list(bulk_params())
        client.sql('INSERT INTO t1 (id, name) values (?, ?)', None, bulk_params)
        client.sql('REFRESH TABLE t1')

    def test_graceful_stop_concurrent_queries(self):
        self.set_settings({
            "cluster.graceful_stop.min_availability": "full",
            "cluster.graceful_stop.force": "false"
        })

        concurrency = 4
        client = self.clients[0]
        run_queries = [True]
        errors = []
        threads_finished_b = threading.Barrier((concurrency * 3) + 1)
        func_args = (client, run_queries, errors, threads_finished_b)
        for i in range(concurrency):
            t = threading.Thread(
                target=TestGracefulStopDuringQueryExecution.exec_select_queries,
                args=func_args
            )
            t.start()
            t = threading.Thread(
                target=TestGracefulStopDuringQueryExecution.exec_insert_queries,
                args=func_args
            )
            t.start()
            t = threading.Thread(
                target=TestGracefulStopDuringQueryExecution.exec_delete_queries,
                args=func_args
            )
            t.start()

        # Ensure we don't hold a connection open to the node getting stopped
        self.clients[1].close()
        decommission(self.clients[0], self.node_names[1])
        try:
            self.assertEqual(wait_for_cluster_size(self.clients[0], TestGracefulStopDuringQueryExecution.NUM_SERVERS - 1), True)
        except:
            # Need to stop threads, otherwise the test would get stuck
            run_queries[0] = False
            threads_finished_b.wait()
            raise

        run_queries[0] = False
        threads_finished_b.wait()
        self.assertEqual(errors, [])

    def tearDown(self):
        self.clients[0].sql('DROP TABLE t1')
        super().tearDown()

    @staticmethod
    def exec_insert_queries(client, is_active, errors, finished):
        while is_active[0]:
            try:
                chars = list(string.ascii_lowercase[:14])
                random.shuffle(chars)
                client.sql(
                    'insert into t1 (id, name) values ($1, $2) on conflict (id) do update set name = $2',
                    (random.randint(0, 2147483647), ''.join(chars))
                )
            except Exception as e:
                errors.append(e)
        finished.wait()

    @staticmethod
    def exec_delete_queries(client, is_active, errors, finished):
        while is_active[0]:
            try:
                chars = list(string.ascii_lowercase[:14])
                random.shuffle(chars)
                pattern = ''.join(chars[:3]) + '%'
                client.sql(
                    'delete from t1 where name like ?', (pattern,))
            except Exception as e:
                if 'RelationUnknown' not in str(e):
                    errors.append(e)
        finished.wait()

    @staticmethod
    def exec_select_queries(client, is_active, errors, finished):
        while is_active[0]:
            try:
                client.sql('select name, count(*) from t1 group by name')
            except Exception as e:
                errors.append(e)
        finished.wait()
