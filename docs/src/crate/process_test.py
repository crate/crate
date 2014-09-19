import unittest
import os
import signal
import time
import random
from crate.testing.layer import CrateLayer
from crate.client.http import Client
from .paths import crate_path
from lovely.testlayers.layer import CascadedLayer


def public_ip():
    import socket
    return socket.gethostbyname(socket.gethostname())


class GracefulStopCrateLayer(CrateLayer):

    def __init__(self,
                 name,
                 crate_home,
                 crate_config=None,
                 port=4200,
                 keep_running=False,
                 transport_port=None,
                 crate_exec=None,
                 cluster_name=None,
                 host='localhost'):
        """
        TODO: replace with new CrateLayer when new crate-python release is out

        :param name: layer name, is also used as the cluser name
        :param crate_home: path to home directory of the crate installation
        :param port: port on which crate should run
        :param keep_running: do not shut down the crate instance for every
                            single test instead just delete all indices
        :param transport_port: port on which transport layer for crate should run
        :param crate_exec: alternative executable command
        :param crate_config: alternative crate config file location
        :param cluster_name: the name of the cluster this node should be part of
        """
        self.keep_running = keep_running
        crate_home = os.path.abspath(crate_home)
        servers = ['%s:%s' % (host, port)]
        self.crate_servers = ['http://%s:%s' % (host, port)]
        if crate_exec is None:
            crate_exec = os.path.join(crate_home, 'bin', 'crate')
        if crate_config is None:
            crate_config = os.path.join(crate_home, 'config', 'crate.yml')
        if cluster_name is None:
            cluster_name = 'Testing{0}'.format(port)
        start_cmd = (
            crate_exec,
            '-Des.index.storage.type=memory',
            '-Des.node.name=%s' % name,
            '-Des.cluster.name=%s' % cluster_name,
            '-Des.http.port=%s-%s' % (port, port),
            '-Des.network.host=%s' % host,
            '-Des.discovery.type=zen',
            '-Des.discovery.zen.ping.multicast.enabled=true',
            '-Des.config=%s' % crate_config,
            '-Des.path.conf=%s' % os.path.dirname(crate_config),
        )
        if transport_port:
            start_cmd += ('-Des.transport.tcp.port=%s' % transport_port,)
        super(CrateLayer, self).__init__(name, servers=servers, start_cmd=start_cmd)
        self.setUpWD()

    def stop(self):
        """do not care if process already died"""
        try:
            super(GracefulStopCrateLayer, self).stop()
        except OSError as e:
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
            layer = GracefulStopCrateLayer(self.node_name(i),
                           crate_path(),
                           host=public_ip(),
                           port=44200+i,
                           transport_port=44300+i,
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

    def settings(self, settings):
        client = self.random_client()
        for key, value in settings.iteritems():
            client.sql("set global transient {}=?".format(key), (value,))

    def wait_for_deallocation(self, nodename, client):
        node_still_up = True
        while node_still_up:
            time.sleep(1)
            response = client.sql("select name from sys.nodes where name=?", (nodename,))
            node_still_up = response.get('rowcount', -1) == 1


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

        os.kill(process.pid, signal.SIGUSR2)

        exit_value = process.wait()
        self.assertTrue(
            exit_value == 0,
            "crate stopped with return value {0}. expected: 0".format(exit_value)
        )


class TestGracefulStopPrimaries(GracefulStopTest):

    NUM_SERVERS = 2

    def test_graceful_stop_primaries(self):
        """
        test min_availability: primaries
        """

        client1 = self.clients[0]
        client2 = self.clients[1]

        client1.sql("create table t1 (id int, name string) "
                    "clustered into 4 shards "
                    "with (number_of_replicas=0)")
        client1.sql("insert into t1 (id, name) values (?, ?), (?, ?)",
                    (1, "Ford", 2, "Trillian"))
        client1.sql("refresh table t1")
        self.settings({
            "cluster.graceful_stop.min_availability": "primaries",
            "cluster.routing.allocation.enable": "new_primaries"
        })
        os.kill(self.crates[0].process.pid, signal.SIGUSR2)
        self.wait_for_deallocation(self.node_names[0], client2)
        response = client2.sql("select table_name, id from sys.shards "
                               "where state='UNASSIGNED'")
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)


class TestGracefulStopFull(GracefulStopTest):

    NUM_SERVERS = 3

    def test_graceful_stop_full(self):
        """
        min_availability: full moves all shards
        """
        crate1, crate2, crate3 = self.crates[0], self.crates[1], self.crates[2]
        client1, client2, client3 = self.clients[0], self.clients[1], self.clients[2]

        client1.sql("create table t1 (id int, name string) "
                    "clustered into 4 shards "
                    "with (number_of_replicas=1)")
        client1.sql("insert into t1 (id, name) values (?, ?), (?, ?)",
                    (1, "Ford", 2, "Trillian"))
        client1.sql("refresh table t1")
        self.settings({
            "cluster.graceful_stop.min_availability": "full",
            "cluster.routing.allocation.enable": "new_primaries"
        })
        os.kill(crate1.process.pid, signal.SIGUSR2)
        self.wait_for_deallocation(self.node_names[0], client2)
        response = client2.sql("select table_name, id from sys.shards "
                               "where state='UNASSIGNED'")
        # assert that all shards are assigned
        self.assertEqual(response.get("rowcount", -1), 0)


class TestGracefulStopNone(GracefulStopTest):

    NUM_SERVERS = 2

    def test_graceful_stop_none(self):
        """
        test min_availability: none
        """

        client1 = self.clients[0]
        client2 = self.clients[1]

        client1.sql("create table t1 (id int, name string) "
                    "clustered into 8 shards "
                    "with (number_of_replicas=0)")
        client1.sql("refresh table t1")
        names = ("Ford", "Trillian", "Zaphod", "Jeltz")
        for i in range(16):
            client1.sql("insert into t1 (id, name) "
                        "values (?, ?)",
                        (i, random.choice(names)))
        client1.sql("refresh table t1")
        self.settings({
            "cluster.graceful_stop.min_availability": "none",
            "cluster.routing.allocation.enable": "none"
        })

        os.kill(self.crates[0].process.pid, signal.SIGUSR2)
        self.wait_for_deallocation(self.node_names[0], client2)
        response = client2.sql("select sys.nodes.id as node_id, id, state from sys.shards "
                               "where state='UNASSIGNED'")

        # assert that some shards are not assigned
        unassigned_shards = response.get("rowcount", -1)
        self.assertTrue(
            unassigned_shards > 0,
            "{0} unassigned shards, expected more than 0".format(unassigned_shards)
        )


