
import unittest
import os
import zipfile
import subprocess
import socket
import time
from crate.testing.layer import CrateLayer
from crate.client import connect


CRATE_HTTP_PORT = '42220'
CRATE_TRANSPORT_PORT = '42330'
NN_PORT = '49000'


here = os.path.dirname(__file__)  # blackbox/hdfs/src
project_root = os.path.abspath(os.path.join(here, '..', '..', '..'))
hdfs_repo_zip = os.path.join(
    project_root,
    'es-repository-hdfs',
    'build',
    'distributions',
    'es-repository-hdfs-hadoop2.zip')
crate_folder = os.path.join(here, '..', '..', 'tmp', 'crate')  # blackbox/tmp/crate


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

    def __init__(self, hadoop_path):
        self.hadoop_path = hadoop_path
        self.hadoop_bin = os.path.join(hadoop_path, 'bin', 'hadoop')
        self.hadoop_mapreduce_client = os.path.join(
            hadoop_path, 'share', 'hadoop', 'mapreduce', 'hadoop-mapreduce-client-jobclient-2.7.1-tests.jar')
        self.yarn_server_jar = os.path.join(
            hadoop_path, 'share', 'hadoop', 'yarn', 'test', 'hadoop-yarn-server-tests-2.7.1-tests.jar')

    def setUp(self):
        cmd = ['bash',
               self.hadoop_bin, 'jar',
               self.hadoop_mapreduce_client, 'minicluster', '-nnport', NN_PORT]

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
        add_hadoop_libs(hdfs_repo_zip, crate_folder)
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
        conn = connect('localhost:' + CRATE_HTTP_PORT)
        c = conn.cursor()
        stmt = '''create repository "test-repo" type hdfs with (uri = ?, path = '/data')'''
        # okay if it doesn't raise a exception
        c.execute(stmt, ('hdfs://127.0.0.1:{nnport}'.format(nnport=NN_PORT),))


def test_suite():
    suite = unittest.TestSuite(unittest.makeSuite(HdfsIntegrationTest))
    crate_layer = HdfsCrateLayer(
        'crate',
        crate_home=crate_folder,
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT
    )
    hadoop_layer = HadoopLayer(
        hadoop_path=os.path.join(project_root, 'blackbox', 'parts', 'hadoop'))
    suite.layer = HadoopAndCrateLayer(crate_layer, hadoop_layer)
    return suite
