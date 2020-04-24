import os
import asyncio
import asyncpg
import unittest
import ssl
from os.path import join
from testutils.paths import crate_path, project_root
from cr8.run_crate import CrateNode


env = os.environ.copy()
env['CRATE_HEAP_SIZE'] = '256M'
crate = CrateNode(
    crate_dir=crate_path(),
    settings={
        'ssl.psql.enabled': True,
        # keystore and certs were generated using the ./devs/tools/create_certs.py script
        'ssl.keystore_filepath': join(project_root, 'blackbox', 'certs', 'node1.jks'),
        'ssl.keystore_password': 'foobar',
        'ssl.keystore_key_password': 'foobar',
        'transport.tcp.port': 0,
        'node.name': 'crate-ssl-test',
        'discovery.type': 'single-node',
        'auth.host_based.enabled': True,
        'auth.host_based.config.0.user': 'crate',
        'auth.host_based.config.0.method': 'trust',
        'auth.host_based.config.1.user': 'client1',
        'auth.host_based.config.1.method': 'cert',
        'auth.host_based.config.1.ssl': 'on',
    },
    env=env,
    version=(4, 2, 0)
)


class SSLIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate.start()

    @classmethod
    def tearDownClass(cls):
        crate.stop()

    def test_pg_client_can_connect_with_client_certificate(self):
        asyncio.run(self._connect_with_client_cert())

    async def _connect_with_client_cert(self):
        conn = await asyncpg.connect(f"postgresql://crate@{crate.addresses.psql.host}:{crate.addresses.psql.port}/doc")
        await conn.execute('CREATE USER client1')
        await conn.close()

        sslcert = join(project_root, 'blackbox', 'certs', 'client1.crt')
        sslkey = join(project_root, 'blackbox', 'certs', 'client1.key')
        assert os.path.exists(sslcert), f'sslcert {sslcert} must exist'
        assert os.path.exists(sslkey), f'sslkey {sslkey} must exist'
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.load_cert_chain(sslcert, sslkey)
        conn_user = await asyncpg.connect(
            host=crate.addresses.psql.host,
            port=crate.addresses.psql.port,
            user='client1',
            database='doc',
            ssl=ssl_context
        )
        await conn_user.fetchval('SELECT 42')
        await conn_user.close()
