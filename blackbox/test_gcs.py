#!/usr/bin/env python3

import unittest
import subprocess
from crate.client import connect
from cr8.run_crate import CrateNode

FAKE_GCS_NAME = 'fake_gcs_server'
FAKE_GCS_PORT = '4443:4443'
FAKE_GCS_IMAGE = 'fsouza/fake-gcs-server'
FAKE_GCS_ENDPOINT = 'http://localhost:4443'

# The fake GCS server ignores credentials,
# but the client library needs something to parse.
SERVICE_ACCOUNT_KEY = '''
{
  "type": "service_account",
  "project_id": "project-id",
  "private_key_id": "0123456789abcdef0123456789abcdef01234567",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\\nMIICWgIBAAKBgF1eM8xX6Ye1sXLCGukLN7nhbBmntqOdBQB1jS2sFASK8k/3aONA\\nHcRH32SGPa8IDgiNGhBYpV7sCDv2WC5YJuqZ2yqf/faAqlbZQKD8v4ZVAU+0HCRX\\nsVaJTL1uIDFpYjykRTI+oKvXLLgm7YNY/pfdc8tNlvLSOD/+licO690TAgMBAAEC\\ngYAjRNZkNkEs1mF1bAUmSup9+L0Q492F6GZfSghOXFYjcSkfM+IXFb0oy82C94KN\\nf4ltiX9y9Ulild52aBPfTcVFGqq+Ja7CTjGn33rq3+XElbWae6TtxH4teYCdJ5tR\\nbAC2zeEubVP7sad1EBL02KsDAzOubYY1TZOgzhGEXI1WQQJBALk3FcFxnSvqk28v\\nQtzN+Qlr1I7ZCD+pjH3UfkapGRMEfLcl1kSsLWVkx0VFhbyjMjLQTVNGFTqpIBYn\\nGtQjvgkCQQCBDQ3f3M0uUpVINBa+RvaHqaofMBFdr/6+16/JBufy5+AfLDhgrXY8\\nzYnO1eUWAlgouo5jADfuvoC5DF7FKsk7AkBo5Cxa+DfVlixW7EIGgFOIJVwkZf8I\\n5sFcxYmaBMvhfEoRAsmc7JFs+azsqxm5IgccxeD0xxzgssUsJotIFtHZAkAqclTB\\n8So7tkYLvbZNV6H29UNThsfvfNfZha/3+yGHr+Tz2+OfSO6/CQvu0EPNfW/p1ZLH\\n1WaQTHCibQMu7ufJAkAV8unhcpAZecw4r4UvsocQsGzkQXpHlC465vfm0qPjGYSc\\ny/uHFT5RzU37DhiKjvQiDVqOMaTNEn/Bfty1A8eK\\n-----END RSA PRIVATE KEY-----\\n",
  "client_email": "service-account-id@project-id.iam.gserviceaccount.com",
  "client_id": "012345678901234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-id%40project-id.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
'''
BUCKET_NAME = 'crate_repository'
BASE_PATH = 'base/path'

crate_node = CrateNode(
    crate_dir=crate_path(),
    settings={
        'transport.tcp.port': 0,
        'psql.port': 0,
    },
    env={
        'CRATE_HEAP_SIZE': '256M'
    },
    version=(4, 0, 0)
)


class GCSRepositorySnapshotIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate_node.start()
        subprocess.run([
            'docker', 'run', '-d', '--name', FAKE_GCS_NAME,
            '-p', FAKE_GCS_PORT, FAKE_GCS_IMAGE], shell=True)

    @classmethod
    def tearDownClass(cls):
        crate_node.stop()
        subprocess.run(['docker', 'stop', FAKE_GCS_NAME])

    def test_snaphot(self):
        with connect(crate_node.http_url) as conn:
            c = conn.cursor()
            c.execute('CREATE TABLE t1 (x int)')
            c.execute('INSERT INTO t1 (x) VALUES (1), (2), (3)')
            c.execute('REFRESH TABLE t1')
            c.execute('CREATE REPOSITORY r1 TYPE gcs WITH (endpoint=?, service_account_key=?, bucket=?, base_path=?)',
                (FAKE_GCS_ENDPOINT, SERVICE_ACCOUNT_KEY, BUCKET_NAME, BASE_PATH))

            # Make sure service_account_key is masked in sys.repositories.
            c.execute("SELECT settings['service_account_key'] from sys.repositories where name = 'r1'")
            settings = c.fetchone()
            self.assertEqual(settings[0], '[xxxxx]')
