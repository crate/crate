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
import time
import logging
from crate.client import connect
from crate.testing.layer import CrateLayer

from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


class SigarIntegrationTest(unittest.TestCase):

    def test_multiple_probe_selects(self):
        conn = connect('localhost:{}'.format(CRATE_HTTP_PORT))
        cur = conn.cursor()
        cur.execute("SELECT os, load, os FROM sys.nodes")
        res = cur.fetchall()
        for row in res:
            self.assertEqual(row[0], row[2])

    def test_sigar_gets_loaded(self):
        conn = connect('localhost:{}'.format(CRATE_HTTP_PORT))
        cur = conn.cursor()
        to_sleep = 0.2
        while True:
            cur.execute("select process['cpu']['percent'] from sys.nodes")
            percent = int(cur.fetchone()[0])
            if percent > -1:
                break
            if to_sleep > 30:
                raise AssertionError(
                    'cpu percent was -1 for too long. Sigar probably missing')
            time.sleep(to_sleep)
            to_sleep *= 2


def test_suite():
    suite = unittest.TestSuite(unittest.makeSuite(SigarIntegrationTest))
    suite.layer = CrateLayer(
        'crate',
        host='localhost',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT
    )
    return suite
