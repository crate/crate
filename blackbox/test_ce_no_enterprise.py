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
import unittest
from testutils.ports import bind_port
from testutils.paths import crate_path
from cr8.run_crate import CrateNode
from test_jmx import JmxClient

JMX_PORT = bind_port()
JMX_OPTS = '''
     -Dcom.sun.management.jmxremote
     -Dcom.sun.management.jmxremote.port={}
     -Dcom.sun.management.jmxremote.ssl=false
     -Dcom.sun.management.jmxremote.authenticate=false
'''


env = os.environ.copy()
env['CRATE_JAVA_OPTS'] = JMX_OPTS.format(JMX_PORT)
crate_layer = CrateNode(
    crate_dir=crate_path(),
    version=(4, 0, 0),
    settings={
        'transport.tcp.port': 0,
        'node.name': 'crate-ce',
    },
    env=env
)


class CeHasNoEnterpriseModulesITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        crate_layer.start()

    @classmethod
    def tearDownClass(cls):
        crate_layer.stop()

    def test_enterprise_jmx_not_available(self):
        jmx_client = JmxClient(JMX_PORT)
        stdout, stderr = jmx_client.query_jmx(
            'io.crate.monitoring:type=QueryStats',
            'SelectQueryTotalCount'
        )

        self.assertEqual(
            stderr,
            'MBean not found: io.crate.monitoring:type=QueryStats\n')
        self.assertEqual(stdout, '')
