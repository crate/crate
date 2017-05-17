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
import time
import logging
from crate.client import connect
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path
from crate.testing.layer import CrateLayer
from subprocess import PIPE, Popen
from urllib.request import urlretrieve

JMX_OPTS = '''
     -Dcom.sun.management.jmxremote
     -Dcom.sun.management.jmxremote.port={}
     -Dcom.sun.management.jmxremote.ssl=false
     -Dcom.sun.management.jmxremote.authenticate=false
'''

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


class JmxTermClient(object):

    JAVA_HOME = os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-8-openjdk/')
    JMX_TERM_VERSION = '1.0-alpha-4'
    JMX_TERM_SOURCE = "https://sourceforge.net/projects/cyclops-group/files/" \
                      "jmxterm/{version}/jmxterm-{version}-uber.jar" \
                      .format(version=JMX_TERM_VERSION)

    CACHE_DIR = os.environ.get(
        'XDG_CACHE_HOME',
        os.path.join(os.path.expanduser('~'), '.cache', 'crate-tests')
    )

    def __init__(self, jmx_port):
        self.jmx_port = jmx_port
        self.jmx_path = self._get_jmx()

    def _get_jmx(self):
        jar_name = 'jmxterm-{}-uber.jar'.format(JmxTermClient.JMX_TERM_VERSION)
        jmx_path = os.path.join(JmxTermClient.CACHE_DIR, 'jmx')
        jar_path = os.path.join(jmx_path, jar_name)
        if not os.path.exists(jar_path):
            os.makedirs(jmx_path, exist_ok=True)
            urlretrieve(JmxTermClient.JMX_TERM_SOURCE, jar_path)
        return jar_path

    def query_jmx(self, bean, attribute):
        p = Popen(
            [
                'java', '-jar', self.jmx_path,
                '-l', 'localhost:{}'.format(self.jmx_port),
                '-v', 'silent', '-n'
            ],
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            env={'JAVA_HOME': JmxTermClient.JAVA_HOME}
        )

        query = 'get -s -b {} {}'.format(bean, attribute)
        return p.communicate(bytes(query, 'utf-8'))


class MonitoringIntegrationTest(unittest.TestCase):

    JMX_PORT = GLOBAL_PORT_POOL.get()
    CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()

    def test_mbean_select_frq_attribute(self):
        jmx_client = JmxTermClient(MonitoringIntegrationTest.JMX_PORT)
        conn = connect('localhost:{}'
                       .format(MonitoringIntegrationTest.CRATE_HTTP_PORT))
        c = conn.cursor()

        to_sleep = 0.2
        while True:
            c.execute("select 1")
            value, _ = jmx_client.query_jmx(
                'io.crate.monitoring:type=QueryStats',
                'SelectQueryFrequency'
            )
            if float(value) > 0.0:
                break
            if to_sleep > 30:
                raise AssertionError('''The mbean attribute has not produced
                                     the expected result.''')
            time.sleep(to_sleep)
            to_sleep *= 2


class MonitoringSettingIntegrationTest(unittest.TestCase):

    JMX_PORT = GLOBAL_PORT_POOL.get()

    def test_enterprise_setting_disabled(self):
        jmx_client = JmxTermClient(MonitoringIntegrationTest.JMX_PORT)
        value, _ = jmx_client.query_jmx(
            'io.crate.monitoring:type=QueryStats',
            'SelectQueryFrequency'
        )
        try:
            float(value)
            raise AssertionError('''The JMX monitoring is enabled.''')
        except:
            pass


def test_suite():
    suite = unittest.TestSuite()
    s1 = unittest.TestSuite(unittest.makeSuite(MonitoringIntegrationTest))
    s1.layer = CrateLayer(
        'crate-enterprise',
        crate_home=crate_path(),
        port=MonitoringIntegrationTest.CRATE_HTTP_PORT,
        transport_port=GLOBAL_PORT_POOL.get(),
        env={
            "CRATE_JAVA_OPTS":
                JMX_OPTS.format(MonitoringIntegrationTest.JMX_PORT)
        },
        settings={
            'stats.enabled': True,
            'license.enterprise': True
        }
    )
    suite.addTest(s1)

    s2 = unittest.TestSuite(unittest.makeSuite(MonitoringSettingIntegrationTest))
    s2.layer = CrateLayer(
        'crate',
        crate_home=crate_path(),
        port=GLOBAL_PORT_POOL.get(),
        transport_port=GLOBAL_PORT_POOL.get(),
        env={
            "CRATE_JAVA_OPTS":
                JMX_OPTS.format(MonitoringSettingIntegrationTest.JMX_PORT)
        },
        settings={
            'stats.enabled': True,
            'license.enterprise': False
        }
    )
    suite.addTest(s2)
    return suite
