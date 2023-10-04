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
import doctest
import zc.customdoctests
from crate.testing.layer import CrateLayer
import os
import time
import shutil
import json
import re
import sys
import random
import tempfile
import logging
import subprocess
from functools import partial
from . import process_test
from testutils.paths import crate_path, project_path
from testutils.ports import GLOBAL_PORT_POOL
from urllib.request import urlopen, Request
from crate.crash.command import CrateShell
from crate.crash.printer import PrintWrapper, ColorPrinter
from crate.client import connect


CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()
CRATE_DSN = 'localhost:' + str(CRATE_HTTP_PORT)

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


CRATE_CE = True if os.environ.get('CRATE_CE') is "1" else False

CRATE_SETTINGS = {'psql.port': GLOBAL_PORT_POOL.get(),
                  'es.api.enabled': 'true'}
if CRATE_CE:
    CRATE_SETTINGS['license.enterprise'] = 'false'
else:
    CRATE_SETTINGS['license.enterprise'] = 'true'
    CRATE_SETTINGS['lang.js.enabled'] = 'true'


class CrateTestShell(CrateShell):

    def __init__(self):
        super(CrateTestShell, self).__init__(is_tty=False)
        self.logger = ColorPrinter(False, stream=PrintWrapper(), line_end='\n')

    def stmt(self, stmt):
        stmt = stmt.replace('\n', ' ')
        self.process(stmt)


cmd = CrateTestShell()


def _execute_sql(stmt):
    """
    Invoke a single SQL statement and automatically close the HTTP connection
    when done.
    """
    with connect(CRATE_DSN) as conn:
        c = conn.cursor()
        c.execute(stmt)


def wait_for_schema_update(schema, table, column):
    with connect(CRATE_DSN) as conn:
        c = conn.cursor()
        count = 0
        while count == 0:
            c.execute(
                ('select count(*) from information_schema.columns '
                 'where table_schema = ? and table_name = ? '
                 'and column_name = ?'),
                (schema, table, column)
            )
            count = c.fetchone()[0]


def wait_for_function(signature):
    with connect(CRATE_DSN) as conn:
        c = conn.cursor()
        wait = 0.0

        while True:
            try:
                c.execute('SELECT ' + signature)
            except Exception as e:
                wait += 0.1
                if wait >= 2.0:
                    raise e
                else:
                    time.sleep(0.1)
            else:
                break


def bash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    s = s.replace(':4200/', ':{0}/'.format(CRATE_HTTP_PORT))
    if s.startswith("crash"):
        s = re.search(r"crash\s+-c\s+\"(.*?)\"", s).group(1)
        return u'cmd.stmt({0})'.format(repr(s.strip().rstrip(';')))
    return (r'pretty_print(sh("""%s""").stdout.decode("utf-8"))' % s) + '\n'


def pretty_print(s):
    try:
        d = json.loads(s)
        print(json.dumps(d, indent=2))
    except json.decoder.JSONDecodeError:
        print(s)


def crash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    if s.startswith('_'):
        return s[1:]
    s = s.replace(':4200', ':{0}'.format(CRATE_HTTP_PORT))
    return u'cmd.stmt({0})'.format(repr(s.strip().rstrip(';')))


bash_parser = zc.customdoctests.DocTestParser(
    ps1=r'sh\$', comment_prefix='#', transform=bash_transform)

crash_parser = zc.customdoctests.DocTestParser(
    ps1='cr>', comment_prefix='#', transform=crash_transform)


class ConnectingCrateLayer(CrateLayer):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('settings', {})
        self.repo_path = kwargs['settings']['path.repo'] = tempfile.mkdtemp()
        super(ConnectingCrateLayer, self).__init__(*args, **kwargs)

    def start(self):
        super(ConnectingCrateLayer, self).start()
        cmd._connect(self.crate_servers[0])

    def tearDown(self):
        print('')
        print('ConnectingCrateLayer.tearDown()')
        shutil.rmtree(self.repo_path, ignore_errors=True)
        super(ConnectingCrateLayer, self).tearDown()


def setUpLocations(test):
    setUp(test)
    _execute_sql("""
        create table locations (
          id string primary key,
          name string,
          "date" timestamp,
          kind string,
          position integer,
          description string,
          race object(dynamic) as (
            interests array(string),
            description string,
            name string
          ),
          information array(object as (
              population long,
              evolution_level short
            )
          ),
          index name_description_ft using fulltext(name, description) with (analyzer='english')
        ) clustered by(id) into 2 shards with (number_of_replicas=0)""".strip())
    _execute_sql("delete from locations")
    locations_file = get_abspath("locations.json")
    _execute_sql("""copy locations from '{0}'""".format(locations_file))
    _execute_sql("""refresh table locations""")
    import_failures_dir = '/tmp/import_data/locations_with_failure'
    os.makedirs(import_failures_dir, exist_ok=True)
    shutil.copy(
        get_abspath("locations_import_summary1.json"),
        os.path.join(import_failures_dir, "locations1.json")
    )
    shutil.copy(
        get_abspath("locations_import_summary2.json"),
        os.path.join(import_failures_dir, "locations2.json")
    )


def setUpUserVisits(test):
    setUp(test)
    _execute_sql("""
        create table uservisits (
          id integer primary key,
          name string,
          visits integer,
          last_visit timestamp
        )
    """.strip())
    uservisits_file = get_abspath("uservisits.json")
    _execute_sql("""copy uservisits from '{0}'""".format(uservisits_file))
    _execute_sql("""refresh table uservisits""")


def setUpArticles(test):
    setUp(test)
    _execute_sql("""
        create table articles (
          id integer primary key,
          name string,
          price float
        ) clustered by(id) into 2 shards with (number_of_replicas=0)""".strip())
    articles_file = get_abspath("articles.json")
    _execute_sql("""copy articles from '{0}'""".format(articles_file))
    _execute_sql("""refresh table articles""")


def setUpColors(test):
    setUp(test)
    _execute_sql("""
        create table colors (
          id integer primary key,
          name string,
          rgb string,
          coolness float
        ) with (number_of_replicas=0)""".strip())
    colors_file = get_abspath("colors.json")
    _execute_sql("""copy colors from '{0}'""".format(colors_file))
    _execute_sql("""refresh table colors""")


def setUpEmployees(test):
    setUp(test)
    _execute_sql("""
        create table employees (
          id integer primary key,
          name string,
          surname string,
          dept_id integer,
          sex string
        ) with (number_of_replicas=0)""".strip())
    emp_file = get_abspath("employees.json")
    _execute_sql("""copy employees from '{0}'""".format(emp_file))
    _execute_sql("""refresh table employees""")


def setUpDepartments(test):
    setUp(test)
    _execute_sql("""
        create table departments (
          id integer primary key,
          name string,
          manager_id integer,
          location integer
        ) with (number_of_replicas=0)""".strip())
    dept_file = get_abspath("departments.json")
    _execute_sql("""copy departments from '{0}'""".format(dept_file))
    _execute_sql("""refresh table departments""")


def setUpQuotes(test):
    setUp(test)
    _execute_sql("""
        create table quotes (
          id integer primary key,
          quote string,
          index quote_ft using fulltext (quote)
        ) clustered by(id) into 2 shards with(number_of_replicas=0)""")

    import_dir = '/tmp/import_data'
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(
        project_path('sql/src/test/resources/essetup/data/copy',
                     'test_copy_from.json'),
        os.path.join(import_dir, "quotes.json")
    )


def setUpPhotos(test):
    setUp(test)
    _execute_sql("""
        create table photos (
          name string,
          location geo_point
        ) with(number_of_replicas=0)""".strip())
    dept_file = get_abspath("photos.json")
    _execute_sql("""copy photos from '{0}'""".format(dept_file))
    _execute_sql("""refresh table photos""")


def setUpCountries(test):
    setUp(test)
    _execute_sql("""
        create table countries (
          name string,
          "geo" geo_shape INDEX using GEOHASH with (precision='1km'),
          population long
        ) with(number_of_replicas=0)""".strip())
    dept_file = get_abspath("countries.json")
    _execute_sql("""copy countries from '{0}'""".format(dept_file))
    _execute_sql("""refresh table countries""")


def setUpLocationsAndQuotes(test):
    setUpLocations(test)
    setUpQuotes(test)


def setUpColorsAndArticles(test):
    setUpColors(test)
    setUpArticles(test)


def setUpLocationsQuotesAndUserVisits(test):
    setUpLocationsAndQuotes(test)
    setUpUserVisits(test)


def setUpEmployeesAndDepartments(test):
    setUpEmployees(test)
    setUpDepartments(test)


def setUpPhotosAndCountries(test):
    setUpPhotos(test)
    setUpCountries(test)


def setUpEmpDeptAndColourArticlesAndGeo(test):
    setUpEmployeesAndDepartments(test)
    setUpColorsAndArticles(test)
    setUpPhotosAndCountries(test)


def setUpTutorials(test):
    setUp(test)
    import_dir = '/tmp/best_practice_data'
    source_dir = 'sql/src/test/resources/essetup/data/best_practice'
    if not os.path.isdir(import_dir):
        os.mkdir(import_dir)
    shutil.copy(project_path(source_dir, 'data_import.json'),
                os.path.join(import_dir, "users.json"))
    shutil.copy(project_path(source_dir, 'data_import.json.gz'),
                os.path.join(import_dir, "users.json.gz"))
    shutil.copy(project_path(source_dir, 'data_import_1408312800.json'),
                os.path.join(import_dir, "users_1408312800.json"))


def setUp(test):
    test.globs['cmd'] = cmd
    test.globs['wait_for_schema_update'] = wait_for_schema_update
    test.globs['wait_for_function'] = wait_for_function


def tearDown(test):
    # drop leftover tables after each test
    with connect(CRATE_DSN) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('blob', 'sys', 'information_schema', 'pg_catalog')
        """)
        for schema, table in c.fetchall():
            try:
                c.execute("""
                    DROP TABLE IF EXISTS "{}"."{}"
                """.format(schema, table))
            except Exception as e:
                print('Failed to drop table {}.{}: {}'.format(schema, table, e))


docsuite = partial(doctest.DocFileSuite,
                   tearDown=tearDown,
                   parser=crash_parser,
                   optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
                   encoding='utf-8')

doctest_file = partial(os.path.join, '..', '..')


def doctest_files(*items):
    return (doctest_file(item) for item in items)


def get_abspath(name):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), name)
    )


def create_doctest_suite():
    crate_layer = ConnectingCrateLayer(
        'crate',
        host='localhost',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT,
        env={'JAVA_HOME': os.environ.get('JAVA_HOME', '')},
        settings=CRATE_SETTINGS
    )
    tests = []

    for fn in doctest_files('general/blobs.rst',
                            'interfaces/http.rst',):
        s = docsuite(fn, parser=bash_parser, setUp=setUpLocations, globs={
            'sh': partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=60,
                shell=True
            ),
            'pretty_print': pretty_print
        })
        s.layer = crate_layer
        tests.append(s)

    if not CRATE_CE:
        # These tests uses features only available in the CrateDB Enterprise Edition
        for fn in doctest_files('general/user-defined-functions.rst',
                                'general/information-schema.rst',
                                'general/builtins/aggregation.rst',
                                'general/builtins/scalar.rst',
                                'admin/user-management.rst',
                                'admin/system-information.rst',
                                'admin/ingestion/rules.rst',
                                'admin/privileges.rst'):
            s = docsuite(fn, setUp=setUpLocationsAndQuotes)
            s.layer = crate_layer
            tests.append(s)

        for fn in doctest_files('general/builtins/window-functions.rst'):
            s = docsuite(fn, setUp=setUpEmpDeptAndColourArticlesAndGeo)
            s.layer = crate_layer
            tests.append(s)

    for fn in doctest_files('general/ddl/create-table.rst',
                            'general/ddl/generated-columns.rst',
                            'general/ddl/constraints.rst',
                            'general/ddl/sharding.rst',
                            'general/ddl/replication.rst',
                            'general/ddl/column-policy.rst',
                            'general/ddl/system-columns.rst',
                            'general/ddl/alter-table.rst',
                            'general/ddl/storage.rst',
                            'general/ddl/fulltext-indices.rst',
                            'admin/runtime-config.rst',
                            'general/ddl/show-create-table.rst',
                            'admin/snapshots.rst',
                            'general/dql/index.rst',
                            'general/dql/refresh.rst',
                            'admin/optimization.rst',
                            'general/dql/fulltext.rst',
                            'general/ddl/data-types.rst',
                            'general/occ.rst',
                            'general/ddl/partitioned-tables.rst',
                            'general/builtins/arithmetic.rst',
                            'general/builtins/table-functions.rst',
                            'general/dql/selects.rst',
                            'interfaces/postgres.rst',
                            'general/ddl/views.rst',
                            'sql/general/value-expressions.rst',
                            'sql/general/lexical-structure.rst'):
        s = docsuite(fn, setUp=setUpLocationsAndQuotes)
        s.layer = crate_layer
        tests.append(s)

    for fn in doctest_files('general/dql/geo.rst',):
        s = docsuite(fn, setUp=setUpCountries)
        s.layer = crate_layer
        tests.append(s)

    for fn in doctest_files('general/dql/joins.rst',
                            'general/builtins/subquery-expressions.rst'):
        s = docsuite(fn, setUp=setUpEmpDeptAndColourArticlesAndGeo)
        s.layer = crate_layer
        tests.append(s)

    for fn in doctest_files('general/dml.rst',):
        s = docsuite(fn, setUp=setUpLocationsQuotesAndUserVisits)
        s.layer = crate_layer
        tests.append(s)

    for fn in doctest_files('general/dql/union.rst',):
        s = docsuite(fn, setUp=setUpPhotosAndCountries)
        s.layer = crate_layer
        tests.append(s)

    # randomize order of tests to make sure they don't depend on each other
    random.shuffle(tests)

    suite = unittest.TestSuite()
    suite.addTests(tests)
    return suite


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromModule(process_test))
    suite.addTests(create_doctest_suite())
    return suite
