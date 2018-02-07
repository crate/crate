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
import re
import tempfile
import logging
from functools import partial
from . import process_test
from testutils.paths import crate_path, project_path
from testutils.ports import GLOBAL_PORT_POOL
from crate.crash.command import CrateCmd
from crate.crash.printer import PrintWrapper, ColorPrinter
from crate.client import connect


CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()
CRATE_DSN = 'localhost:' + str(CRATE_HTTP_PORT)

log = logging.getLogger('crate.testing.layer')
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
log.addHandler(ch)


class CrateTestCmd(CrateCmd):

    def __init__(self):
        super(CrateTestCmd, self).__init__(is_tty=False)
        self.logger = ColorPrinter(False, stream=PrintWrapper(), line_end='\n')

    def stmt(self, stmt):
        stmt = stmt.replace('\n', ' ')
        if stmt.startswith('\\'):
            self.process(stmt)
        else:
            self.execute(stmt)


cmd = CrateTestCmd()


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
    return (
        r'import subprocess;'
        r'print(subprocess.check_output(r"""%s""",stderr=subprocess.STDOUT,shell=True).decode("utf-8"))' % s
    ) + '\n'


def crash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    if s.startswith('_'):
        return s[1:]
    s = s.replace(':4200', ':{0}'.format(CRATE_HTTP_PORT))
    return u'cmd.stmt({0})'.format(repr(s.strip().rstrip(';')))


bash_parser = zc.customdoctests.DocTestParser(
    ps1='sh\$', comment_prefix='#', transform=bash_transform)

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
        shutil.rmtree(self.repo_path, ignore_errors=True)
        super(ConnectingCrateLayer, self).tearDown()


crate_layer = ConnectingCrateLayer(
    'crate',
    host='localhost',
    crate_home=crate_path(),
    port=CRATE_HTTP_PORT,
    transport_port=CRATE_TRANSPORT_PORT,
    env={'JAVA_HOME': os.environ.get('JAVA_HOME', '')},
    settings={
         # The disk.watermark settings can be removed once crate-python > 0.21.1 has been released
        'cluster.routing.allocation.disk.watermark.low': '100k',
        'cluster.routing.allocation.disk.watermark.high': '10k',
        'cluster.routing.allocation.disk.watermark.flood_stage': '1k',
        'license.enterprise': 'true',
        'lang.js.enabled': 'true'
    }
)


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
    setUp(test)
    setUpLocations(test)
    setUpQuotes(test)


def setUpColorsAndArticles(test):
    setUp(test)
    setUpColors(test)
    setUpArticles(test)


def setUpLocationsQuotesAndUserVisits(test):
    setUp(test)
    setUpLocationsAndQuotes(test)
    setUpUserVisits(test)


def setUpEmployeesAndDepartments(test):
    setUp(test)
    setUpEmployees(test)
    setUpDepartments(test)


def setUpPhotosAndCountries(test):
    setUp(test)
    setUpPhotos(test)
    setUpCountries(test)


def setUpEmpDeptAndColourArticlesAndGeo(test):
    setUp(test)
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
    with connect(CRATE_DSN) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('blob', 'sys', 'information_schema', 'pg_catalog')
            ORDER BY 1, 2
        """)
        for schema, table in c.fetchall():
            try:
                c.execute("""
                    DROP TABLE IF EXISTS "{}"."{}"
                """.format(schema, table))
            except Exception as e:
                print('Failed to drop table {}.{}: {}'.format(schema, table, e))


def get_abspath(name):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), name)
    )


def test_suite():
    suite = unittest.TestSuite()

    # Graceful stop tests
    process_suite = unittest.TestLoader().loadTestsFromModule(process_test)
    suite.addTest(process_suite)

    # Documentation tests
    docsuite = partial(doctest.DocFileSuite,
                       optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
                       encoding='utf-8')

    docs_suite = unittest.TestSuite()
    s = docsuite('../../general/blobs.rst', parser=bash_parser, setUp=setUp)
    s.layer = crate_layer
    docs_suite.addTest(s)
    for fn in ('interfaces/http.rst',):
        s = docsuite('../../' + fn,
                     parser=bash_parser,
                     setUp=setUpLocations,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/ddl/create-table.rst',
               'general/ddl/generated-columns.rst',
               'general/ddl/constraints.rst',
               'general/ddl/sharding.rst',
               'general/ddl/replication.rst',
               'general/ddl/column-policy.rst',
               'general/ddl/fulltext-indices.rst',
               'general/ddl/system-columns.rst',
               'general/ddl/alter-table.rst',
               'general/ddl/storage.rst',
               'admin/runtime-config.rst',
               'general/ddl/show-create-table.rst',
               'general/user-defined-functions.rst',
               'admin/user-management.rst',
               'admin/privileges.rst',
               'admin/ingestion/rules.rst',
               'general/dql/index.rst',
               'general/dql/refresh.rst',
               'admin/optimization.rst',
               'general/dql/fulltext.rst',
               'general/ddl/data-types.rst',
               'general/occ.rst',
               'general/information-schema.rst',
               'general/ddl/partitioned-tables.rst',
               'general/builtins/aggregation.rst',
               'general/builtins/arithmetic.rst',
               'general/builtins/scalar.rst',
               'general/builtins/table-functions.rst',
               'admin/system-information.rst',
               'general/dql/selects.rst',
               'interfaces/postgres.rst'):
        s = docsuite('../../' + fn, parser=crash_parser,
                     setUp=setUpLocationsAndQuotes,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/geo.rst',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpCountries,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/joins.rst',
               'general/builtins/subquery-expressions.rst',):
        path = os.path.join('..', '..', fn)
        s = docsuite(path,
                     parser=crash_parser,
                     setUp=setUpEmpDeptAndColourArticlesAndGeo,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dml.rst',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpLocationsQuotesAndUserVisits,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('admin/snapshots.rst',):
        s = docsuite('../../' + fn,
                     parser=crash_parser,
                     setUp=setUpLocationsAndQuotes,
                     tearDown=tearDown)
        s.layer = crate_layer
        docs_suite.addTest(s)
    for fn in ('general/dql/union.rst',):
        path = os.path.join('..', '..', fn)
        s = doctest.DocFileSuite(path,
                                 parser=crash_parser,
                                 setUp=setUpPhotosAndCountries,
                                 tearDown=tearDown,
                                 optionflags=doctest.NORMALIZE_WHITESPACE |
                                             doctest.ELLIPSIS,
                                 encoding='utf-8')
        s.layer = crate_layer
        docs_suite.addTest(s)

    suite.addTests(docs_suite)
    return suite
