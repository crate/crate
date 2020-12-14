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

import doctest
import zc.customdoctests
from cr8.run_crate import CrateNode
import os
import time
import shutil
import json
import re
import random
import tempfile
import subprocess
import unittest
from functools import partial
from testutils.paths import crate_path, project_path
from crate.crash.command import CrateShell
from crate.crash.printer import PrintWrapper, ColorPrinter
from crate.client import connect


ITEST_FILE_NAME_FILTER = os.environ.get('ITEST_FILE_NAME_FILTER')
if ITEST_FILE_NAME_FILTER:
    print("Applying file name filter: {}".format(ITEST_FILE_NAME_FILTER))


def is_target_file_name(item):
    return not ITEST_FILE_NAME_FILTER or (item and ITEST_FILE_NAME_FILTER in item)


CRATE_CE = True if os.environ.get('CRATE_CE') == "1" else False

CRATE_SETTINGS = {
    'psql.port': 0,
    'transport.tcp.port': 0,
    'node.name': 'crate',
    'cluster.name': 'Testing-CrateDB'
}


class CrateTestShell(CrateShell):

    def __init__(self):
        super(CrateTestShell, self).__init__(is_tty=False)
        self.logger = ColorPrinter(False, stream=PrintWrapper(), line_end='\n')


cmd = CrateTestShell()


def pretty_print(s):
    try:
        d = json.loads(s)
        print(json.dumps(d, indent=2))
    except json.decoder.JSONDecodeError:
        print(s)


class ConnectingCrateLayer(CrateNode):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('settings', {})
        self.repo_path = kwargs['settings']['path.repo'] = tempfile.mkdtemp()
        super().__init__(*args, **kwargs)

    def start(self):
        super().start()
        cmd._connect(self.http_url)

    def stop(self):
        print('')
        print('ConnectingCrateLayer.stop()')
        shutil.rmtree(self.repo_path, ignore_errors=True)
        super().stop()


crate = ConnectingCrateLayer(
    crate_dir=crate_path(),
    env={
        'JAVA_HOME': os.environ.get('JAVA_HOME', ''),
        'CRATE_JAVA_OPTS': '-Dio.netty.leakDetection.level=paranoid',
    },
    settings=CRATE_SETTINGS,
    version=(4, 0, 0)
)


def crash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    if s.startswith('_'):
        return s[1:]
    if hasattr(crate, 'addresses'):
        s = s.replace(':4200', ':{0}'.format(crate.addresses.http.port))
    return u'cmd.process({0})'.format(repr(s.strip().rstrip(';')))


def bash_transform(s):
    # The examples in the docs show the real port '4200' to a reader.
    # Our test suite requires the port to be '44200' to avoid conflicts.
    # Therefore, we need to replace the ports before a test is being run.
    if hasattr(crate, 'addresses'):
        s = s.replace(':4200', ':{0}'.format(crate.addresses.http.port))
    if s.startswith("crash"):
        s = re.search(r"crash\s+-c\s+\"(.*?)\"", s).group(1)
        return u'cmd.process({0})'.format(repr(s.strip().rstrip(';')))
    return (r'pretty_print(sh(r"""%s""").stdout.decode("utf-8"))' % s) + '\n'


bash_parser = zc.customdoctests.DocTestParser(
    ps1='sh\\$', comment_prefix='#', transform=bash_transform)

crash_parser = zc.customdoctests.DocTestParser(
    ps1='cr>', comment_prefix='#', transform=crash_transform)


def _execute_sql(stmt):
    """
    Invoke a single SQL statement and automatically close the HTTP connection
    when done.
    """
    with connect(crate.http_url) as conn:
        c = conn.cursor()
        c.execute(stmt)


def wait_for_schema_update(schema, table, column):
    with connect(crate.http_url) as conn:
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
    with connect(crate.http_url) as conn:
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




def setUpLocations(test):
    setUp(test)
    _execute_sql("""
        create table locations (
          id integer primary key,
          name string,
          "date" timestamp with time zone,
          kind string,
          position integer,
          description string,
          inhabitants object(dynamic) as (
            interests array(string),
            description string,
            name string
          ),
          information array(object as (
              population long,
              evolution_level short
            )
          ),
          landmarks array(string),
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
          last_visit timestamp with time zone
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
        project_path('server/src/test/resources/essetup/data/copy',
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
    with connect(crate.http_url) as conn:
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

doctest_file = partial(os.path.join, 'docs')


def doctest_files(*items):
    return (doctest_file(item) for item in items if is_target_file_name(item))


def get_abspath(name):
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), 'testdata', name)
    )


class DocTests(unittest.TestSuite):

    def run(self, result, debug=False):
        crate.start()
        try:
            super().run(result, debug)
        finally:
            crate.stop()
            cmd.close()


def load_tests(loader, suite, ignore):
    tests = []
    for fn in doctest_files('general/blobs.rst', 'interfaces/http.rst',):
        tests.append(
            docsuite(
                fn,
                parser=bash_parser,
                setUp=setUpLocations,
                globs={
                    'sh': partial(
                        subprocess.run,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        timeout=60,
                        shell=True
                    ),
                    'pretty_print': pretty_print
                }
            )
        )

    if not CRATE_CE:
        # These tests uses features only available in the CrateDB Enterprise Edition
        for fn in doctest_files('general/user-defined-functions.rst',
                                'general/information-schema.rst',
                                'general/builtins/aggregation.rst',
                                'general/builtins/scalar.rst',
                                'admin/user-management.rst',
                                'admin/system-information.rst',
                                'admin/privileges.rst'):
            tests.append(docsuite(fn, setUp=setUpLocationsAndQuotes))

        for fn in doctest_files('general/builtins/window-functions.rst'):
            tests.append(docsuite(fn, setUp=setUpEmpDeptAndColourArticlesAndGeo))

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
                            'general/ddl/partitioned-tables.rst',
                            'general/builtins/arithmetic.rst',
                            'general/builtins/table-functions.rst',
                            'general/builtins/array-comparisons.rst',
                            'general/dql/selects.rst',
                            'interfaces/postgres.rst',
                            'general/ddl/views.rst',
                            'sql/general/value-expressions.rst',
                            'sql/general/lexical-structure.rst',
                            'sql/statements/values.rst'):
        tests.append(docsuite(fn, setUp=setUpLocationsAndQuotes))

    for fn in doctest_files('general/occ.rst', 'sql/statements/refresh.rst'):
        tests.append(docsuite(fn, setUp=setUp))

    for fn in doctest_files('general/dql/geo.rst',):
        tests.append(docsuite(fn, setUp=setUpCountries))

    for fn in doctest_files('general/dql/joins.rst',
                            'general/builtins/subquery-expressions.rst'):
        tests.append(docsuite(fn, setUp=setUpEmpDeptAndColourArticlesAndGeo))

    for fn in doctest_files('general/dml.rst',):
        tests.append(docsuite(fn, setUp=setUpLocationsQuotesAndUserVisits))

    for fn in doctest_files('general/dql/union.rst',):
        tests.append(docsuite(fn, setUp=setUpPhotosAndCountries))

    if not tests:
        raise ValueError("ITEST_FILE_NAME_FILTER, no matches for: {}".format(ITEST_FILE_NAME_FILTER))

    # randomize order of tests to make sure they don't depend on each other
    random.shuffle(tests)
    return DocTests(tests)
