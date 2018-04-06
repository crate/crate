"""
Program to execute sqllogictest files against CrateDB.

See https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

This program can only execute "full scripts". "prototype scripts" are not
supported.
"""

import os
import re
import sys
import logging
import argparse
import psycopg2
from functools import partial
from hashlib import md5
from tqdm import tqdm

# disable monitor thread
tqdm.monitor_interval = 0


QUERY_WHITELIST = [re.compile(o, re.IGNORECASE) for o in [
    # CREATE INDEX is not supported, but raises SQLParseException
    'CREATE INDEX.*',
    '.*BETWEEN.*NULL.*',
    # Result is not deterministic
    'SELECT - SUM \\( col1 \\) \\* \\+ col1 FROM tab0 cor0 GROUP BY col1, col1',
]]

varchar_to_string = partial(re.compile('VARCHAR\(\d+\)').sub, 'STRING')
text_to_string = partial(re.compile('TEXT').sub, 'STRING')
real_to_double = partial(re.compile('REAL').sub, 'DOUBLE')


class IncorrectResult(BaseException):
    pass


class Statement:
    def __init__(self, cmd):
        """Create a statement

        A statement is usually a DML statement that is expected to either work
        or raise an error

        cmd format is:

            statement [ok | error]
            <statement>
        """
        self.expect_ok = cmd[0].endswith('ok')
        self.query = '\n'.join(cmd[1:])

    def execute(self, cursor):
        stmt = real_to_double(text_to_string(varchar_to_string(self.query)))
        try:
            cursor.execute(stmt)
        except psycopg2.Error as e:
            if self.expect_ok:
                raise IncorrectResult(e)

    def __repr__(self):
        return 'Statement<{0:.30}>'.format(self.query)


def validate_hash(rows, formats, expected_values, hash_):
    values = len(rows)
    if values != expected_values:
        raise IncorrectResult(
            'Expected {0} values, got {1}'.format(expected_values, values))
    m = md5()
    for row in rows:
        m.update('{0}'.format(row).encode('ascii'))
        m.update('\n'.encode('ascii'))
    digest = m.hexdigest()
    if digest != hash_:
        raise IncorrectResult('Expected values hashing to {0}. Got {1}\n{2}'.format(
            hash_, digest, rows))


def validate_cmp_result(rows, formats, expected_rows):
    if rows != expected_rows:
        raise IncorrectResult(
            'Expected rows: {0}. Got {1}'.format(expected_rows, rows))


class Query:

    HASHING_RE = re.compile('(\d+) values hashing to ([a-z0-9]+)')
    VALID_RESULT_FORMATS = set('TIR')

    def __init__(self, cmd):
        """Create a query

        cmd format is:

            query <type-string> <sort-mode> [<label>]
            <the actual query
            can take up multiple lines>
            ----
            <result or num values + hash>

        type-string is one of I, R, T per column where:
            I -> Integer result
            R -> Floating point result
            T -> Text result

        sort-mode is either nosort or rowsort.
        (There is also valuesort - but this is not yet implemented)

        label is optional and ignored.

        The result itself is either:

         - The rows transformed to have a single column

            Example:

                2 rows with 2 columns:

                    a| b
                    c| d

                Becomes:

                    a
                    b
                    c
                    d

         - The number of values in the result + a md5 hash of the result
        """
        self.result = None
        for i, line in enumerate(cmd):
            if line.startswith('---'):
                self.query = ' '.join(cmd[1:i])
                self.result = cmd[i + 1:]
                break
        else:
            self.query = ' '.join(cmd[1:])

        __, result_formats, sort, *__ = cmd[0].split()
        if result_formats and not (set(result_formats) & Query.VALID_RESULT_FORMATS):
            raise ValueError(
                'Invalid result format codes: {0}\n{1}'.format(result_formats, cmd))
        self.result_formats = result_formats
        self.sort = sort
        self._init_validation_function()

    def _init_validation_function(self):
        if not self.result:
            return
        if len(self.result) == 1:
            m = Query.HASHING_RE.match(self.result[0])
            if m:
                values, hash_ = m.groups()
                self.validate_result = partial(
                    validate_hash, expected_values=int(values), hash_=hash_)
                return
        self.format_rows(self.result)
        self.validate_result = partial(
            validate_cmp_result, expected_rows=self.result)

    def validate_result(self, rows, formats):
        pass

    def format_rows(self, rows):
        for i, row in enumerate(rows):
            if row is None:
                rows[i] = row = 'NULL'
            fmt = self.result_formats[i % len(self.result_formats)]
            if (row != 'NULL'):
                if fmt == 'I':
                    rows[i] = int(row)
                elif fmt == 'R':
                    rows[i] = float(row)
                elif fmt == 'T':
                    rows[i] = str(row)

    def execute(self, cursor):
        cursor.execute(self.query)
        rows = cursor.fetchall()

        if len(rows) > 1 and self.sort == 'rowsort':
            rows = sorted(rows, key=lambda row: [str(c) for c in row])
        # flatten the row values for comparison
        rows = [col for row in rows for col in row]
        if self.sort == 'valuesort':
            rows = sorted(rows, key=lambda v: str(v))
        self.format_rows(rows)
        self.validate_result(rows, self.result_formats)

    def __repr__(self):
        return 'Query<{0}, {1}, {2:.30}>'.format(
            self.result_formats, self.sort, self.query)


def parse_cmd(cmd):
    """Parse a command into Statement or Query

    >>> parse_cmd(['statement ok', 'INSERT INTO tab0 VALUES(35,97,1)'])
    Statement

    >>> parse_cmd([
    ...     'query III rowsort',
    ...     'SELECT ALL * FROM tab0 AS cor0',
    ...     '---',
    ...     '9 values hashing to 38a1673e2e09d694c8cec45c797034a7',
    ... ])
    Query

    >>> parse_cmd([
    ...     'skipif mysql # not compatible',
    ...     'query I rowsort label-208',
    ...     'SELECT - col1 / col2 col2 FROM tab1 AS cor0',
    ...     '----',
    ...     '0',
    ...     '0',
    ...     '0'
    ... ])
    Query
    """
    type_ = cmd[0]
    while type_.startswith(('skipif', 'onlyif')):
        cmd.pop(0)
        type_ = cmd[0]
    if type_.startswith('statement'):
        return Statement(cmd)
    if type_.startswith('query'):
        return Query(cmd)
    raise ValueError('Could not parse command: {0}'.format(cmd))


def get_commands(lines):
    """Split lines by empty line occurences into lists of lines"""
    command = []
    for line in lines:
        if line.startswith(('#', 'hash-threshold')):
            continue
        line = line.strip()
        if not line or line == '':
            if not command:
                continue
            yield command
            command = []
        else:
            command.append(line)
    if command:
        yield command


def _exec_on_crate(cmd):
    for line in cmd:
        if line.startswith('skipif crate'):
            return False
        if line.startswith('onlyif') and not line.startswith('onlyif crate'):
            return False
    return True


def _refresh_tables(cursor):
    cursor.execute("select table_name from information_schema.tables "
                   "where table_type = 'BASE TABLE' and table_schema = 'doc'")
    rows = cursor.fetchall()
    for (table,) in rows:
        cursor.execute('refresh table ' + table)


def _drop_relations(cursor):
    cursor.execute("select table_name from information_schema.tables "
                   "where table_type = 'BASE TABLE' and table_schema = 'doc'")
    for (table,) in cursor.fetchall():
        cursor.execute('drop table ' + table)
    cursor.execute("select table_name from information_schema.tables "
                   "where table_type = 'VIEW' and table_schema = 'doc'")
    views = [row[0] for row in cursor.fetchall()]
    if views:
        cursor.execute('drop view ' + ', '.join(views))


def get_logger(level, filename=None):
    logger = logging.getLogger('sqllogic')
    logger.setLevel(logging.NOTSET)
    handler = logging.FileHandler(filename) if filename else logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter('%(levelname)s; %(testfile)s; %(message)s'))
    logger.addHandler(handler)
    return logger


def run_file(filename, host, port, log_level, log_file, failfast):
    logger = get_logger(log_level, log_file)
    conn = psycopg2.connect("host=" + host + " port=" + port + " user=crate dbname='doc'")
    cursor = conn.cursor()
    fh = open(filename, 'r', encoding='utf-8')
    commands = get_commands(fh)
    commands = (cmd for cmd in commands if _exec_on_crate(cmd))
    if os.environ.get('TQDM_ENABLED', 'True').lower() == 'true':
        commands = tqdm(commands)
    dml_done = False
    attr = dict(testfile=fh.name)
    try:
        for cmd in commands:
            s_or_q = parse_cmd(cmd)
            if not dml_done and isinstance(s_or_q, Query):
                dml_done = True
                _refresh_tables(cursor)
            try:
                s_or_q.execute(cursor)
            except psycopg2.Error as e:
                logger.info('%s; %s', s_or_q.query, e, extra=attr)
            except IncorrectResult as e:
                if not any(p.match(s_or_q.query) for p in QUERY_WHITELIST):
                    logger.error('%s; %s', s_or_q.query, e, extra=attr)
                    if failfast:
                        raise e
                else:
                    logger.debug('%s; %s', cmd[1], 'Query is whitelisted', extra=attr)
            except NotImplementedError as e:
                logger.warn('%s; %s', s_or_q.query, e, extra=attr)
    finally:
        fh.close()
        _drop_relations(cursor)
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(prog='sqllogictest.py', description=__doc__)
    parser.add_argument('-f', '--file',
                        type=str, required=True)
    parser.add_argument('--host',
                        type=str, default='localhost')
    parser.add_argument('--port',
                        type=str, default='5432')
    parser.add_argument('-l', '--log-level',
                        type=int, default=logging.WARNING,
                        help='Python log levels: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50')
    parser.add_argument('--failfast',
                        action='store_true', default=False,
                        help='Fail on first error.')
    args = parser.parse_args()
    run_file(args.file, args.host, args.port, args.log_level, None, args.failfast)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
