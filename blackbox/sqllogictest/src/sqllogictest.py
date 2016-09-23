"""
Program to execute sqllogictest files against Crate.

See https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

This program can only execute "full scripts". "prototype scripts" are not
supported.
"""

import argparse
import re
from functools import partial
from hashlib import md5
from crate.client import connect
from crate.client import exceptions
from tqdm import tqdm


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
        self.stmt = '\n'.join(cmd[1:])

    def execute(self, cursor):
        try:
            cursor.execute(self.stmt)
        except exceptions.ProgrammingError as e:
            if self.expect_ok:
                raise IncorrectResult(e)

    def __repr__(self):
        return 'Statement<{0:.30}>'.format(self.stmt)


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
            if fmt == 'I' and row != 'NULL':
                rows[i] = int(row)

    def execute(self, cursor):
        cursor.execute(self.query)
        rows = cursor.fetchall()
        if self.sort == 'rowsort':
            rows = sorted(rows, key=lambda row: [str(c) for c in row])
        rows = [col for row in rows for col in row]
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
        if line.startswith('hash-threshold'):
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


def run_file(fh, hosts, verbose, failfast):
    conn = connect(hosts)
    cursor = conn.cursor()
    worked = 0
    failures = 0
    unsupported_statements = []
    incorrect_results = []
    commands = get_commands(fh)
    commands = (cmd for cmd in commands if _exec_on_crate(cmd))
    for cmd in tqdm(commands):
        s_or_q = parse_cmd(cmd)
        try:
            s_or_q.execute(cursor)
        except exceptions.ProgrammingError as e:
            unsupported_statements.append((cmd[1], e))
            failures += 1
        except IncorrectResult as e:
            tqdm.write(str(cmd))
            if failfast:
                raise e
            else:
                incorrect_results.append((cmd[1], e))
        else:
            worked += 1
    print('{0} queries worked'.format(worked))
    print('{0} queries are unsupported'.format(failures))
    if not failfast:
        for incorrect in incorrect_results:
            print('Query: \n\t{0}\n\t{1}'.format(*incorrect))
    if verbose:
        print('Unsupported statements:')
        for unsupported in unsupported_statements:
            print('Query: \n\t{0}\n\t{1}'.format(*unsupported))



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--file', type=argparse.FileType('r'), required=True)
    parser.add_argument('--hosts', type=str, default='http://localhost:4200')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('--failfast', action='store_true', default=False)
    args = parser.parse_args()
    run_file(args.file, args.hosts, args.verbose, args.failfast)


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
