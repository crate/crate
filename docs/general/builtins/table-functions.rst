.. highlight:: psql

.. _table-functions:

===============
Table functions
===============

Table functions are :ref:`functions <gloss-function>` that produce a set of
rows. They can be used in place of a relation in the ``FROM`` clause.

If used within the select list, the table functions will be :ref:`evaluated
<gloss-evaluation>` per row of the relations in the ``FROM`` clause, generating
one or more rows which are appended to the result set.  If multiple table
functions with different amounts of rows are used, ``NULL`` values will be
returned for the functions that are exhausted.

For example::


    cr> select unnest([1, 2, 3]), unnest([1, 2]);
    +-------------------+----------------+
    | unnest([1, 2, 3]) | unnest([1, 2]) |
    +-------------------+----------------+
    |                 1 |              1 |
    |                 2 |              2 |
    |                 3 |           NULL |
    +-------------------+----------------+
    SELECT 3 rows in set (... sec)


.. note::

    Table functions in the select list are executed after aggregations. So
    aggregations can be used as arguments to table functions, but the other way
    around is not allowed, unless sub queries are utilized.

    For example::

        (SELECT aggregate_func(col) FROM (SELECT table_func(...) AS col) ...)

.. rubric:: Table of contents

.. contents::
   :local:


.. _table-functions-scalar:

Scalar functions
================

A :ref:`scalar function <scalar-functions>`, when used in the ``FROM`` clause
in place of a relation, will result in a table of one row and one column,
containing the :ref:`scalar value <gloss-scalar>` returned from the function.

::

    cr> SELECT * FROM abs(-5), initcap('hello world');
    +-----+-------------+
    | abs | initcap     |
    +-----+-------------+
    |   5 | Hello World |
    +-----+-------------+
    SELECT 1 row in set (... sec)


``empty_row( )``
================

empty_row doesn't take any argument and produces a table with an empty row and
no column.

::

    cr> select * from empty_row();
    SELECT OK, 1 row affected  (... sec)


.. _unnest:

``unnest( array [ array , ] )``
===============================

unnest takes any number of array parameters and produces a table where each
provided array argument results in a column.

The columns are named ``colN`` where ``N`` is a number starting at 1.

::

    cr> select * from unnest([1, 2, 3], ['Arthur', 'Trillian', 'Marvin']);
    +------+----------+
    | col1 | col2     |
    +------+----------+
    |    1 | Arthur   |
    |    2 | Trillian |
    |    3 | Marvin   |
    +------+----------+
    SELECT 3 rows in set (... sec)


.. _table-functions-generate-series:

``pg_catalog.generate_series(start, stop, [step])``
===================================================

Generate a series of values from inclusive start to inclusive stop with
``step`` increments.

The argument can be ``integer`` or ``bigint``, in which case ``step`` is
optional and defaults to ``1``.

``start`` and ``stop`` can also be of type ``timestamp with time zone`` or
``timestamp without time zone`` in which case ``step`` is required and must be
of type ``interval``.

The return value always matches the ``start`` / ``stop`` types.


::

    cr> SELECT * FROM generate_series(1, 4);
    +-----------------+
    | generate_series |
    +-----------------+
    |               1 |
    |               2 |
    |               3 |
    |               4 |
    +-----------------+
    SELECT 4 rows in set (... sec)

::

    cr> SELECT
    ...     x,
    ...     date_format('%Y-%m-%d, %H:%i', x)
    ...     FROM generate_series('2019-01-01 00:00'::timestamp, '2019-01-04 00:00'::timestamp, '30 hours'::interval) AS t(x);
    +---------------+-----------------------------------+
    |             x | date_format('%Y-%m-%d, %H:%i', x) |
    +---------------+-----------------------------------+
    | 1546300800000 | 2019-01-01, 00:00                 |
    | 1546408800000 | 2019-01-02, 06:00                 |
    | 1546516800000 | 2019-01-03, 12:00                 |
    +---------------+-----------------------------------+
    SELECT 3 rows in set (... sec)


.. _table-functions-generate-subscripts:

``pg_catalog.generate_subscripts(array, dim, [reverse])``
=========================================================

Generate the subscripts for the specified dimension ``dim`` of the given
``array``. Zero rows are returned for arrays that do not have the requested
dimension, or for ``NULL`` arrays (but valid subscripts are returned for
``NULL`` array elements).

If ``reverse`` is ``true`` the subscripts will be returned in reverse order.

This example takes a one dimensional array of four elements, where elements
at positions 1 and 3 are ``NULL``:

::

    cr> SELECT generate_subscripts([NULL, 1, NULL, 2], 1) AS s;
    +---+
    | s |
    +---+
    | 1 |
    | 2 |
    | 3 |
    | 4 |
    +---+
    SELECT 4 rows in set (... sec)

This example returns the reversed list of subscripts for the same array:

::

    cr> SELECT generate_subscripts([NULL, 1, NULL, 2], 1, true) AS s;
    +---+
    | s |
    +---+
    | 4 |
    | 3 |
    | 2 |
    | 1 |
    +---+
    SELECT 4 rows in set (... sec)

This example works on an array of three dimensions. Each of the elements within
a given level must be either ``NULL``, or an array of the same size as the
other arrays within the same level.

::

    cr> select generate_subscripts([[[1],[2]], [[3],[4]], [[4],[5]]], 2) as s;
    +---+
    | s |
    +---+
    | 1 |
    | 2 |
    +---+
    SELECT 2 rows in set (... sec)


.. _table-functions-regexp-matches:

``regexp_matches(source, pattern [, flags])``
=============================================

Uses the :ref:`regular expression <gloss-regular-expression>` ``pattern`` to
match against the ``source`` string.

The result rows have one column:

.. list-table::
    :header-rows: 1

    * - Column name
      - Description
    * - groups
      - ``array(text)``

If ``pattern`` matches ``source``, an array of the matched regular expression
groups is returned.

If no regular expression group was used, the whole pattern is used as a group.

A regular expression group is formed by a subexpression that is surrounded by
parentheses. The position of a group is determined by the position of its
opening parenthesis.

For example when matching the pattern ``\b([A-Z])`` a match for the
subexpression ``([A-Z])`` would create group No. 1. If you want to group items
with parentheses, but without grouping, use ``(?...)``.

For example matching the regular expression ``([Aa](.+)z)`` against
``alcatraz``, results in these groups:

- group 1: ``alcatraz`` (from first to last parenthesis or whole pattern)
- group 2: ``lcatra`` (beginning at second parenthesis)

The ``regexp_matches`` :ref:`function <gloss-function>` will return all groups
as a ``text`` array::

    cr> select regexp_matches('alcatraz', '(a(.+)z)') as matched;
    +------------------------+
    | matched                |
    +------------------------+
    | ["alcatraz", "lcatra"] |
    +------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select regexp_matches('alcatraz', 'traz') as matched;
    +----------+
    | matched  |
    +----------+
    | ["traz"] |
    +----------+
    SELECT 1 row in set (... sec)

Through array element access functionality, a group can be selected directly.
See :ref:`sql_dql_object_arrays` for details.

::

    cr> select regexp_matches('alcatraz', '(a(.+)z)')[2] as second_group;
    +--------------+
    | second_group |
    +--------------+
    | lcatra       |
    +--------------+
    SELECT 1 row in set (... sec)


.. _table-functions-regexp-matches-flags:

Flags
.....

This function takes a number of flags as optional third parameter. These flags
are given as a string containing any of the characters listed below. Order does
not matter.

+-------+---------------------------------------------------------------------+
| Flag  | Description                                                         |
+=======+=====================================================================+
| ``i`` | enable case insensitive matching                                    |
+-------+---------------------------------------------------------------------+
| ``u`` | enable unicode case folding when used together with ``i``           |
+-------+---------------------------------------------------------------------+
| ``U`` | enable unicode support for character classes like ``\W``            |
+-------+---------------------------------------------------------------------+
| ``s`` | make ``.`` match line terminators, too                              |
+-------+---------------------------------------------------------------------+
| ``m`` | make ``^`` and ``$`` match on the beginning or end of a line        |
|       | too.                                                                |
+-------+---------------------------------------------------------------------+
| ``x`` | permit whitespace and line comments starting with ``#``             |
+-------+---------------------------------------------------------------------+
| ``d`` | only ``\n`` is considered a line-terminator when using ``^``, ``$`` |
|       | and ``.``                                                           |
+-------+---------------------------------------------------------------------+
| ``g`` | keep matching until the end of ``source``, instead of stopping at   |
|       | the first match.                                                    |
+-------+---------------------------------------------------------------------+


Examples
........

In this example the ``pattern`` does not match anything in the ``source`` and
the result is an empty table:

::

    cr> select regexp_matches('foobar', '^(a(.+)z)$') as matched;
    +---------+
    | matched |
    +---------+
    +---------+
    SELECT 0 rows in set (... sec)

In this example we find the term that follows two digits:

::

    cr> select regexp_matches('99 bottles of beer on the wall', '\d{2}\s(\w+).*', 'ixU')
    ... as matched;
    +-------------+
    | matched     |
    +-------------+
    | ["bottles"] |
    +-------------+
    SELECT 1 row in set (... sec)

This example shows the use of flag ``g``, splitting ``source`` into a set of
arrays, each containing two entries:

::

    cr>  select regexp_matches('#abc #def #ghi #jkl', '(#[^\s]*) (#[^\s]*)', 'g') as matched;
    +------------------+
    | matched          |
    +------------------+
    | ["#abc", "#def"] |
    | ["#ghi", "#jkl"] |
    +------------------+
    SELECT 2 rows in set (... sec)


.. _pg_catalog.pg_get_keywords:

``pg_catalog.pg_get_keywords()``
================================

Returns a list of SQL keywords and their categories.

The result rows have three columns:

.. list-table::
    :header-rows: 1

    * - Column name
      - Description
    * - ``word``
      - The SQL keyword
    * - ``catcode``
      - Code for the category (`R` for reserved keywords, `U` for unreserved
        keywords)
    * - ``catdesc``
      - The description of the category

::

    cr> SELECT * FROM pg_catalog.pg_get_keywords() ORDER BY 1 LIMIT 4;
    +----------+---------+------------+
    | word     | catcode | catdesc    |
    +----------+---------+------------+
    | absolute | U       | unreserved |
    | add      | R       | reserved   |
    | alias    | U       | unreserved |
    | all      | R       | reserved   |
    +----------+---------+------------+
    SELECT 4 rows in set (... sec)


.. _information_schema._pg_expandarray:

``information_schema._pg_expandarray(array)``
=============================================

Takes an array and returns a set of value and an index into the array.

.. list-table::
    :header-rows: 1

    * - Column name
      - Description
    * - x
      - Value within the array
    * - n
      - Index of the value within the array

::

    cr> SELECT information_schema._pg_expandarray(ARRAY['a', 'b']) AS result;
    +----------+
    | result   |
    +----------+
    | ["a", 1] |
    | ["b", 2] |
    +----------+
    SELECT 2 rows in set (... sec)

::

    cr> SELECT * from information_schema._pg_expandarray(ARRAY['a', 'b']);
    +---+---+
    | x | n |
    +---+---+
    | a | 1 |
    | b | 2 |
    +---+---+
    SELECT 2 rows in set (... sec)
