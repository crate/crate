.. highlight:: psql

.. _ref-table-functions:

===============
Table functions
===============

Table functions are functions that produce a set of rows.
They can either be used in place of a relation in the ``FROM`` clause,
or within the select list of a query.

If used within the select list, the table functions will be evaluated
per row of the relations in the ``FROM`` clause,
generating one or more rows which are appended to the result set.
If multiple table functions with different amount of rows are used, ``null``
values will be returned for the functions that are exhausted. An example::


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
    (SELECT aggregate_func(col) FROM (SELECT table_func(...) as col) ...)

.. rubric:: Table of contents

.. contents::
   :local:

.. _table-functions-scalar:

Scalar functions
================

:ref:`scalar`, when used in the ``FROM`` clause in place of a relation,
will result in a table of one row and one column, containing the value
returned from the scalar function.

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

The columns are named ``colN`` where N is a number starting at 1.

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

``generate_series(start, stop, [step])``
========================================

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
    +------+
    | col1 |
    +------+
    |    1 |
    |    2 |
    |    3 |
    |    4 |
    +------+
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


.. _pg_catalog.pg_get_keywords:


``pg_catalog.pg_get_keywords()``
================================

Returns a list of SQL keywords and their categories.

The result rows have three columns:

.. list-table::
    :header-rows: 1

    * - Column name
      - Description
    * - word
      - The SQL keyword
    * - catcode
      - Code for the category (`R` for reserved keywords, `U` for unreserved
        keywords)
    * - catdesc
      - The description of the category

::

    cr> SELECT * FROM pg_catalog.pg_get_keywords() ORDER BY 1 LIMIT 4;
    +----------+---------+------------+
    | word     | catcode | catdesc    |
    +----------+---------+------------+
    | add      | R       | reserved   |
    | alias    | U       | unreserved |
    | all      | R       | reserved   |
    | allocate | U       | unreserved |
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

    cr> SELECT information_schema._pg_expandarray(ARRAY['a', 'b']);
    +-----------------------------+
    | _pg_expandarray(['a', 'b']) |
    +-----------------------------+
    | ["a", 1]                    |
    | ["b", 2]                    |
    +-----------------------------+
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
