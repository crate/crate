.. highlight:: psql

.. _window-functions:

================
Window functions
================

Window functions are :ref:`functions <gloss-function>` which perform a
computation across a set of rows which are related to the current row. This is
comparable to :ref:`aggregation functions <aggregation-functions>`, but window
functions do not cause multiple rows to be grouped into a single row.


.. rubric:: Table of contents

.. contents::
   :local:


.. _window-function-call:

Window function call
====================


.. _window-call-synopsis:

Synopsis
--------

The synopsis of a window function call is one of the following

::

   function_name ( { * | [ expression [, expression ... ] ] } )
                 [ FILTER ( WHERE condition ) ]
                 [ { RESPECT | IGNORE } NULLS ]
                 over_clause

where ``function_name`` is a name of a :ref:`general-purpose window
<window-functions-general-purpose>` or :ref:`aggregate function
<aggregation-functions>` and ``expression`` is a column reference, :ref:`scalar
function <scalar-functions>` or literal.

If ``FILTER`` is specified, then only the rows that met the :ref:`WHERE
<sql-select-where>` condition are supplied to the window function. Only window
functions that are :ref:`aggregates <aggregation>` accept the ``FILTER``
clause.

If ``IGNORE NULLS`` option is specified, then the null values are excluded from
the window function executions. The window functions that support this option
are: :ref:`window-functions-lead`, :ref:`window-functions-lag`,
:ref:`window-functions-first-value`, :ref:`window-functions-last-value`,
and :ref:`window-functions-nth-value`. If a function supports this option and
it is not specified, then ``RESPECT NULLS`` is set by default.

The :ref:`window-definition-over` clause is what declares a function to be a
window function.

The window function call that uses a ``wildcard`` instead of an ``expression``
as a function argument is supported only by the ``count(*)`` aggregate
function.


.. _window-definition:

Window definition
=================


.. _window-definition-over:

OVER
----

.. _window-definition-over-synopsis:

Synopsis
........

::

   OVER { window_name | ( [ window_definition ] ) }

where ``window_definition`` has the syntax

::

   window_definition:
      [ window_name ]
      [ PARTITION BY expression [, ...] ]
      [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
      [ { RANGE | ROWS } BETWEEN frame_start AND frame_end ]

The ``window_name`` refers to ``window_definition`` defined in the
:ref:`WINDOW <sql-select-window>` clause.

The ``frame_start`` and ``frame_end`` can be one of

::

   UNBOUNDED PRECEDING
   offset PRECEDING
   CURRENT ROW
   offset FOLLOWING
   UNBOUNDED FOLLOWING

The default frame definition is ``RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW``. If ``frame_end`` is omitted it defaults to ``CURRENT ROW``.

``frame_start`` cannot be ``FOLLOWING`` or ``UNBOUNDED FOLLOWING`` and
``frame_end`` cannot be ``PRECEDING`` or ``UNBOUNDED PRECEDING``.

In ``RANGE`` mode if the ``frame_start`` is ``CURRENT ROW`` the frame starts
with the current row's first peer (a row that the window's ``ORDER BY``
:ref:`expression <gloss-expression>` sorts as equal to the current row), while
a ``frame_end`` of ``CURRENT ROW`` means the frame will end with the current's
row last peer row.

In ``ROWS`` mode ``CURRENT_ROW`` means the current row.

The ``offset PRECEDING`` and ``offset FOLLOWING`` options vary in meaning
depending on the frame mode. In ``ROWS`` mode, the ``offset`` is an integer
indicating that the frame start or end is offsetted by that many rows before or
after the current row. In ``RANGE`` mode, the use of a custom ``offset`` option
requires that there is exactly one ``ORDER BY`` column in the window
definition. The frame contains those rows whose ordering column value is no
more than ``offset`` minus (for ``PRECEDING``) or plus (for ``FOLLOWING``) the
current row's ordering column value. Because the value of ``offset`` is
substracted/added to the values of the ordering column, only type combinations
that support addition/substraction operations are allowed. For instance, when
the ordering column is of type :ref:`timestamp <type-timestamp>`, the
``offset`` expression can be an :ref:`interval <type-interval>`.

The :ref:`window-definition-over` clause defines the ``window`` containing the
appropriate rows which will take part in the ``window function`` computation.

An empty :ref:`window-definition-over` clause defines a ``window`` containing
all the rows in the result set.

Example::

   cr> SELECT dept_id, COUNT(*) OVER() AS cnt FROM employees ORDER BY 1, 2;
   +---------+-----+
   | dept_id | cnt |
   +---------+-----+
   |    4001 |  18 |
   |    4001 |  18 |
   |    4001 |  18 |
   |    4002 |  18 |
   |    4002 |  18 |
   |    4002 |  18 |
   |    4002 |  18 |
   |    4003 |  18 |
   |    4003 |  18 |
   |    4003 |  18 |
   |    4003 |  18 |
   |    4003 |  18 |
   |    4004 |  18 |
   |    4004 |  18 |
   |    4004 |  18 |
   |    4006 |  18 |
   |    4006 |  18 |
   |    4006 |  18 |
   +---------+-----+
   SELECT 18 rows in set (... sec)

The ``PARTITION BY`` clause groups the rows within a window into
partitions which are processed separately by the window function, each
partition in turn becoming a window. If ``PARTITION BY`` is not specified, all
the rows are considered a single partition.

Example::

   cr> SELECT dept_id, ROW_NUMBER() OVER(PARTITION BY dept_id) AS row_num
   ... FROM employees ORDER BY 1, 2;
   +---------+---------+
   | dept_id | row_num |
   +---------+---------+
   |    4001 |       1 |
   |    4001 |       2 |
   |    4001 |       3 |
   |    4002 |       1 |
   |    4002 |       2 |
   |    4002 |       3 |
   |    4002 |       4 |
   |    4003 |       1 |
   |    4003 |       2 |
   |    4003 |       3 |
   |    4003 |       4 |
   |    4003 |       5 |
   |    4004 |       1 |
   |    4004 |       2 |
   |    4004 |       3 |
   |    4006 |       1 |
   |    4006 |       2 |
   |    4006 |       3 |
   +---------+---------+
   SELECT 18 rows in set (... sec)

If ``ORDER BY`` is supplied the ``window`` definition consists of a range of
rows starting with the first row in the ``partition`` and ending with the
current row, plus any subsequent rows that are equal to the current row, which
are the current row's ``peers``.

Example::

   cr> SELECT
   ...   dept_id,
   ...   sex,
   ...   COUNT(*) OVER(PARTITION BY dept_id ORDER BY sex) AS cnt
   ... FROM employees
   ... ORDER BY 1, 2, 3
   +---------+-----+-----+
   | dept_id | sex | cnt |
   +---------+-----+-----+
   |    4001 | M   |   3 |
   |    4001 | M   |   3 |
   |    4001 | M   |   3 |
   |    4002 | F   |   1 |
   |    4002 | M   |   4 |
   |    4002 | M   |   4 |
   |    4002 | M   |   4 |
   |    4003 | M   |   5 |
   |    4003 | M   |   5 |
   |    4003 | M   |   5 |
   |    4003 | M   |   5 |
   |    4003 | M   |   5 |
   |    4004 | F   |   1 |
   |    4004 | M   |   3 |
   |    4004 | M   |   3 |
   |    4006 | F   |   1 |
   |    4006 | M   |   3 |
   |    4006 | M   |   3 |
   +---------+-----+-----+
   SELECT 18 rows in set (... sec)

.. NOTE::

   Taking into account the ``peers`` concept mentioned above, for an empty
   :ref:`window-definition-over` clause all the rows in the result set are
   ``peers``.

.. NOTE::

   :ref:`Aggregation functions <aggregation>` will be treated as ``window
   functions`` when used in conjunction with the :ref:`window-definition-over`
   clause.

.. NOTE::

   Window definitions order or partitioned by an array column type are
   currently not supported.

In the ``UNBOUNDED FOLLOWING`` case the ``window`` for each row starts with
each row and ends with the last row in the current ``partition``. If the
``current row`` has ``peers`` the ``window`` will include (or start with) all
the ``current row`` peers and end at the upper bound of the ``partition``.

Example::

   cr> SELECT
   ...   dept_id,
   ...   sex,
   ...   COUNT(*) OVER(
   ...     PARTITION BY dept_id
   ...     ORDER BY
   ...       sex RANGE BETWEEN CURRENT ROW
   ...       AND UNBOUNDED FOLLOWING
   ...   ) partitionByDeptOrderBySex
   ... FROM employees
   ... ORDER BY 1, 2, 3
   +---------+-----+---------------------------+
   | dept_id | sex | partitionbydeptorderbysex |
   +---------+-----+---------------------------+
   |    4001 | M   |                         3 |
   |    4001 | M   |                         3 |
   |    4001 | M   |                         3 |
   |    4002 | F   |                         4 |
   |    4002 | M   |                         3 |
   |    4002 | M   |                         3 |
   |    4002 | M   |                         3 |
   |    4003 | M   |                         5 |
   |    4003 | M   |                         5 |
   |    4003 | M   |                         5 |
   |    4003 | M   |                         5 |
   |    4003 | M   |                         5 |
   |    4004 | F   |                         3 |
   |    4004 | M   |                         2 |
   |    4004 | M   |                         2 |
   |    4006 | F   |                         3 |
   |    4006 | M   |                         2 |
   |    4006 | M   |                         2 |
   +---------+-----+---------------------------+
   SELECT 18 rows in set (... sec)


.. _window-definition-named-windows:

Named windows
-------------

It is possible to define a list of named window definitions that can be
referenced in :ref:`window-definition-over` clauses. To do this, use the
:ref:`sql-select-window` clause in the :ref:`sql-select` clause.

Named windows are particularly useful when the same window definition
could be used in multiple :ref:`window-definition-over` clauses. For instance

::

   cr> SELECT
   ...   x,
   ...   FIRST_VALUE(x) OVER (w) AS "first",
   ...   LAST_VALUE(x) OVER (w) AS "last"
   ... FROM (VALUES (1), (2), (3), (4)) AS t(x)
   ... WINDOW w AS (ORDER BY x)
   +---+-------+------+
   | x | first | last |
   +---+-------+------+
   | 1 |     1 |    1 |
   | 2 |     1 |    2 |
   | 3 |     1 |    3 |
   | 4 |     1 |    4 |
   +---+-------+------+
   SELECT 4 rows in set (... sec)

If a ``window_name`` is specified in the window definition of the
:ref:`window-definition-over` clause, then there must be a named window entry
that matches the ``window_name`` in the window definition list of the
:ref:`sql-select-window` clause.

If the :ref:`window-definition-over` clause has its own non-empty window
definition and references a window definition from the :ref:`sql-select-window`
clause, then it can only add clauses from the referenced window, but not
overwrite them.

::

   cr> SELECT
   ...   x,
   ...   LAST_VALUE(x) OVER (w ORDER BY x) AS y
   ... FROM (VALUES
   ...      (1, 1),
   ...      (2, 1),
   ...      (3, 2),
   ...      (4, 2) ) AS t(x, y)
   ... WINDOW w AS (PARTITION BY y)
   +---+---+
   | x | y |
   +---+---+
   | 1 | 1 |
   | 2 | 2 |
   | 3 | 3 |
   | 4 | 4 |
   +---+---+
   SELECT 4 rows in set (... sec)

Otherwise, an attempt to override the clauses of the referenced window by the
window definition of the :ref:`window-definition-over` clause will result in
failure.

::

   cr> SELECT
   ...   FIRST_VALUE(x) OVER (w ORDER BY x)
   ... FROM (VALUES(1), (2), (3), (4)) as t(x)
   ... WINDOW w AS (ORDER BY x)
   SQLParseException[Cannot override ORDER BY clause of window w]

It is not possible to define the ``PARTITION BY`` clause in the window
definition of the :ref:`window-definition-over` clause if it references a
window definition from the :ref:`sql-select-window` clause.

The window definitions in the :ref:`sql-select-window` clause cannot define
its own window frames, if they are referenced by non-empty window definitions
of the :ref:`window-definition-over` clauses.

The definition of the named window can itself begin with a ``window_name``.  In
this case all the elements of interconnected named windows will be copied to
the window definition of the :ref:`window-definition-over` clause if it
references the named window definition that has subsequent window
references. The window definitions in the ``WINDOW`` clause permits only
backward references.

::

   cr> SELECT
   ...   x,
   ...   ROW_NUMBER() OVER (w) AS y
   ... FROM (VALUES
   ...      (1, 1),
   ...      (3, 2),
   ...      (2, 1)) AS t (x, y)
   ... WINDOW p AS (PARTITION BY y),
   ...        w AS (p ORDER BY x)
   +---+---+
   | x | y |
   +---+---+
   | 1 | 1 |
   | 2 | 2 |
   | 3 | 1 |
   +---+---+
   SELECT 3 rows in set (... sec)


.. _window-functions-general-purpose:

General-purpose window functions
================================


``row_number()``
----------------

Returns the number of the current row within its window.

Example::

   cr> SELECT
   ...  col1,
   ...  ROW_NUMBER() OVER(ORDER BY col1) as row_num
   ... FROM (VALUES('x'), ('y'), ('z')) AS t;
   +------+---------+
   | col1 | row_num |
   +------+---------+
   | x    |       1 |
   | y    |       2 |
   | z    |       3 |
   +------+---------+
   SELECT 3 rows in set (... sec)


.. _window-functions-first-value:

``first_value(arg)``
--------------------

Returns the argument value :ref:`evaluated <gloss-evaluation>` at the first row
within the window.

Its return type is the type of its argument.

Example::

   cr> SELECT
   ...  col1,
   ...  FIRST_VALUE(col1) OVER (ORDER BY col1) AS value
   ... FROM (VALUES('x'), ('y'), ('y'), ('z')) AS t;
   +------+-------+
   | col1 | value |
   +------+-------+
   | x    | x     |
   | y    | x     |
   | y    | x     |
   | z    | x     |
   +------+-------+
   SELECT 4 rows in set (... sec)


.. _window-functions-last-value:

``last_value(arg)``
-------------------

Returns the argument value :ref:`evaluated <gloss-evaluation>` at the last row
within the window.

Its return type is the type of its argument.

Example::

   cr> SELECT
   ...  col1,
   ...  LAST_VALUE(col1) OVER(ORDER BY col1) AS value
   ... FROM (VALUES('x'), ('y'), ('y'), ('z')) AS t;
   +------+-------+
   | col1 | value |
   +------+-------+
   | x    | x     |
   | y    | y     |
   | y    | y     |
   | z    | z     |
   +------+-------+
   SELECT 4 rows in set (... sec)


.. _window-functions-nth-value:

``nth_value(arg, number)``
--------------------------

Returns the argument value :ref:`evaluated <gloss-evaluation>` at row that is
the nth row within the window. ``NULL`` is returned if the nth row doesn't
exist in the window.

Its return type is the type of its first argument.

Example::

   cr> SELECT
   ...  col1,
   ...  NTH_VALUE(col1, 3) OVER(ORDER BY col1) AS val
   ... FROM (VALUES ('x'), ('y'), ('y'), ('z')) AS t;
   +------+------+
   | col1 | val  |
   +------+------+
   | x    | NULL |
   | y    | y    |
   | y    | y    |
   | z    | y    |
   +------+------+
   SELECT 4 rows in set (... sec)


.. _window-functions-lag:

``lag(arg [, offset [, default] ])``
------------------------------------


.. _window-functions-lag-synopsis:

Synopsis
........

::

   lag(argument any [, offset integer [, default any]])

Returns the argument value :ref:`evaluated <gloss-evaluation>` at the row that
precedes the current row by the offset within the partition. If there is no
such row, the return value is ``default``. If ``offset`` or ``default``
arguments are missing, they default to ``1`` and ``null``, respectively.

Both ``offset`` and ``default`` are evaluated with respect to the current row.

If ``offset`` is ``0``, then argument value is evaluated for the current row.

The ``default`` and ``argument`` data types must match.

Example::

   cr> SELECT
   ...   dept_id,
   ...   year,
   ...   budget,
   ...   LAG(budget) OVER(
   ...      PARTITION BY dept_id) prev_budget
   ... FROM (VALUES
   ...      (1, 2017, 45000),
   ...      (1, 2018, 35000),
   ...      (2, 2017, 15000),
   ...      (2, 2018, 65000),
   ...      (2, 2019, 12000))
   ... as t (dept_id, year, budget);
   +---------+------+--------+-------------+
   | dept_id | year | budget | prev_budget |
   +---------+------+--------+-------------+
   |       1 | 2017 |  45000 |        NULL |
   |       1 | 2018 |  35000 |       45000 |
   |       2 | 2017 |  15000 |        NULL |
   |       2 | 2018 |  65000 |       15000 |
   |       2 | 2019 |  12000 |       65000 |
   +---------+------+--------+-------------+
   SELECT 5 rows in set (... sec)


.. _window-functions-lead:

``lead(arg [, offset [, default] ])``
-------------------------------------


.. _window-functions-lead-synopsis:

Synopsis
........

::

   lead(argument any [, offset integer [, default any]])

The ``lead`` function is the counterpart of the :ref:`lag window function
<window-functions-lag>` as it allows the :ref:`evaluation <gloss-evaluation>`
of the argument at rows that follow the current row. ``lead`` returns the
argument value evaluated at the row that follows the current row by the offset
within the partition. If there is no such row, the return value is ``default``.
If ``offset`` or ``default`` arguments are missing, they default to ``1`` or
``null``, respectively.

Both ``offset`` and ``default`` are evaluated with respect to the current row.

If ``offset`` is ``0``, then argument value is evaluated for the current row.

The ``default`` and ``argument`` data types must match.

Example::

   cr> SELECT
   ...   dept_id,
   ...   year,
   ...   budget,
   ...   LEAD(budget) OVER(
   ...      PARTITION BY dept_id) next_budget
   ... FROM (VALUES
   ...      (1, 2017, 45000),
   ...      (1, 2018, 35000),
   ...      (2, 2017, 15000),
   ...      (2, 2018, 65000),
   ...      (2, 2019, 12000))
   ... as t (dept_id, year, budget);
   +---------+------+--------+-------------+
   | dept_id | year | budget | next_budget |
   +---------+------+--------+-------------+
   |       1 | 2017 |  45000 |       35000 |
   |       1 | 2018 |  35000 |        NULL |
   |       2 | 2017 |  15000 |       65000 |
   |       2 | 2018 |  65000 |       12000 |
   |       2 | 2019 |  12000 |        NULL |
   +---------+------+--------+-------------+
   SELECT 5 rows in set (... sec)


.. _window-functions-rank:

``rank()``
----------


.. _window-functions-rank-synopsis:

Synopsis
........

::

    rank()

Returns the rank of every row within a partition of a result set.

Within each partition, the rank of the first row is ``1``. Subsequent tied
rows are given the same rank, and the potential rank of the next row
is incremented. Because of this, ranks may not be sequential.

Example::

    cr> SELECT
    ...   name,
    ...   department_id,
    ...   salary,
    ...   RANK() OVER (ORDER BY salary desc) as salary_rank
    ... FROM (VALUES
    ...      ('Bobson Dugnutt', 1, 2000),
    ...      ('Todd Bonzalez', 2, 2500),
    ...      ('Jess Brewer', 1, 2500),
    ...      ('Safwan Buchanan', 1, 1900),
    ...      ('Hal Dodd', 1, 2500),
    ...      ('Gillian Hawes', 2, 2000))
    ... as t (name, department_id, salary);
    +-----------------+---------------+--------+-------------+
    | name            | department_id | salary | salary_rank |
    +-----------------+---------------+--------+-------------+
    | Todd Bonzalez   |             2 |   2500 |           1 |
    | Jess Brewer     |             1 |   2500 |           1 |
    | Hal Dodd        |             1 |   2500 |           1 |
    | Bobson Dugnutt  |             1 |   2000 |           4 |
    | Gillian Hawes   |             2 |   2000 |           4 |
    | Safwan Buchanan |             1 |   1900 |           6 |
    +-----------------+---------------+--------+-------------+
    SELECT 6 rows in set (... sec)


.. _window-functions-dense-rank:

``dense_rank()``
----------------


.. _window-functions-dense-rank-synopsis:

Synopsis
........

::

    dense_rank()

Returns the rank of every row within a partition of a result set, similar to
``rank``. However, unlike ``rank``, ``dense_rank`` always returns sequential
rank values.

Within each partition, the rank of the first row is ``1``. Subsequent tied
rows are given the same rank.

Example::

    cr> SELECT
    ...   name,
    ...   department_id,
    ...   salary,
    ...   DENSE_RANK() OVER (ORDER BY salary desc) as salary_rank
    ... FROM (VALUES
    ...      ('Bobson Dugnutt', 1, 2000),
    ...      ('Todd Bonzalez', 2, 2500),
    ...      ('Jess Brewer', 1, 2500),
    ...      ('Safwan Buchanan', 1, 1900),
    ...      ('Hal Dodd', 1, 2500),
    ...      ('Gillian Hawes', 2, 2000))
    ... as t (name, department_id, salary);
    +-----------------+---------------+--------+-------------+
    | name            | department_id | salary | salary_rank |
    +-----------------+---------------+--------+-------------+
    | Todd Bonzalez   |             2 |   2500 |           1 |
    | Jess Brewer     |             1 |   2500 |           1 |
    | Hal Dodd        |             1 |   2500 |           1 |
    | Bobson Dugnutt  |             1 |   2000 |           2 |
    | Gillian Hawes   |             2 |   2000 |           2 |
    | Safwan Buchanan |             1 |   1900 |           3 |
    +-----------------+---------------+--------+-------------+
    SELECT 6 rows in set (... sec)


.. _window-aggregate-functions:

Aggregate window functions
==========================

See :ref:`aggregation`.
