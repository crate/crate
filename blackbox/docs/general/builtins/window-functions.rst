.. highlight:: psql
.. _window-functions:

================
Window functions
================

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

Window functions are functions which perform a computation across a set of rows
which are related to the current row. This is comparable to aggregation
functions, but window functions do not cause multiple rows to be grouped
into a single row.

.. _window-definition:

Window Definition
=================

.. _over:

OVER
----

Synopsis
........

::

   OVER (
      [ PARTITION BY expression [, ...] ]
      [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
      [ { RANGE | ROWS } BETWEEN frame_start AND frame_end ]
   )

where ``frame_start`` and ``frame_end`` can be one of

::

   UNBOUNDED PRECEDING
   CURRENT ROW
   UNBOUNDED FOLLOWING

The default frame definition is ``RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW``. If ``frame_end`` is omitted it defaults to ``CURRENT ROW``.

``frame_start`` cannot be ``UNBOUNDED FOLLOWING`` and ``frame_end`` cannot be
``UNBOUNDED PRECEDING``.

In ``RANGE`` mode if the ``frame_start`` is ``CURRENT ROW`` the frame starts
with the current row's first peer (a row that the window's ``ORDER BY``
expression sorts as equal to the current row), while a ``frame_end`` of
``CURRENT ROW`` means the frame will end with the current's row last peer row.

In ``ROWS`` mode ``CURRENT_ROW`` means the current row.

The ``OVER`` clause defines the ``window`` containing the appropriate rows
which will take part in the ``window function`` computation.

An empty ``OVER`` clause defines a ``window`` containing all the rows in the
result set.

Example::

   cr> SELECT dept_id, COUNT(*) OVER() FROM employees ORDER BY 1, 2;
   +---------+------------------+
   | dept_id | count(*) OVER () |
   +---------+------------------+
   |    4001 |               18 |
   |    4001 |               18 |
   |    4001 |               18 |
   |    4002 |               18 |
   |    4002 |               18 |
   |    4002 |               18 |
   |    4002 |               18 |
   |    4003 |               18 |
   |    4003 |               18 |
   |    4003 |               18 |
   |    4003 |               18 |
   |    4003 |               18 |
   |    4004 |               18 |
   |    4004 |               18 |
   |    4004 |               18 |
   |    4006 |               18 |
   |    4006 |               18 |
   |    4006 |               18 |
   +---------+------------------+
   SELECT 18 rows in set (... sec)

The ``PARTITION BY`` clause groups the rows within a window into
partitions which are processed separately by the window function, each
partition in turn becoming a window. If ``PARTITION BY`` is not specified, all
the rows are considered a single partition.

Example::

   cr> SELECT dept_id, ROW_NUMBER() OVER(PARTITION BY dept_id) FROM employees ORDER BY 1, 2;
   +---------+------------------------------------------+
   | dept_id | row_number() OVER (PARTITION BY dept_id) |
   +---------+------------------------------------------+
   |    4001 |                                        1 |
   |    4001 |                                        2 |
   |    4001 |                                        3 |
   |    4002 |                                        1 |
   |    4002 |                                        2 |
   |    4002 |                                        3 |
   |    4002 |                                        4 |
   |    4003 |                                        1 |
   |    4003 |                                        2 |
   |    4003 |                                        3 |
   |    4003 |                                        4 |
   |    4003 |                                        5 |
   |    4004 |                                        1 |
   |    4004 |                                        2 |
   |    4004 |                                        3 |
   |    4006 |                                        1 |
   |    4006 |                                        2 |
   |    4006 |                                        3 |
   +---------+------------------------------------------+
   SELECT 18 rows in set (... sec)

If ``ORDER BY`` is supplied the ``window`` definition consists of a range of
rows starting with the first row in the ``partition`` and ending with the
current row, plus any subsequent rows that are equal to the current row, which
are the current row's ``peers``.

Example::

   cr> SELECT
   ...   dept_id,
   ...   sex,
   ...   COUNT(*) OVER(PARTITION BY dept_id ORDER BY sex)
   ... FROM employees
   ... ORDER BY 1, 2, 3
   +---------+-----+---------------------------------------------------------+
   | dept_id | sex | count(*) OVER (PARTITION BY dept_id ORDER BY "sex" ASC) |
   +---------+-----+---------------------------------------------------------+
   |    4001 | M   |                                                       3 |
   |    4001 | M   |                                                       3 |
   |    4001 | M   |                                                       3 |
   |    4002 | F   |                                                       1 |
   |    4002 | M   |                                                       4 |
   |    4002 | M   |                                                       4 |
   |    4002 | M   |                                                       4 |
   |    4003 | M   |                                                       5 |
   |    4003 | M   |                                                       5 |
   |    4003 | M   |                                                       5 |
   |    4003 | M   |                                                       5 |
   |    4003 | M   |                                                       5 |
   |    4004 | F   |                                                       1 |
   |    4004 | M   |                                                       3 |
   |    4004 | M   |                                                       3 |
   |    4006 | F   |                                                       1 |
   |    4006 | M   |                                                       3 |
   |    4006 | M   |                                                       3 |
   +---------+-----+---------------------------------------------------------+
   SELECT 18 rows in set (... sec)

.. note::

   Taking into account the ``peers`` concept mentioned above, for an empty
   ``OVER`` clause all the rows in the result set are ``peers``.

.. note::

   :ref:`Aggregation functions <aggregation>` will be treated as
   ``window functions`` when used in conjuction with the ``OVER`` clause.

.. note::

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

General-Purpose Window Functions
================================

``row_number()``
----------------

Returns the number of the current row within its window.

Example::

   cr> SELECT col1, ROW_NUMBER() OVER(ORDER BY col1) FROM unnest(['x','y','z']);
   +------+-----------------------------------------+
   | col1 | row_number() OVER (ORDER BY "col1" ASC) |
   +------+-----------------------------------------+
   | x    |                                       1 |
   | y    |                                       2 |
   | z    |                                       3 |
   +------+-----------------------------------------+
   SELECT 3 rows in set (... sec)

.. _window-function-firstvalue:

``first_value(arg)``
--------------------

.. note::

   The ``first_value`` window function is an :ref:`enterprise
   feature <enterprise-features>`.

Returns the argument value evaluated at the first row within the window.

Its return type is the type of its argument.

Example::

   cr> SELECT col1, FIRST_VALUE(col1) OVER(ORDER BY col1) FROM unnest(['x','y', 'y', 'z']);
   +------+----------------------------------------------+
   | col1 | first_value(col1) OVER (ORDER BY "col1" ASC) |
   +------+----------------------------------------------+
   | x    | x                                            |
   | y    | x                                            |
   | y    | x                                            |
   | z    | x                                            |
   +------+----------------------------------------------+
   SELECT 4 rows in set (... sec)

.. _window-function-lastvalue:

``last_value(arg)``
-------------------

.. note::

   The ``last_value`` window function is an :ref:`enterprise
   feature <enterprise-features>`.

Returns the argument value evaluated at the last row within the window.

Its return type is the type of its argument.

Example::

   cr> SELECT col1, LAST_VALUE(col1) OVER(ORDER BY col1) FROM unnest(['x','y', 'y', 'z']);
   +------+---------------------------------------------+
   | col1 | last_value(col1) OVER (ORDER BY "col1" ASC) |
   +------+---------------------------------------------+
   | x    | x                                           |
   | y    | y                                           |
   | y    | y                                           |
   | z    | z                                           |
   +------+---------------------------------------------+
   SELECT 4 rows in set (... sec)

.. _window-function-nthvalue:

``nth_value(arg, number)``
--------------------------

.. note::

   The ``nth_value`` window function is an :ref:`enterprise
   feature <enterprise-features>`.

Returns the argument value evaluated at row that is the nth row within the
window. Null is returned if the nth row doesn't exist in the window.

Its return type is the type of its first argument.

Example::

   cr> SELECT col1, NTH_VALUE(col1, 3) OVER(ORDER BY col1) FROM unnest(['x','y', 'y', 'z']);
   +------+-----------------------------------------------+
   | col1 | nth_value(col1, 3) OVER (ORDER BY "col1" ASC) |
   +------+-----------------------------------------------+
   | x    | NULL                                          |
   | y    | y                                             |
   | y    | y                                             |
   | z    | y                                             |
   +------+-----------------------------------------------+
   SELECT 4 rows in set (... sec)

.. _window-function-lag:

``lag(arg [, offset [, default] ])``
------------------------------------

.. note::

   The ``lag`` window function is an :ref:`enterprise feature
   <enterprise-features>`.

Synopsis
........

::

   lag(argument any [, offset integer [, default any]])

Returns the argument value evaluated at the row that precedes the current row
by the offset within the partition. If there is no such row, the return value
is ``default``. If ``offset`` or ``default`` arguments are missing, they
default to ``1`` and ``null``, respectively.

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
   ... FROM unnest(
   ...   [1, 1, 2, 2, 2],
   ...   [2017, 2018, 2017, 2018, 2019],
   ...   [45000, 35000, 15000, 65000, 12000]
   ... ) as t (dept_id, year, budget);
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

.. _window-function-lead:

``lead(arg [, offset [, default] ])``
-------------------------------------

.. note::

   The ``lead`` window function is an :ref:`enterprise feature
   <enterprise-features>`.

Synopsis
........

::

   lead(argument any [, offset integer [, default any]])

The ``lead`` function is the counterpart of the
:ref:`lag window function <window-function-lag>` as it allows the evaluation of
the argument at rows that follow the current row. ``lead`` returns the argument
value evaluated at the row that follows the current row by the offset within
the partition. If there is no such row, the return value is ``default``.
If ``offset`` or ``default`` arguments are missing, they default to ``1`` and
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
   ... FROM unnest(
   ...   [1, 1, 2, 2, 2],
   ...   [2017, 2018, 2017, 2018, 2019],
   ...   [45000, 35000, 15000, 65000, 12000]
   ... ) as t (dept_id, year, budget);
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

Aggregate Window Functions
==========================

See :ref:`aggregation`.
