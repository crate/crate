.. highlight:: psql
.. _window-functions:

================
Window Functions
================

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

Window functions are functions which perform a computation across a set of rows
which are related to the current row. This is comparable to aggregation
functions, but window functions do not cause multiple rows to be grouped
into a single row.

Window Definition
=================

The ``OVER`` clause defines the ``window`` containing the appropriate rows
which will take part in the ``window function`` computation.

An empty ``OVER`` clause defines a ``window`` containing all the rows in the
result set. 

Example::

   cr> select dept_id, count(*) OVER() from employees order by 1, 2;
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

   cr> select dept_id, row_number() OVER(PARTITION BY dept_id) from employees order by 1, 2;
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

   cr> select dept_id, sex, count(*) OVER(PARTITION BY dept_id ORDER BY sex) from employees order by 1,2,3;
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

.. _over:

OVER
----

Synopsis
........

::

   OVER (
      [ PARTITION BY expression [, ...] ]
      [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
   )

General-Purpose Window Functions
================================

``row_number()``
----------------

Returns the number of the current row within its window.

Example::

   cr> select col1, row_number() over(order by col1) from unnest(['x','y','z']);
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

   cr> select col1, first_value(col1) over(order by col1) from unnest(['x','y', 'y', 'z']);
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

   cr> select col1, last_value(col1) over(order by col1) from unnest(['x','y', 'y', 'z']);
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

   cr> select col1, nth_value(col1, 3) over(order by col1) from unnest(['x','y', 'y', 'z']);
   +------+-----------------------------------------------+
   | col1 | nth_value(col1, 3) OVER (ORDER BY "col1" ASC) |
   +------+-----------------------------------------------+
   | x    | NULL                                          |
   | y    | y                                             |
   | y    | y                                             |
   | z    | y                                             |
   +------+-----------------------------------------------+
   SELECT 4 rows in set (... sec)

Aggregate Window Functions
==========================

See :ref:`aggregation`.
