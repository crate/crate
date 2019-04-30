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

.. _window-definition:

Window Definition
=================

The ``OVER`` clause defines the ``window`` containing the appropriate rows
which will take part in the ``window function`` computation.

An empty ``OVER`` clause defines a ``window`` containing all the rows in the
result set.

Example::

   cr> select name, avg(price) OVER() from articles order by name;
   +------------------------------+--------------------+
   | name                         | avg(price) OVER () |
   +------------------------------+--------------------+
   | Infinite Improbability Drive | 18375.317556142807 |
   | Kill-o-Zap blaster pistol    | 18375.317556142807 |
   | Starship Titanic             | 18375.317556142807 |
   | Towel                        | 18375.317556142807 |
   +------------------------------+--------------------+
   SELECT 4 rows in set (... sec)


If ``ORDER BY`` is supplied the ``window`` definition consists of a range of
rows starting with the first row in the result set and ending with the current
row, plus any subsequent rows that are equal to the current row, which are the
current row's ``peers``.


Example::

   cr> select col1, sum(col1) over (order by col1) from unnest([1, 5, 2, 3, 2, 5, 4]);
   +------+--------------------------------------+
   | col1 | sum(col1) OVER (ORDER BY "col1" ASC) |
   +------+--------------------------------------+
   |    1 |                                    1 |
   |    2 |                                    5 |
   |    2 |                                    5 |
   |    3 |                                    8 |
   |    4 |                                   12 |
   |    5 |                                   22 |
   |    5 |                                   22 |
   +------+--------------------------------------+
   SELECT 7 rows in set (... sec)

.. note::

   Taking into account the ``peers`` concept mentioned above, for an empty
   ``OVER`` clause all the rows in the result set are ``peers``.

.. note::

   :ref:`Aggregation functions <aggregation>` will be treated as
   ``window functions`` when used in conjuction with the ``OVER`` clause.

.. note::

   Window definitions order or partitioned by an array column type are
   currently not supported.

.. _over:

OVER
----

Synopsis
........

::

   OVER (
      [ ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
   )

Example::

   cr> select price, sum(price) OVER(ORDER BY price) from articles;
   +----------+----------------------------------------+
   |    price | sum(price) OVER (ORDER BY "price" ASC) |
   +----------+----------------------------------------+
   |     1.29 |                                  1.29  |
   |  3499.99 |                               3501.28  |
   | 19999.99 |                              23501.27  |
   | 50000.0  |                              73501.266 |
   +----------+----------------------------------------+
   SELECT 4 rows in set (... sec)


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
   feature <enterprise_features>`.

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
   feature <enterprise_features>`.

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
   feature <enterprise_features>`.

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
