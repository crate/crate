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

Window definition
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

