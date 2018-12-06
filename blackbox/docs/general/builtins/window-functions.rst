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

.. note::

   :ref:`Aggregation functions <aggregation>` will be treated as
   ``window functions`` when used in conjuction with the ``OVER`` clause.

.. _over:

OVER
----

Synopsis
........

::

   OVER ()

.. note::

   At this time only the empty ``OVER()`` clause is supported, which means the
   ``window`` for each row is the range from the first row in the result set to
   the last one.

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

