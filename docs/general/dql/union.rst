.. highlight:: psql

.. _sql-union:

=====
Union
=====

:ref:`UNION <sql-select-union>` can be used to combine results from
multiple :ref:`SELECT <sql-select>` statements.

.. SEEALSO::

    `Union (SQL)`_

.. rubric:: Table of contents

.. contents::
   :local:

.. _union-all:

Union All
---------

If duplicates are allowed, use ``UNION ALL``.

::

    cr> select name from photos
    ... union all
    ... select name from countries
    ... union all
    ... select name from photos
    ... order by name;
    +--------------+
    | name         |
    +--------------+
    | Austria      |
    | Berlin Wall  |
    | Berlin Wall  |
    | Eiffel Tower |
    | Eiffel Tower |
    | France       |
    | Germany      |
    | South Africa |
    | Turkey       |
    +--------------+
    SELECT 9 rows in set (... sec)

.. _union-distinct:

Union Distinct
--------------

To remove duplicates, use ``UNION DISTINCT`` or simply ``UNION``.

::

    cr> select name from photos
    ... union distinct
    ... select name from countries
    ... union
    ... select name from photos
    ... order by name;
    +--------------+
    | name         |
    +--------------+
    | Austria      |
    | Berlin Wall  |
    | Eiffel Tower |
    | France       |
    | Germany      |
    | South Africa |
    | Turkey       |
    +--------------+
    SELECT 7 rows in set (... sec)


.. _Union (SQL): https://en.wikipedia.org/wiki/Set_operations_(SQL)#UNION_operator