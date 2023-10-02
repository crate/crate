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

.. _union-of-object-types:

Union of object types
---------------------

When object types are unioned, the resulting object will contain the merged
sub-columns from both of the input objects.

::

    cr> SET error_on_unknown_object_key = FALSE;
    SET OK, 0 rows affected  (... sec)
    cr> SELECT o, o['a'], o['b'] FROM (SELECT {a=1} AS o UNION SELECT {b=1} AS o) AS t;
    +----------+--------+--------+
    | o        | o['a'] | o['b'] |
    +----------+--------+--------+
    | {"a": 1} |      1 |   NULL |
    | {"b": 1} |   NULL |      1 |
    +----------+--------+--------+
    SELECT 2 rows in set (... sec)

.. _union-of-different_types:

Union of different types
------------------------

If types do not match, CrateDB will choose one of the type with higher
precedence and try to implicitly cast the values with lower precedence.

::

    cr> SELECT pg_typeof(c) FROM (SELECT 1 AS c UNION SELECT '1' AS c UNION SELECT 1 AS c) AS t;
    +-------------------------+
    | pg_catalog.pg_typeof(c) |
    +-------------------------+
    | integer                 |
    +-------------------------+
    SELECT 1 row in set (... sec)
