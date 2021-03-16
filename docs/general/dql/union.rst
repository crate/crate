.. highlight:: psql

.. _sql_union:

=========
Union All
=========

:ref:`UNION ALL <sql-select-union-all>` can be used to combine results from
multiple :ref:`SELECT <sql-select>` statements.

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
