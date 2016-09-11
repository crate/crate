.. highlight:: psql
.. _sql_union:

=========
Union All
=========

UNION ALL can be used to combine results from multiple ``SELECT`` statements.
For further information on the syntax and usages see :ref:`sql_reference_union`.
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
