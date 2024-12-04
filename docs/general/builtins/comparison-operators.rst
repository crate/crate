.. highlight:: psql

.. _comparison-operators:

====================
Comparison operators
====================

A comparison :ref:`operator <gloss-operator>` tests the relationship between
two values and returns a corresponding value of ``true``, ``false``, or
``NULL``.

.. rubric:: Table of contents

.. contents::
   :local:


.. _comparison-operators-basic:

Basic operators
===============

For simple :ref:`data types <data-types>`, the following basic operators can be
used:

========  ==========================
Operator  Description
========  ==========================
``<``     Less than
--------  --------------------------
``>``     Greater than
--------  --------------------------
``<=``    Less than or equal to
--------  --------------------------
``>=``    Greater than or equal to
--------  --------------------------
``=``     Equal
--------  --------------------------
``<>``    Not equal
--------  --------------------------
``!=``    Not equal (same as ``<>``)
========  ==========================

When comparing strings, a `lexicographical comparison`_ is performed::

    cr> select name from locations where name > 'Argabuthon' order by name;
    +------------------------------------+
    | name                               |
    +------------------------------------+
    | Arkintoofle Minor                  |
    | Bartledan                          |
    | Galactic Sector QQ7 Active J Gamma |
    | North West Ripple                  |
    | Outer Eastern Rim                  |
    +------------------------------------+
    SELECT 5 rows in set (... sec)

When comparing dates, `ISO date formats`_ can be used::

    cr> select date, position from locations where date <= '1979-10-12' and
    ... position < 3 order by position;
    +--------------+----------+
    | date         | position |
    +--------------+----------+
    | 308534400000 |        1 |
    | 308534400000 |        2 |
    +--------------+----------+
    SELECT 2 rows in set (... sec)

When comparing Geo Shapes, `topological comparison`_ is used.
Topological equality means that the geometries have the same dimension, and their point-sets occupy the same space.
This means that the order of vertices may be different in topologically equal geometries::

    cr> SELECT 'POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0))'::GEO_SHAPE = 'POLYGON (( 1 0, 1 1, 0 1, 0 0, 1 0))'::GEO_SHAPE as res;
    +------+
    | res  |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)

Geometry collections, containing only linestrings, points or polygons are
normalized to MultiLineString, MultiPoint and MultiPolygon. Hence, geometry
collection of points is equal to a MultiPoint with the same points set::

    cr> select 'MULTIPOINT ((10 40), (40 30), (20 20))'::GEO_SHAPE = 'GEOMETRYCOLLECTION (POINT (10 40), POINT(40 30), POINT(20 20))'::GEO_SHAPE;
    +------+
    | true |
    +------+
    | TRUE |
    +------+
    SELECT 1 row in set (... sec)

.. TIP::

    Comparison operators are commonly used to filter rows (e.g., in the
    :ref:`WHERE <sql-select-where>` and :ref:`HAVING <sql-select-having>`
    clauses of a :ref:`SELECT <sql-select>` statement). However, basic
    comparison operators can be used as :ref:`value expressions
    <sql-operator-invocation>` in any context. For example::

        cr> SELECT 1 < 10 as my_column;
        +-----------+
        | my_column |
        +-----------+
        | TRUE      |
        +-----------+
        SELECT 1 row in set (... sec)

.. _comparison-operators-where:

``WHERE`` clause operators
==========================

Within a :ref:`sql_dql_where_clause`, the following operators can also be used:

=================================  ===================================================
Operator                           Description
=================================  ===================================================
``~`` , ``~*`` , ``!~`` , ``!~*``  See :ref:`sql_dql_regexp`
---------------------------------  ---------------------------------------------------
:ref:`sql_dql_like`                Matches a part of the given value
---------------------------------  ---------------------------------------------------
:ref:`sql_dql_not`                 Negates a condition
---------------------------------  ---------------------------------------------------
:ref:`sql_dql_is_null`             Matches a null value
---------------------------------  ---------------------------------------------------
:ref:`sql_dql_is_not_null`         Matches a non-null value
---------------------------------  ---------------------------------------------------
``ip << range``                    True if IP is within the given IP range (using
                                   `CIDR notation`_)
---------------------------------  ---------------------------------------------------
``x BETWEEN y AND z``              Shortcut for ``x >= y AND x <= z``
=================================  ===================================================

.. SEEALSO::

    - :ref:`sql_array_comparisons`

    - :ref:`sql_subquery_expressions`


.. _CIDR notation: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_blocks
.. _ISO date formats: http://joda-time.sourceforge.net/api-release/org/joda/time/format/ISODateTimeFormat.html#dateOptionalTimeParser%28%29
.. _lexicographical comparison: https://lucene.apache.org/core/6_6_0/core/org/apache/lucene/search/TermRangeQuery.html
.. _topological comparison: https://postgis.net/docs/ST_Equals.html
