.. highlight:: psql
.. _aggregation:

===========
Aggregation
===========

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

An *aggregate function* computes a single result from a set of input values.

::

   cr> SELECT count(*)
   ... FROM locations;
   +----------+
   | count(*) |
   +----------+
   |       13 |
   +----------+
   SELECT 1 row in set (... sec)

In the example above the ``count(*)`` aggregate function computes the result
across all rows.

Aggregate functions can be used with the :ref:`sql_dql_group_by` clause of
the :ref:`sql_reference_select` statement. If so, an aggregate function computes
a single result per each group of input values produced by a query.

::

   cr> SELECT kind, count(*)
   ... FROM locations
   ... GROUP BY kind;
   +-------------+----------+
   | kind        | count(*) |
   +-------------+----------+
   | Galaxy      |        4 |
   | Star System |        4 |
   | Planet      |        5 |
   +-------------+----------+
   SELECT 3 rows in set (... sec)

For a tabulated summary of aggregate functions, see :ref:`sql_dql_aggregation`.

.. _aggregate-expressions:

Aggregate Expressions
=====================

An *aggregate expression* represents the application of an aggregate function
across rows selected by a query. Besides the function signature, expressions
might contain supplementary clauses and keywords.

Synopsis
--------

The synopsis of an aggregate expression is one of the following

::

   aggregate_function ( * ) [ FILTER ( WHERE condition ) ]
   aggregate_function ( [ DISTINCT ] expression [ , ... ] ) [ FILTER ( WHERE condition ) ]

where ``aggregate_function`` is a name of an
:ref:`aggregate function <aggregate-functions>` 
and ``expression`` is a column reference, scalar function or literal.

If ``FILTER`` is specified, then only the rows that met the
:ref:`sql_dql_where_clause` condition are supplied to the aggregate function.

The optional ``DISTINCT`` keyword is only supported by aggregate functions
that explicitly mention its support. Please refer to existing
:ref:`limitations <aggregation-limitations>` for further information.

The aggregate expression form that uses a ``wildcard`` instead of an
``expression`` as a function argument is supported only by the ``count(*)``
aggregate function.

.. _aggregate-functions:

Aggregate functions
===================

``count``
---------

.. _aggregation-count-star:

``count(*)``
~~~~~~~~~~~~

This aggregate function simply returns the number of rows that match the
query.

``count(columName)`` is also possible, but currently only works on a primary key
column. The semantics are the same.

The return value is always of type ``bigint``.

::

    cr> select count(*) from locations;
    +----------+
    | count(*) |
    +----------+
    | 13       |
    +----------+
    SELECT 1 row in set (... sec)

``count(*)`` can also be used on group by queries::

    cr> select count(*), kind from locations group by kind order by kind asc;
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    | 4        | Galaxy      |
    | 5        | Planet      |
    | 4        | Star System |
    +----------+-------------+
    SELECT 3 rows in set (... sec)

``count(columnName)``
~~~~~~~~~~~~~~~~~~~~~

In contrast to the :ref:`aggregation-count-star` function the ``count``
function used with a column name as parameter will return the number of rows
with a non-``NULL`` value in that column.

Example::

    cr> select count(name), count(*), date from locations group by date
    ... order by count(name) desc, count(*) desc;
    +-------------+----------+---------------+
    | count(name) | count(*) | date          |
    +-------------+----------+---------------+
    | 7           | 8        | 1373932800000 |
    | 4           | 4        | 308534400000  |
    | 1           | 1        | 1367366400000 |
    +-------------+----------+---------------+
    SELECT 3 rows in set (... sec)

``count(distinct columnName)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``count`` aggregate function also supports the ``distinct`` keyword. This
keyword changes the behaviour of the function so that it will only count the
number of distinct values in this column that are not ``NULL``::

    cr> select 
    ...   count(distinct kind) AS num_kind,
    ...   count(*),
    ...   date
    ... from locations group by date
    ... order by num_kind, count(*) desc;
    +----------+----------+---------------+
    | num_kind | count(*) |          date |
    +----------+----------+---------------+
    |        1 |        1 | 1367366400000 |
    |        3 |        8 | 1373932800000 |
    |        3 |        4 |  308534400000 |
    +----------+----------+---------------+
    SELECT 3 rows in set (... sec)

::

    cr> select count(distinct kind) AS num_kind from locations;
    +----------+
    | num_kind |
    +----------+
    |        3 |
    +----------+
    SELECT 1 row in set (... sec)

``min``
-------

The ``min`` aggregate function returns the smallest value in a column that is
not ``NULL``. Its single argument is a column name and its return value is
always of the type of that column.

Example::

    cr> select min(position), kind
    ... from locations
    ... where name not like 'North %'
    ... group by kind order by min(position) asc, kind asc;
    +---------------+-------------+
    | min(position) | kind        |
    +---------------+-------------+
    | 1             | Planet      |
    | 1             | Star System |
    | 2             | Galaxy      |
    +---------------+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select min(date) from locations;
    +--------------+
    | min(date)    |
    +--------------+
    | 308534400000 |
    +--------------+
    SELECT 1 row in set (... sec)

``min`` returns ``NULL`` if the column does not contain any value but ``NULL``.
It is allowed on columns with primitive data types. On ``text`` columns it will
return the lexicographically smallest.

::

    cr> select min(name), kind from locations
    ... group by kind order by kind asc;
    +------------------------------------+-------------+
    | min(name)                          | kind        |
    +------------------------------------+-------------+
    | Galactic Sector QQ7 Active J Gamma | Galaxy      |
    |                                    | Planet      |
    | Aldebaran                          | Star System |
    +------------------------------------+-------------+
    SELECT 3 rows in set (... sec)

``max``
-------

It behaves exactly like ``min`` but returns the biggest value in a column that
is not ``NULL``.

Some Examples::

    cr> select max(position), kind from locations
    ... group by kind order by kind desc;
    +---------------+-------------+
    | max(position) | kind        |
    +---------------+-------------+
    | 4             | Star System |
    | 5             | Planet      |
    | 6             | Galaxy      |
    +---------------+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select max(position) from locations;
    +---------------+
    | max(position) |
    +---------------+
    | 6             |
    +---------------+
    SELECT 1 row in set (... sec)

::

    cr> select max(name), kind from locations
    ... group by kind order by max(name) desc;
    +-------------------+-------------+
    | max(name)         | kind        |
    +-------------------+-------------+
    | Outer Eastern Rim | Galaxy      |
    | Bartledan         | Planet      |
    | Altair            | Star System |
    +-------------------+-------------+
    SELECT 3 rows in set (... sec)

``sum``
-------

returns the sum of a set of numeric input values that are not ``NULL``.
Depending on the argument type a suitable return type is chosen. For ``real``
and ``double precison`` argument types the return type is equal to the argument
type. For ``char``, ``smallint``, ``integer`` and ``bigint`` the return type
changes to ``bigint``. If the range of ``bigint`` values (-2^64 to 2^64-1) gets
exceeded an `ArithmeticException` will be raised.

::

    cr> select sum(position), kind from locations
    ... group by kind order by sum(position) asc;
    +---------------+-------------+
    | sum(position) | kind        |
    +---------------+-------------+
    | 10            | Star System |
    | 13            | Galaxy      |
    | 15            | Planet      |
    +---------------+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select sum(position) as position_sum from locations;
    +--------------+
    | position_sum |
    +--------------+
    | 38           |
    +--------------+
    SELECT 1 row in set (... sec)

::

    cr> select sum(name), kind from locations group by kind order by sum(name) desc;
    SQLActionException[SQLParseException: Cannot cast value `North West Ripple` to type `char`]

``avg`` and ``mean``
--------------------

The ``avg`` and ``mean`` aggregate function returns the arithmetic mean, the
*average*, of all values in a column that are not ``NULL`` as a
``double precision`` value. It accepts all numeric columns and timestamp
columns as single argument. Using ``avg`` on other column types is not allowed.

Example::

    cr> select avg(position), kind from locations
    ... group by kind order by kind;
    +---------------+-------------+
    | avg(position) | kind        |
    +---------------+-------------+
    | 3.25          | Galaxy      |
    | 3.0           | Planet      |
    | 2.5           | Star System |
    +---------------+-------------+
    SELECT 3 rows in set (... sec)

``avg(distinct columnName)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``avg`` aggregate function also supports the ``distinct`` keyword. This
keyword changes the behaviour of the function so that it will only average the
number of distinct values in this column that are not ``NULL``::

    cr> select 
    ...   avg(distinct position) AS avg_pos,
    ...   count(*),
    ...   date
    ... from locations group by date
    ... order by 1 desc, count(*) desc;
    +---------+----------+---------------+
    | avg_pos | count(*) |          date |
    +---------+----------+---------------+
    |     4.0 |        1 | 1367366400000 |
    |     3.6 |        8 | 1373932800000 |
    |     2.0 |        4 |  308534400000 |
    +---------+----------+---------------+
    SELECT 3 rows in set (... sec)

::

    cr> select avg(distinct position) AS avg_pos from locations;
    +---------+
    | avg_pos |
    +---------+
    |     3.5 |
    +---------+
    SELECT 1 row in set (... sec)


.. _string_agg:

``string_agg``
--------------

::

   string_agg(text, text) -> text
   string_agg(expression, delimiter)

The ``string_agg`` aggregate function concatenates the input values into a
string, where each value is separated by a delimiter.

If all input values are null, null is returned as a result.


::

   cr> select string_agg(col1, ', ') from (values('a'), ('b'), ('c')) as t;
   +------------------------+
   | string_agg(col1, ', ') |
   +------------------------+
   | a, b, c                |
   +------------------------+
   SELECT 1 row in set (... sec)

.. _array_agg:

``array_agg``
-------------

::

    array_agg(any_non_array) -> array  

The ``array_agg`` aggregate function concatenates all input values into an
array.

::

    cr> SELECT array_agg(x) FROM (VALUES (42), (832), (null), (17)) as t (x);
    +---------------------+
    | array_agg(x)        |
    +---------------------+
    | [42, 832, null, 17] |
    +---------------------+
    SELECT 1 row in set (... sec)


``geometric_mean``
------------------

The ``geometric_mean`` aggregate function computes the geometric mean, a mean
for positive numbers. For details see: `Geometric Mean`_.

``geometric mean`` is defined on all numeric types and on timestamp. It always
returns double values. If a value is negative, all values were null or we got
no value at all ``NULL`` is returned. If any of the aggregated values is ``0``
the result will be ``0.0`` as well.

.. CAUTION::

    Due to java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

Example::

    cr> select geometric_mean(position), kind from locations
    ... group by kind order by kind;
    +--------------------------+-------------+
    | geometric_mean(position) | kind        |
    +--------------------------+-------------+
    |       2.6321480259049848 | Galaxy      |
    |       2.6051710846973517 | Planet      |
    |       2.213363839400643  | Star System |
    +--------------------------+-------------+
    SELECT 3 rows in set (... sec)

``variance``
------------

The ``variance`` aggregate function computes the `Variance`_ of the set of
non-null values in a column. It is a measure about how far a set of numbers is
spread. A variance of ``0.0`` indicates that all values are the same.

``variance`` is defined on all numeric types and on timestamp. It returns a
``double precision`` value. If all values were null or we got no value at all
``NULL`` is returned.

Example::

    cr> select variance(position), kind from locations
    ... group by kind order by kind desc;
    +--------------------+-------------+
    | variance(position) | kind        |
    +--------------------+-------------+
    |             1.25   | Star System |
    |             2.0    | Planet      |
    |             3.6875 | Galaxy      |
    +--------------------+-------------+
    SELECT 3 rows in set (... sec)

.. CAUTION::

    Due to java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

``stddev``
----------

The ``stddev`` aggregate function computes the `Standard Deviation`_ of the
set of non-null values in a column. It is a measure of the variation of data
values. A low standard deviation indicates that the values tend to be near the
mean.

``stddev`` is defined on all numeric types and on timestamp. It always returns
``double precision`` values. If all values were null or we got no value at all
``NULL`` is returned.

Example::

    cr> select stddev(position), kind from locations
    ... group by kind order by kind;
    +--------------------+-------------+
    |   stddev(position) | kind        |
    +--------------------+-------------+
    | 1.920286436967152  | Galaxy      |
    | 1.4142135623730951 | Planet      |
    | 1.118033988749895  | Star System |
    +--------------------+-------------+
    SELECT 3 rows in set (... sec)

.. CAUTION::

    Due to java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

``percentile``
--------------

The ``percentile`` aggregate function computes a `Percentile`_ over numeric
non-null values in a column.

Percentiles show the point at which a certain percentage of observed values
occur. For example, the 98th percentile is the value which is greater than 98%
of the observed values. The result is defined and computed as an interpolated
weighted average. According to that it allows the median of the input data to
be defined conveniently as the 50th percentile.

The function expects a single fraction or an array of fractions and a column
name. Independent of the input column data type the result of ``percentile``
always returns a ``double precision``. If the value at the specified column is
``null`` the row is ignored. Fractions must be double precision values between
0 and 1. When supplied a single fraction, the function will return a single
value corresponding to the percentile of the specified fraction::

    cr> select percentile(position, 0.95), kind from locations
    ... group by kind order by kind;
    +----------------------------+-------------+
    | percentile(position, 0.95) | kind        |
    +----------------------------+-------------+
    |                        6.0 | Galaxy      |
    |                        5.0 | Planet      |
    |                        4.0 | Star System |
    +----------------------------+-------------+
    SELECT 3 rows in set (... sec)

When supplied an array of fractions, the function will return an array of
values corresponding to the percentile of each fraction specified::

    cr> select percentile(position, [0.0013, 0.9987]) as perc from locations;
    +------------+
    | perc       |
    +------------+
    | [1.0, 6.0] |
    +------------+
    SELECT 1 row in set (... sec)

When a query with ``percentile`` function won't match any rows then a null
result is returned.

To be able to calculate percentiles over a huge amount of data and to scale out
CrateDB calculates approximate instead of accurate percentiles. The algorithm
used by the percentile metric is called `TDigest`_. The accuracy/size trade-off
of the algorithm is defined by a single compression parameter which has a
constant value of ``100``. However, there are a few guidelines to keep in mind
in this implementation:

    - Extreme percentiles (e.g. 99%) are more accurate
    - For small sets percentiles are highly accurate
    - It's difficult to generalize the exact level of accuracy, as it depends
      on your data distribution and volume of data being aggregated

``arbitrary``
-------------

The ``arbitrary`` aggregate function returns a single value of a column.
Which value it returns is not defined.

It accepts references to columns of all primitive types.

Using ``arbitrary`` on ``Object`` columns is not supported.

Its return type is the type of its parameter column and can be ``NULL`` if the
column contains ``NULL`` values.

Example::

    cr> select arbitrary(position) from locations;
    +---------------------+
    | arbitrary(position) |
    +---------------------+
    | ...                 |
    +---------------------+
    SELECT 1 row in set (... sec)

::

    cr> select arbitrary(name), kind from locations
    ... where name != ''
    ... group by kind order by kind desc;
    +-...-------------+-------------+
    | arbitrary(name) | kind        |
    +-...-------------+-------------+
    | ...             | Star System |
    | ...             | Planet      |
    | ...             | Galaxy      |
    +-...-------------+-------------+
    SELECT 3 rows in set (... sec)

An example use case is to group a table with many rows per user by ``user_id``
and get the ``username`` for every group, that means every user. This works as
rows with same ``user_id`` have the same ``username``.  This method performs
better than grouping on ``username`` as grouping on number types is generally
faster than on strings.  The advantage is that the ``arbitrary`` function does
very little to no computation as for example ``max`` aggregate function would
do.

.. _aggregation-hll-distinct:

``hyperloglog_distinct``
------------------------

.. note::

   The ``hyperloglog_distinct`` aggregate function is an :ref:`enterprise
   feature <enterprise-features>`.

The ``hyperloglog_distinct`` aggregate function calculates an approximate count
of distinct non-null values using the `HyperLogLog++`_ algorithm.

The return value data type is always a ``bigint``.

The first argument can be a reference to a column of all
:ref:`sql_ddl_datatypes_primitives`. :ref:`sql_ddl_datatypes_compound` and
:ref:`sql_ddl_datatypes_geographic` are not supported.

The optional second argument defines the used ``precision`` for the
`HyperLogLog++`_ algorithm. This allows to trade memory for accuracy, valid
values are ``4`` to ``18``. A precision of ``4`` uses approximately ``16``
bytes of memory. Each increase in precision doubles the memory requirement. So
precision ``5`` uses approximately ``32`` bytes, up to ``262144`` bytes for
precision ``18``.

The default value for the ``precision`` which is used if the second argument is
left out is ``14``.


Examples::

    cr> select hyperloglog_distinct(position) from locations;
    +--------------------------------+
    | hyperloglog_distinct(position) |
    +--------------------------------+
    | 6                              |
    +--------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select hyperloglog_distinct(position, 4) from locations;
    +-----------------------------------+
    | hyperloglog_distinct(position, 4) |
    +-----------------------------------+
    | 6                                 |
    +-----------------------------------+
    SELECT 1 row in set (... sec)

.. _aggregation-limitations:

Limitations
===========

 - ``DISTINCT`` is not supported with aggregations on :ref:`sql_joins`.
 - Aggregate functions can only be applied to columns with a plain index,
   which is the default for all :ref:`primitive type
   <sql_ddl_datatypes_primitives>` columns. For more information, please refer
   to :ref:`sql_ddl_index_plain`.

.. _Geometric Mean: https://en.wikipedia.org/wiki/Mean#Geometric_mean_.28GM.29
.. _Variance: https://en.wikipedia.org/wiki/Variance
.. _Standard Deviation: https://en.wikipedia.org/wiki/Standard_deviation
.. _Percentile: https://en.wikipedia.org/wiki/Percentile
.. _TDigest: https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
