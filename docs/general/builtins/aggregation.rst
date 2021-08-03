.. highlight:: psql
.. _aggregation:

===========
Aggregation
===========

When :ref:`selecting data <sql_dql_aggregation>` from CrateDB, you can use an
`aggregate function`_ to calculate a single summary value for one or more
columns.

For example::

   cr> SELECT count(*) FROM locations;
   +----------+
   | count(*) |
   +----------+
   |       13 |
   +----------+
   SELECT 1 row in set (... sec)

Here, the :ref:`count(*) <aggregation-count-star>` function computes the result
across all rows.

Aggregate :ref:`functions <gloss-function>` can be used with the
:ref:`sql_dql_group_by` clause. When used like this, an aggregate function
returns a single summary value for each grouped collection of column values.

For example::

   cr> SELECT kind, count(*) FROM locations GROUP BY kind;
   +-------------+----------+
   | kind        | count(*) |
   +-------------+----------+
   | Galaxy      |        4 |
   | Star System |        4 |
   | Planet      |        5 |
   +-------------+----------+
   SELECT 3 rows in set (... sec)


.. TIP::

    Aggregation works across all the rows that match a query or on all matching
    rows in every distinct group of a ``GROUP BY`` statement. Aggregating
    ``SELECT`` statements without ``GROUP BY`` will always return one row.

.. rubric:: Table of contents

.. contents::
   :local:


.. _aggregation-expressions:

Aggregate expressions
=====================

An *aggregate expression* represents the application of an :ref:`aggregate
function <aggregation-functions>` across rows selected by a query. Besides the
function signature, :ref:`expressions <gloss-expression>` might contain
supplementary clauses and keywords.

The synopsis of an aggregate expression is one of the following::

   aggregate_function ( * ) [ FILTER ( WHERE condition ) ]
   aggregate_function ( [ DISTINCT ] expression [ , ... ] ) [ FILTER ( WHERE condition ) ]

Here, ``aggregate_function`` is a name of an aggregate function and
``expression`` is a column reference, :ref:`scalar function <scalar-functions>`
or literal.

If ``FILTER`` is specified, then only the rows that met the
:ref:`sql_dql_where_clause` condition are supplied to the aggregate function.

The optional ``DISTINCT`` keyword is only supported by aggregate functions
that explicitly mention its support. Please refer to existing
:ref:`limitations <aggregation-limitations>` for further information.

The aggregate expression form that uses a ``wildcard`` instead of an
``expression`` as a function argument is supported only by the ``count(*)``
aggregate function.


.. _aggregation-functions:

Aggregate functions
===================


.. _aggregation-arbitrary:

``arbitrary(column)``
---------------------

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


.. _aggregation-array-agg:

``array_agg(column)``
---------------------

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

.. SEEALSO::

    :ref:`aggregation-string-agg`


.. _aggregation-avg:

``avg(column)``
---------------

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


.. _aggregation-avg-distinct:

``avg(DISTINCT column)``
~~~~~~~~~~~~~~~~~~~~~~~~

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


.. _aggregation-count:

``count(column)``
-----------------

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


.. _aggregation-count-distinct:

``count(DISTINCT column)``
~~~~~~~~~~~~~~~~~~~~~~~~~~

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


.. _aggregation-count-star:

``count(*)``
~~~~~~~~~~~~

This aggregate function simply returns the number of rows that match the query.

``count(columName)`` is also possible, but currently only works on a primary
key column. The semantics are the same.

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


.. _aggregation-geometric-mean:

``geometric_mean(column)``
--------------------------

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


.. _aggregation-hyperloglog-distinct:

``hyperloglog_distinct(column, [precision])``
---------------------------------------------

The ``hyperloglog_distinct`` aggregate function calculates an approximate count
of distinct non-null values using the `HyperLogLog++`_ algorithm.

The return value data type is always a ``bigint``.

The first argument can be a reference to a column of all
:ref:`data-types-primitive`. :ref:`data-types-container` and
:ref:`data-types-geo` are not supported.

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


.. _aggregation-mean:

``mean(column)``
----------------

An alias for :ref:`aggregation-avg`.


.. _aggregation-min:

``min(column)``
---------------

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


.. _aggregation-max:

``max(column)``
---------------

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


.. _aggregation-stddev:

``stddev(column)``
------------------

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


.. _aggregation-string-agg:

``string_agg(column, delimiter)``
---------------------------------

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

.. SEEALSO::

    :ref:`aggregation-array-agg`


.. _aggregation-percentile:

``percentile(column, {fraction | fractions})``
----------------------------------------------

The ``percentile`` aggregate function computes a `Percentile`_ over numeric
non-null values in a column.

Percentiles show the point at which a certain percentage of observed values
occur. For example, the 98th percentile is the value which is greater than 98%
of the observed values. The result is defined and computed as an interpolated
weighted average. According to that it allows the median of the input data to
be defined conveniently as the 50th percentile.

The :ref:`function <gloss-function>` expects a single fraction or an array of
fractions and a column name. Independent of the input column data type the
result of ``percentile`` always returns a ``double precision``. If the value at
the specified column is ``null`` the row is ignored. Fractions must be double
precision values between 0 and 1. When supplied a single fraction, the function
will return a single value corresponding to the percentile of the specified
fraction::

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


.. _aggregation-sum:

``sum(column)``
---------------

Returns the sum of a set of numeric input values that are not ``NULL``.
Depending on the argument type a suitable return type is chosen. For ``real``
and ``double precison`` argument types the return type is equal to the argument
type. For ``char``, ``smallint``, ``integer`` and ``bigint`` the return type
changes to ``bigint``. If the range of ``bigint`` values (-2^64 to 2^64-1) gets
exceeded an ``ArithmeticException`` will be raised.

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
    SQLParseException[Cannot cast value `North West Ripple` to type `char`]

If the ``sum`` aggregation on a numeric data type with the fixed length can
potentially exceed its range it is possible to handle the overflow by casting
the :ref:`function <gloss-function>` argument to the :ref:`numeric type
<type-numeric>` with an arbitrary precision.

.. Hidden: create user visits table

    cr> CREATE TABLE uservisits (id integer, count bigint)
    ... CLUSTERED INTO 1 SHARDS
    ... WITH (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

.. Hidden: insert into uservisits table

    cr> INSERT INTO uservisits VALUES (1, 9223372036854775807), (2, 10);
    INSERT OK, 2 rows affected  (... sec)

.. Hidden: refresh uservisits table

    cr> REFRESH TABLE uservisits;
    REFRESH OK, 1 row affected  (... sec)

The ``sum`` aggregation on the ``bigint`` column will result in an overflow
in the following aggregation query::

    cr> SELECT sum(count)
    ... FROM uservisits;
    ArithmeticException[long overflow]

To address the overflow of the sum aggregation on the given field, we cast
the aggregation column to the ``numeric`` data type::

    cr> SELECT sum(count::numeric)
    ... FROM uservisits;
    +-----------------------------+
    | sum(cast(count AS numeric)) |
    +-----------------------------+
    |         9223372036854775817 |
    +-----------------------------+
    SELECT 1 row in set (... sec)

.. Hidden: refresh uservisits table

    cr> DROP TABLE uservisits;
    DROP OK, 1 row affected (... sec)


.. _aggregation-variance:

``variance(column)``
--------------------

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


.. _aggregation-limitations:

Limitations
===========

 - ``DISTINCT`` is not supported with aggregations on :ref:`sql_joins`.

 - Aggregate functions can only be applied to columns with a :ref:`plain index
   <sql_ddl_index_plain>`, which is the default for all :ref:`primitive type
   <data-types-primitive>` columns.


.. _Aggregate function: https://en.wikipedia.org/wiki/Aggregate_function
.. _Geometric Mean: https://en.wikipedia.org/wiki/Geometric_mean
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _Percentile: https://en.wikipedia.org/wiki/Percentile
.. _Standard Deviation: https://en.wikipedia.org/wiki/Standard_deviation
.. _TDigest: https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
.. _Variance: https://en.wikipedia.org/wiki/Variance
