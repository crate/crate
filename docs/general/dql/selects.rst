.. highlight:: psql
.. _sql_dql_queries:

==============
Selecting data
==============

Selecting (i.e. retrieving) data from CrateDB is done by using a SQL ``SELECT``
statement. The response to a ``SELECT`` query contains the column names of the
result, the actual result rows as a two-dimensional array of values, the row
count and the duration.

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

A simple select::

    cr> select name, position from locations order by id limit 2;
    +-------------------+----------+
    | name              | position |
    +-------------------+----------+
    | North West Ripple | 1        |
    | Arkintoofle Minor | 3        |
    +-------------------+----------+
    SELECT 2 rows in set (... sec)

If the '*' operator is used, all columns defined in the schema are returned for
each row::

    cr> select * from locations order by id limit 1 offset 1;
    +----+-------------------+--------------+--------+----------+---------...-+-------...-+-------------+
    | id | name              |         date | kind   | position | description | race      | information |
    +----+-------------------+--------------+--------+----------+---------...-+-------...-+-------------+
    | 10 | Arkintoofle Minor | 308534400000 | Planet |        3 | Motivate... | {"desc... |        NULL |
    +----+-------------------+--------------+--------+----------+---------...-+-------...-+-------------+
    SELECT 1 row in set (... sec)

Aliases can be used to change the output name of the columns::

    cr> select name as n
    ... from locations
    ... where name = 'North West Ripple';
    +-------------------+
    | n                 |
    +-------------------+
    | North West Ripple |
    +-------------------+
    SELECT 1 row in set (... sec)

.. _sql_dql_from_clause:

``FROM`` clause
===============

The ``FROM`` clause is used to reference the relation this select query is
based upon. Can be a single table, many tables, a view, a :ref:`JOIN <sql_joins>`
or another ``SELECT`` statement. See :ref:`SELECT Reference
<sql_reference_select>`

Tables and views are referenced by schema and table name and can optionally be
aliased.  If the relation ``t`` is only referenced by name, CrateDB assumes the
relation ``doc.t`` was meant. Schemas that were newly created using
:ref:`ref-create-table` must be referenced explicitly.

The two following queries are equivalent::

    cr> select name, position from locations
    ... order by name desc nulls last limit 2;
    +-------------------+----------+
    | name              | position |
    +-------------------+----------+
    | Outer Eastern Rim |        2 |
    | North West Ripple |        1 |
    +-------------------+----------+
    SELECT 2 rows in set (... sec)

::

    cr> select doc.locations.name as n, position from doc.locations
    ... order by name desc nulls last limit 2;
    +-------------------+----------+
    | n                 | position |
    +-------------------+----------+
    | Outer Eastern Rim |        2 |
    | North West Ripple |        1 |
    +-------------------+----------+
    SELECT 2 rows in set (... sec)

A table can be aliased for the sake of brevity too::

    cr> select name from doc.locations as l
    ... where l.name = 'Outer Eastern Rim';
    +-------------------+
    | name              |
    +-------------------+
    | Outer Eastern Rim |
    +-------------------+
    SELECT 1 row in set (... sec)

.. _sql_dql_joins:

Joins
=====

.. NOTE::

    CrateDB currently supports only a limited set of JOINs.

    See the :ref:`sql_joins` for current state.

.. _sql_dql_distinct_clause:

``DISTINCT`` clause
===================

If DISTINCT is specified, one unique row is kept. All other duplicate rows are
removed from the result set::

    cr> select distinct date from locations order by date;
    +---------------+
    | date          |
    +---------------+
    | 308534400000  |
    | 1367366400000 |
    | 1373932800000 |
    +---------------+
    SELECT 3 rows in set (... sec)

.. note::

   Using `DISTINCT` is only supported on :ref:`sql_ddl_datatypes_primitives`.

.. _sql_dql_where_clause:

``WHERE`` clause
================

A simple where clause example using an equality operator::

    cr> select description from locations where id = '1';
    +---------------------------------------...--------------------------------------+
    | description                                                                    |
    +---------------------------------------...--------------------------------------+
    | Relative to life on NowWhat, living on... a factor of about seventeen million. |
    +---------------------------------------...--------------------------------------+
    SELECT 1 row in set (... sec)

Comparison operators
--------------------

These :ref:`sql_operators` are supported and can be used for all simple data
types.

For strings a lexicographical comparison is performed based on the Lucene
TermRangeQuery::

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

For details please refer to the `Apache Lucene`_ site.

.. _`Apache Lucene`: https://lucene.apache.org/core/4_0_0/core/org/apache/lucene/search/Query.html

Number and date field comparison behave as expected from standard SQL.

The following example uses one of the supported ISO date formats::

    cr> select date, position from locations where date <= '1979-10-12' and
    ... position < 3 order by position;
    +--------------+----------+
    | date         | position |
    +--------------+----------+
    | 308534400000 | 1        |
    | 308534400000 | 2        |
    +--------------+----------+
    SELECT 2 rows in set (... sec)

For a detailed explanation of the supported ISO date formats please refer to
the `joda date_optional_time`_ site.

.. _`joda date_optional_time`: http://joda-time.sourceforge.net/api-release/org/joda/time/format/ISODateTimeFormat.html#dateOptionalTimeParser%28%29

For custom date types, or defined date formats in the object mapping the
corresponding format should be used for a comparison. Otherwise the operation
may fail.

.. _sql_ddl_regexp:

Regular expressions
===================

Operators for matching using regular expressions.

.. list-table::
   :widths: 5 20 15
   :header-rows: 1

   * - Operator
     - Description
     - Example
   * - ``~``
     - Matches regular expression, case sensitive
     - ::

         'foo' ~ '.*foo.*'
   * - ``~*``
     - Matches regular expression, case insensitive
     - ::

         'Foo' ~* '.*foo.*'
   * - ``!~``
     - Does not match regular expression, case sensitive
     - ::

         'Foo' !~ '.*foo.*'
   * - ``!~*``
     - Does not match regular expression, case insensitive
     - ::

         'foo' !~* '.*bar.*'

The ``~`` operator can be used to match a string against a regular expression.
It returns ``true`` if the string matches the pattern, ``false`` if not, and
``NULL`` if string is ``NULL``.

To negate the matching, use the optional ``!`` prefix. The operator returns
``true`` if the string does not match the pattern, ``false`` otherwise.

The regular expression pattern is implicitly anchored, that means that the
whole string must match, not a single subsequence. All unicode characters are
allowed.

If using `PCRE`_ features in the regular expression pattern, the operator uses
the regular expression engine of the Java standard library ``java.util.regex``.

If not using `PCRE`_ features in the regular expression pattern, the operator
uses `Lucene Regular Expressions`_, which are optimized for fast regular
expression matching on Lucene terms.

`Lucene Regular Expressions`_ are basically `POSIX Extended Regular
Expressions`_ without the character classes and with some extensions, like a
metacharacter ``#``  for the empty string or ``~`` for negation and others. By
default all Lucene extensions are enabled. See the Lucene documentation for
more details.

.. NOTE::

    Since case-insensitive matching using ``~*`` or ``!~*`` implicitly uses the
    regular expression engine of the Java standard library, features of `Lucene
    Regular Expressions`_ do not work there.

Examples::

    cr> select name from locations where name ~ '([A-Z][a-z0-9]+)+'
    ... order by name;
    +------------+
    | name       |
    +------------+
    | Aldebaran  |
    | Algol      |
    | Altair     |
    | Argabuthon |
    | Bartledan  |
    +------------+
    SELECT 5 rows in set (... sec)

::

    cr> select 'matches' from sys.cluster where
    ... 'gcc --std=c99 -Wall source.c' ~ '[A-Za-z0-9]+( (-|--)[A-Za-z0-9]+)*( [^ ]+)*';
    +-----------+
    | 'matches' |
    +-----------+
    | matches   |
    +-----------+
    SELECT 1 row in set (... sec)

::

    cr> select 'no_match' from sys.cluster where 'foobaz' !~ '(foo)?(bar)$';
    +------------+
    | 'no_match' |
    +------------+
    | no_match   |
    +------------+
    SELECT 1 row in set (... sec)

.. _Lucene Regular Expressions: https://lucene.apache.org/core/4_9_0/core/org/apache/lucene/util/automaton/RegExp.html
.. _POSIX Extended Regular Expressions: https://en.wikipedia.org/wiki/Regular_expression#POSIX_extended
.. _PCRE: https://en.wikipedia.org/wiki/Perl_Compatible_Regular_Expressions

.. _sql_dql_like:

``LIKE (ILIKE)``
================

CrateDB supports the ``LIKE`` and ``ILIKE`` operators. These operators can
be used to query for rows where only part of a columns value should match
something. The only difference is that, in the case of ``ILIKE``, the
matching is case insensitive.

For example to get all locations where the name starts with 'Ar' the following
queries can be used::

    cr> select name from locations where name like 'Ar%' order by name asc;
    +-------------------+
    | name              |
    +-------------------+
    | Argabuthon        |
    | Arkintoofle Minor |
    +-------------------+
    SELECT 2 rows in set (... sec)

::

    cr> select name from locations where name ilike 'ar%' order by name asc;
    +-------------------+
    | name              |
    +-------------------+
    | Argabuthon        |
    | Arkintoofle Minor |
    +-------------------+
    SELECT 2 rows in set (... sec)

The following wildcard operators are available:

== ========================================
%  A substitute for zero or more characters
_  A substitute for a single character
== ========================================

The wildcard operators may be used at any point in the string literal. For
example a more complicated like clause could look like this::

    cr> select name from locations where name like '_r%a%' order by name asc;
    +------------+
    | name       |
    +------------+
    | Argabuthon |
    +------------+
    SELECT 1 row in set (... sec)

In order so search for the wildcard characters themselves it is possible to
escape them using a backslash::

    cr> select description from locations
    ... where description like '%\%' order by description asc;
    +-------------------------+
    | description             |
    +-------------------------+
    | The end of the Galaxy.% |
    +-------------------------+
    SELECT 1 row in set (... sec)

.. CAUTION::

    Queries with a like/ilike clause can be quite slow. Especially if the clause
    starts with a wildcard character. Because in that case CrateDB has to iterate
    over all rows and can't utilize the index. For better performance consider
    using a fulltext index.

.. _sql_dql_not:

``NOT``
=======

``NOT`` negates a boolean expression::

    [ NOT ] boolean_expression

The result type is boolean.

==========  ======
expression  result
==========  ======
true        false
false       true
null        null
==========  ======

.. WARNING::

   CrateDB handles the case of ``NOT (NULL)`` inconsistently. The above is only
   true when the ``NOT`` appears in a ``SELECT`` clause or a ``WHERE`` clause
   that operates on system tables. The result of ``NOT (NULL)`` in a
   ``WHERE`` clause that operates on user tables will produce
   inconsistent but deterministic results (``NULL`` or ``TRUE``)
   depending on the specifics of the clause. This does not adhere to
   standard SQL three-valued-logic and will be fixed in a future release.

.. _sql_dql_in:

``IN``
======

CrateDB also supports the binary operator ``IN``, which allows you to verify
the membership of left-hand operand in a right-hand set of expressions. Returns
``true`` if any evaluated expression value from a right-hand set equals
left-hand operand. Returns ``false`` otherwise::

    cr> select name, kind from locations
    ... where (kind in ('Star System', 'Planet'))  order by name asc;
     +---------------------+-------------+
     | name                | kind        |
     +---------------------+-------------+
     |                     | Planet      |
     | Aldebaran           | Star System |
     | Algol               | Star System |
     | Allosimanius Syneca | Planet      |
     | Alpha Centauri      | Star System |
     | Altair              | Star System |
     | Argabuthon          | Planet      |
     | Arkintoofle Minor   | Planet      |
     | Bartledan           | Planet      |
     +---------------------+-------------+
     SELECT 9 rows in set (... sec)

The ``IN`` construct can be used in :ref:`sql_subquery_expressions` or
:ref:`sql_array_comparisons`.

.. _sql_dql_is_null:

``IS NULL``
===========

Returns ``TRUE`` if ``expr`` evaluates to ``NULL``. Given a column reference it
returns ``TRUE`` if the field contains ``NULL`` or is missing.

Use this predicate to check for ``NULL`` values as SQL's three-valued logic
does always return ``NULL`` when comparing ``NULL``.

:expr:
  Expression of one of the supported :ref:`data-types` supported by CrateDB.

::

    cr> select name from locations where race is null order by name;
    +------------------------------------+
    | name                               |
    +------------------------------------+
    |                                    |
    | Aldebaran                          |
    | Algol                              |
    | Allosimanius Syneca                |
    | Alpha Centauri                     |
    | Altair                             |
    | Argabuthon                         |
    | Galactic Sector QQ7 Active J Gamma |
    | North West Ripple                  |
    | Outer Eastern Rim                  |
    | NULL                               |
    +------------------------------------+
    SELECT 11 rows in set (... sec)

::

    cr> select count(*) from locations where name is null;
    +----------+
    | count(*) |
    +----------+
    |        1 |
    +----------+
    SELECT 1 row in set (... sec)

.. _sql_dql_is_not_null:

``IS NOT NULL``
===============

Returns ``TRUE`` if ``expr`` does not evaluate to ``NULL``. Additionally, for
column references it returns ``FALSE`` if the column does not exist.

Use this predicate to check for non-``NULL`` values as SQL's three-valued logic
does always return ``NULL`` when comparing ``NULL``.

:expr:
  Expression of one of the supported :ref:`data-types` supported by CrateDB.

::

    cr> select name from locations where race['interests'] is not null;
    +-------------------+
    | name              |
    +-------------------+
    | Arkintoofle Minor |
    | Bartledan         |
    +-------------------+
    SELECT 2 rows in set (... sec)

::

    cr> select count(*) from locations where name is not null;
    +----------+
    | count(*) |
    +----------+
    |       12 |
    +----------+
    SELECT 1 row in set (... sec)

.. _sql_dql_any_array:

``ANY (array)``
===============

The ANY (or SOME) operator allows to search for elements within arrays. This
allows to query for rows where an element of an array is, for example, equal to
or greater than some ``expression``.

The following example returns any row where the array ``race['interests']``
contains an element 'netball'::

    cr> select race['name'], race['interests'] from locations
    ... where 'netball' = ANY(race['interests']);
    +----------------+-----------------------------------------+
    | race['name']   | race['interests']                       |
    +----------------+-----------------------------------------+
    | Bartledannians | ["netball", "books with 100.000 words"] |
    +----------------+-----------------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select race['name'], race['interests'] from locations
    ... where 'books%' LIKE ANY(race['interests']);
    +----------------+-----------------------------------------+
    | race['name']   | race['interests']                       |
    +----------------+-----------------------------------------+
    | Bartledannians | ["netball", "books with 100.000 words"] |
    +----------------+-----------------------------------------+
    SELECT 1 row in set (... sec)

It can also be used on arrays::

    cr> select name, race['interests'] from locations
    ... where name = ANY(ARRAY['Bartledan', 'Algol'])
    ... order by name asc;
    +-----------+-----------------------------------------+
    | name      | race['interests']                       |
    +-----------+-----------------------------------------+
    | Algol     | NULL                                    |
    | Bartledan | ["netball", "books with 100.000 words"] |
    +-----------+-----------------------------------------+
    SELECT 2 rows in set (... sec)

This way it can be used as a shortcut for ``name = 'Bartledan' OR name =
'Algol'`` or any other ANY comparison.

The ``ANY`` construct can be used in :ref:`sql_subquery_expressions` or
:ref:`sql_array_comparisons`.


Negating ``ANY``
----------------

One important thing to notice when using ANY is that negating the ANY operator
does not behave as negating normal comparison operators.

The following query can be translated to *get all rows where race['interests']
has at least one element that equals 'netball'*::

    cr> select race['name'], race['interests'] from locations
    ... where 'netball' = ANY(race['interests']);
    +----------------+-----------------------------------------+
    | race['name']   | race['interests']                       |
    +----------------+-----------------------------------------+
    | Bartledannians | ["netball", "books with 100.000 words"] |
    +----------------+-----------------------------------------+
    SELECT 1 row in set (... sec)

The following query using the negated operator ``!=`` can be translated to *get
all rows where race['interests'] has at least one element that does not equal
'netball'*. As you see, the result is the same in this case::

    cr> select race['name'], race['interests'] from locations
    ... where 'netball' != ANY(race['interests']);
    +----------------+-----------------------------------------+
    | race['name']   | race['interests']                       |
    +----------------+-----------------------------------------+
    | Minories       | ["baseball", "short stories"]           |
    | Bartledannians | ["netball", "books with 100.000 words"] |
    +----------------+-----------------------------------------+
    SELECT 2 rows in set (... sec)

.. NOTE::

    When using the negated operator ``!= ANY`` by default the maximum size of
    the array to operate on is ``8192``. To be able to use larger arrays the
    :ref:`indices.query.bool.max_clause_count
    <indices.query.bool.max_clause_count>` setting must be changed
    appropriately on each node.

Negating the ``=`` query from above is totally different. It can be translated
to *get all rows where race['interests'] has no value that equals 'netball'*::

    cr> select race['name'], race['interests'] from locations
    ... where not 'netball' = ANY(race['interests']) order by race['name'];
    +--------------+-------------------------------+
    | race['name'] | race['interests']             |
    +--------------+-------------------------------+
    | Minories     | ["baseball", "short stories"] |
    +--------------+-------------------------------+
    SELECT 1 row in set (... sec)

.. TIP::

    When using ``NOT <value> = ANY(<array_col>)`` the performance of the query
    could be quite bad, because special handling is required to implement the
    `3-valued logic`_. To achieve better performance, consider using the
    :ref:`ignore3vl function<ignore3vl>`.

The same behaviour (though different comparison operations involved) holds true
for operators

 - ``LIKE`` and ``NOT LIKE``

 - all other comparison operators (excluding ``IS NULL`` and ``IS NOT NULL``)

.. NOTE::

    When using the operators ``LIKE ANY`` and ``NOT LIKE ANY`` by default the
    maximum size of the array to operate on is ``8192``. To be able to use
    larger arrays the :ref:`indices.query.bool.max_clause_count
    <indices.query.bool.max_clause_count>` setting must be changed
    appropriately on each node.


.. _sql_dql_objects:

Inner objects and nested objects
================================

CrateDB supports an ``object`` data type, used for simple storing a whole
object into a column and it's even possible to select and query for properties
of such objects.

Select a property of an inner object::

    cr> select name, race['name'] from locations where name = 'Bartledan';
    +-----------+----------------+
    | name      | race['name']   |
    +-----------+----------------+
    | Bartledan | Bartledannians |
    +-----------+----------------+
    SELECT 1 row in set (... sec)

Query for a property of an inner object::

    cr> select name, race['name'] from locations
    ... where race['name'] = 'Bartledannians';
    +-----------+----------------+
    | name      | race['name']   |
    +-----------+----------------+
    | Bartledan | Bartledannians |
    +-----------+----------------+
    SELECT 1 row in set (... sec)

Inserting objects::

    cr> insert into locations (id, name, position, kind, race)
    ... values ('DO', 'Dornbirn', 14, 'City', {name='Vorarlberger',
    ...     description = 'Very nice people with a strange accent',
    ...     interests = ['mountains', 'cheese', 'enzian']}
    ... );
    INSERT OK, 1 row affected (... sec)


.. _sql_dql_object_arrays:

Object arrays
=============

Arrays in CrateDB can be queried for containment using the
:ref:`sql_dql_any_array` operator.

It is possible to access fields of :ref:`sql_dql_objects` using subscript
expressions. If the parent is an object array, you'll get an array of the
selected field.


Examples::

    cr> select name, information['population'] from locations
    ... where information['population'] is not null
    ... order by name;
    +-------------------+---------------------------+
    | name              | information['population'] |
    +-------------------+---------------------------+
    | North West Ripple | [12, 42]                  |
    | Outer Eastern Rim | [5673745846]              |
    +-------------------+---------------------------+
    SELECT 2 rows in set (... sec)

::

    cr> select information from locations
    ... where information['population'] is not null
    ... order by name;
    +----------------------------------------------------...-----------------------+
    | information                                                                  |
    +----------------------------------------------------...-----------------------+
    | [{"evolution_level": 4, "population": 12}, {"evolu...": 42, "popul...": 42}] |
    | [{"evolution_level": 2, "population": 5673745846}]                           |
    +---------------------------------------------------...------------------------+
    SELECT 2 rows in set (... sec)

::

    cr> insert into locations (id, name, position, kind, information)
    ... values (
    ...   'B', 'Berlin', 15, 'City',
    ...   [{evolution_level=6, population=3600001},
    ...   {evolution_level=42, population=1}]
    ... );
    INSERT OK, 1 row affected (... sec)

::

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

::

    cr> select name from locations where 4 < ANY (information['evolution_level'])
    ... order by name;
    +-------------------+
    | name              |
    +-------------------+
    | Berlin            |
    | North West Ripple |
    +-------------------+
    SELECT 2 rows in set (... sec)


.. note::

    Although it is possible to use ``? = ANY (object_array)`` it's usage is
    discouraged as it cannot utilize the index and has to do the equivalent of
    a table scan.

.. _sql_dql_object_arrays_select:

Selecting array elements
------------------------

Array elements can be selected directly using a integer value greater than or
equal to **1**. The maximum supported array index is **2147483648**. Using an
index greater than the actual array size results in a NULL value.

::

    cr> select name, information[1]['population'] as population from locations
    ... where information['population'] is not null
    ... order by name;
    +-------------------+------------+
    | name              | population |
    +-------------------+------------+
    | Berlin            |    3600001 |
    | North West Ripple |         12 |
    | Outer Eastern Rim | 5673745846 |
    +-------------------+------------+
    SELECT 3 rows in set (... sec)

.. NOTE::

   Only 1 array notation inside a subscript expression is supported,
   e.g. following won't work:

   ``select information[1][tags][1] from locations;``

.. _sql_dql_aggregation:

Data aggregation
================

CrateDB supports :ref:`aggregation` via the following aggregation functions.

Aggregation works across all the rows that match a query or on all matching
rows in every distinct group of a ``GROUP BY`` statement. Aggregating
``SELECT`` statements without ``GROUP BY`` will always return one row.

+---------------------+---------------+----------------------------------+-----------------------+
| Name                | Arguments     | Description                      | Return Type           |
+=====================+===============+==================================+=======================+
| ARBITRARY           | column name of| Returns an undefined value of    | the input             |
|                     | a primitive   | all the values in the argument   | column type or NULL   |
|                     | typed         | column. Can be NULL.             | if some value of the  |
|                     | column        |                                  | matching rows in that |
|                     | (all but      |                                  | column is NULL        |
|                     | object)       |                                  |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| AVG / MEAN          | column name of| Returns the arithmetic mean of   | double or NULL        |
|                     | a numeric or  | the values in the argument       | if all values of all  |
|                     | timestamp     | column.                          | matching rows in that |
|                     | column        | NULL-values are ignored.         | column are NULL       |
+---------------------+---------------+----------------------------------+-----------------------+
| COUNT(*)            | star as       | Counts the number of rows        | long                  |
|                     | parameter or  | that match the query.            |                       |
|                     | as constant   |                                  |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| COUNT               | column name   | Counts the number of rows        | long                  |
|                     |               | that contain a non NULL          |                       |
|                     |               | value for the given column.      |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| COUNT(DISTINCT col) | column name   | Counts the number of distinct    | long                  |
|                     |               | values for the given column      |                       |
|                     |               | that are not NULL.               |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| GEOMETRIC_MEAN      | column name of| Computes the geometric mean for  | double or NULL        |
|                     | a numeric or  | positive numbers.                | if all values of all  |
|                     | timestamp     |                                  | matching rows in that |
|                     | column        |                                  | are NULL or if a value|
|                     |               |                                  | is negative.          |
+---------------------+---------------+----------------------------------+-----------------------+
| MIN                 | column name of| Returns the smallest of the      | the input             |
|                     | a numeric,    | values in the argument column    | column type or NULL   |
|                     | timestamp     | in case of strings this          | if all values in that |
|                     | or string     | means the lexicographically      | matching rows in that |
|                     | column        | smallest. NULL-values are ignored| column are NULL       |
+---------------------+---------------+----------------------------------+-----------------------+
| MAX                 | column name of| Returns the biggest of the       | the input             |
|                     | a numeric,    | values in the argument column    | column type or NULL   |
|                     | timestamp     | in case of strings this          | if all values of all  |
|                     | or string     | means the lexicographically      | matching rows in that |
|                     | column        | biggest. NULL-values are ignored | column are NULL       |
+---------------------+---------------+----------------------------------+-----------------------+
| STDDEV              | column name of| Returns the standard deviation   | double or NULL        |
|                     | a numeric or  | of the values in the argument    | if all values are NULL|
|                     | timestamp     | column.                          | or we got no value at |
|                     | column        | NULL-values are ignored.         | all                   |
+---------------------+---------------+----------------------------------+-----------------------+
| STRING_AGG          | an expression | Concatenated input values into   | text                  |
|                     | and delimiter | a string, separated by a         |                       |
|                     | of a text type| delimiter.                       |                       |
|                     |               | NULL-values are ignored.         |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| PERCENTILE          | column of a   | Returns the provided percentile  | a double precision    |
|                     | numeric type  | of the values in the argument    | value                 |
|                     | and a double  | column.                          |                       |
|                     | percentile    | NULL-values are ignored.         |                       |
|                     | value         |                                  |                       |
+---------------------+---------------+----------------------------------+-----------------------+
| SUM                 | column name of| Returns the sum of the values in | double or NULL        |
|                     | a numeric or  | the argument column.             | if all values of all  |
|                     | timestamp     | NULL-values are ignored.         | matching rows in that |
|                     | column        |                                  | column are NULL       |
+---------------------+---------------+----------------------------------+-----------------------+
| VARIANCE            | column name of| Returns the variance of the      | double or NULL        |
|                     | a numeric or  | values in the argument column.   | if all values are NULL|
|                     | timestamp     | NULL-values are ignored.         | or we got no value at |
|                     | column        |                                  | all                   |
+---------------------+---------------+----------------------------------+-----------------------+

Some Examples::

    cr> select count(*) from locations;
    +----------+
    | count(*) |
    +----------+
    | 15       |
    +----------+
    SELECT 1 row in set (... sec)

::

    cr> select count(*) from locations where kind = 'Planet';
    +----------+
    | count(*) |
    +----------+
    | 5        |
    +----------+
    SELECT 1 row in set (... sec)

::

    cr> select count(name), count(*) from locations;
    +-------------+----------+
    | count(name) | count(*) |
    +-------------+----------+
    | 14          | 15       |
    +-------------+----------+
    SELECT 1 row in set (... sec)

::

    cr> select max(name) from locations;
    +-------------------+
    | max(name)         |
    +-------------------+
    | Outer Eastern Rim |
    +-------------------+
    SELECT 1 row in set (... sec)

::

    cr> select min(date) from locations;
    +--------------+
    | min(date)    |
    +--------------+
    | 308534400000 |
    +--------------+
    SELECT 1 row in set (... sec)

::

    cr> select count(*), kind from locations
    ... group by kind order by kind asc;
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    | 2        | City        |
    | 4        | Galaxy      |
    | 5        | Planet      |
    | 4        | Star System |
    +----------+-------------+
    SELECT 4 rows in set (... sec)

::

    cr> select max(position), kind from locations
    ... group by kind order by max(position) desc;
    +---------------+-------------+
    | max(position) | kind        |
    +---------------+-------------+
    | 15            | City        |
    | 6             | Galaxy      |
    | 5             | Planet      |
    | 4             | Star System |
    +---------------+-------------+
    SELECT 4 rows in set (... sec)

::

    cr> select min(name), kind from locations
    ... group by kind order by min(name) asc;
    +------------------------------------+-------------+
    | min(name)                          | kind        |
    +------------------------------------+-------------+
    |                                    | Planet      |
    | Aldebaran                          | Star System |
    | Berlin                             | City        |
    | Galactic Sector QQ7 Active J Gamma | Galaxy      |
    +------------------------------------+-------------+
    SELECT 4 rows in set (... sec)

::

    cr> select count(*), min(name), kind from locations
    ... group by kind order by kind;
    +----------+------------------------------------+-------------+
    | count(*) | min(name)                          | kind        |
    +----------+------------------------------------+-------------+
    | 2        | Berlin                             | City        |
    | 4        | Galactic Sector QQ7 Active J Gamma | Galaxy      |
    | 5        |                                    | Planet      |
    | 4        | Aldebaran                          | Star System |
    +----------+------------------------------------+-------------+
    SELECT 4 rows in set (... sec)

::

    cr> select sum(position) as sum_positions, kind from locations
    ... group by kind order by sum_positions;
    +---------------+-------------+
    | sum_positions | kind        |
    +---------------+-------------+
    |            10 | Star System |
    |            13 | Galaxy      |
    |            15 | Planet      |
    |            29 | City        |
    +---------------+-------------+
    SELECT 4 rows in set (... sec)

Window functions
================

CrateDB supports the :ref:`OVER <over>` clause to enable the execution of
:ref:`window functions <window-functions>`::

   cr> select sum(position) OVER() AS pos_sum, name from locations order by name;
   +---------+------------------------------------+
   | pos_sum | name                               |
   +---------+------------------------------------+
   |      67 |                                    |
   |      67 | Aldebaran                          |
   |      67 | Algol                              |
   |      67 | Allosimanius Syneca                |
   |      67 | Alpha Centauri                     |
   |      67 | Altair                             |
   |      67 | Argabuthon                         |
   |      67 | Arkintoofle Minor                  |
   |      67 | Bartledan                          |
   |      67 | Berlin                             |
   |      67 | Dornbirn                           |
   |      67 | Galactic Sector QQ7 Active J Gamma |
   |      67 | North West Ripple                  |
   |      67 | Outer Eastern Rim                  |
   |      67 | NULL                               |
   +---------+------------------------------------+
   SELECT 15 rows in set (... sec)

.. _sql_dql_group_by:

``GROUP BY``
============

CrateDB supports the ``group by`` clause. This clause can be used to group the
resulting rows by the value(s) of one or more columns. That means that rows
that contain duplicate values will be merged.

This is useful if used in conjunction with aggregation functions::

    cr> select count(*), kind from locations
    ... group by kind order by count(*) desc, kind asc;
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    | 5        | Planet      |
    | 4        | Galaxy      |
    | 4        | Star System |
    | 2        | City        |
    +----------+-------------+
    SELECT 4 rows in set (... sec)

.. NOTE::

   All columns that are used either as result column or in the order by clause
   have to be used within the group by clause. Otherwise the statement won't
   execute.

   Grouping will be executed against the real table column when aliases that
   shadow the table columns are used.

   Grouping on array columns doesn't work, but arrays can be unnested in a
   subquery using :ref:`unnest`, it is then possible to use GROUP BY on the
   subquery.

.. _sql_dql_having:

``HAVING``
----------

The having clause is the equivalent to the where clause for the resulting rows
of a group by clause.

A simple having clause example using an equality operator::

    cr> select count(*), kind from locations
    ... group by kind having count(*) = 4 order by kind;
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    |        4 | Galaxy      |
    |        4 | Star System |
    +----------+-------------+
    SELECT 2 rows in set (... sec)

The condition of the having clause can refer to the resulting columns of the
group by clause.

It is also possible to use aggregates in the having clause just like in the
result columns::

    cr> select count(*), kind from locations
    ... group by kind having min(name) = 'Berlin';
    +----------+------+
    | count(*) | kind |
    +----------+------+
    |        2 | City |
    +----------+------+
    SELECT 1 row in set (... sec)

::

    cr> select count(*), kind from locations
    ... group by kind having count(*) = 4 and kind like 'Gal%';
    +----------+--------+
    | count(*) | kind   |
    +----------+--------+
    |        4 | Galaxy |
    +----------+--------+
    SELECT 1 row in set (... sec)

.. NOTE::

   Aliases are not supported in the having clause.

.. _`3-valued logic`: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
