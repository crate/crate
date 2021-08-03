.. highlight:: psql
.. _sql_dql_queries:

==============
Selecting data
==============

Selecting (i.e., retrieving) data from CrateDB can be done by using an SQL
:ref:`SELECT <sql-select>` statement. The response to a ``SELECT`` query
includes the column names of the result, the result rows as a two-dimensional
array of values, the row count, and the execution time.

.. rubric:: Table of contents

.. contents::
   :local:


Introduction
============

A simple select::

    cr> select id, name from locations order by id limit 2;
    +----+-------------------+
    | id | name              |
    +----+-------------------+
    |  1 | North West Ripple |
    |  2 | Outer Eastern Rim |
    +----+-------------------+
    SELECT 2 rows in set (... sec)

If the ``*`` :ref:`operator <gloss-operator>` is used, all columns defined in
the schema are returned for each row::

    cr> select * from locations order by id limit 2;
    +----+-------------------+--------------+--------+----------+-------------...-+-----------+
    | id | name              |         date | kind   | position | description ... | landmarks |
    +----+-------------------+--------------+--------+----------+-------------...-+-----------+
    |  1 | North West Ripple | 308534400000 | Galaxy |        1 | Relative to ... |      NULL |
    |  2 | Outer Eastern Rim | 308534400000 | Galaxy |        2 | The Outer Ea... |      NULL |
    +----+-------------------+--------------+--------+----------+-------------...-+-----------+
    SELECT 2 rows in set (... sec)

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
based upon. Can be a single table, many tables, a view, a :ref:`JOIN
<sql-select-joined-relation>` or another :ref:`SELECT <sql-select>` statement.

Tables and views are referenced by schema and table name and can optionally be
aliased.  If the relation ``t`` is only referenced by name, CrateDB assumes the
relation ``doc.t`` was meant. Schemas that were newly created using
:ref:`sql-create-table` must be referenced explicitly.

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

If ``DISTINCT`` is specified, one unique row is kept. All other duplicate rows
are removed from the result set::

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

   Using ``DISTINCT`` is only supported on :ref:`data-types-primitive`.


.. _sql_dql_where_clause:

``WHERE`` clause
================

Here is a simple ``WHERE`` clause using an equality :ref:`operator
<sql_dql_comparison_operators>`::

    cr> select description from locations where id = '1';
    +---------------------------------------...--------------------------------------+
    | description                                                                    |
    +---------------------------------------...--------------------------------------+
    | Relative to life on NowWhat, living on... a factor of about seventeen million. |
    +---------------------------------------...--------------------------------------+
    SELECT 1 row in set (... sec)


.. _sql_dql_comparison_operators:

Comparison operators
====================

CrateDB supports a variety of :ref:`comparison operators
<comparison-operators-where>` (including basic operators such as ``=``, ``<``, ``>``,
and so on).


.. _sql_dql_regexp:

Regular expressions
-------------------

Comparison operators for matching using :ref:`regular expressions
<gloss-regular-expression>`:

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
metacharacter ``#`` for the empty string or ``~`` for negation and others. By
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


.. _sql_dql_like:

``LIKE (ILIKE)``
----------------

CrateDB supports the ``LIKE`` and ``ILIKE`` :ref:`operators <gloss-operator>`.
These operators can be used to query for rows where only part of a columns
value should match something. The only difference is that, in the case of
``ILIKE``, the matching is case insensitive.

For example to get all locations where the name starts with ``Ar`` the
following queries can be used::

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

===== ========================================
``%``  A substitute for zero or more characters
``_``  A substitute for a single character
===== ========================================

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

    ``LIKE`` and ``ILIKE`` clauses can slow a query down, especially when used
    in combination with wildcard characters. This is because CrateDB has to
    iterate over all rows for the comparison and cannot utilize the index.

    For better performance, consider using :ref:`fulltext search
    <sql_dql_fulltext_search>`.


.. _sql_dql_not:

``NOT``
--------

``NOT`` negates a :ref:`boolean expression <sql-literal-value>`::

    [ NOT ] boolean_expression

The result type is boolean.

==========  ======
expression  result
==========  ======
true        false
false       true
null        null
==========  ======


.. _sql_dql_is_null:

``IS NULL``
-----------

Returns ``TRUE`` if ``expr`` :ref:`evaluates <gloss-evaluation>` to
``NULL``. Given a column reference, it returns ``TRUE`` if the field contains
``NULL`` or is missing.

Use this predicate to check for ``NULL`` values as SQL's three-valued logic
does always return ``NULL`` when comparing ``NULL``.

:expr:
  :ref:`Expression <gloss-expression>` of one of the supported
  :ref:`data types <data-types>` supported by CrateDB.

::

    cr> select name from locations where inhabitants is null order by name;
    +------------------------------------+
    | name                               |
    +------------------------------------+
    |                                    |
    | Aldebaran                          |
    | Algol                              |
    | Allosimanius Syneca                |
    | Alpha Centauri                     |
    | Altair                             |
    | Galactic Sector QQ7 Active J Gamma |
    | North West Ripple                  |
    | Outer Eastern Rim                  |
    | NULL                               |
    +------------------------------------+
    SELECT 10 rows in set (... sec)

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
---------------

Returns ``TRUE`` if ``expr`` does not :ref:`evaluate <gloss-evaluation>` to
``NULL``. Additionally, for column references it returns ``FALSE`` if the
column does not exist.

Use this predicate to check for non-``NULL`` values as SQL's three-valued logic
does always return ``NULL`` when comparing ``NULL``.

:expr:
  :ref:`Expression <gloss-expression>` of one of the supported
  :ref:`data types <data-types>` supported by CrateDB.

::

    cr> select name from locations where inhabitants['interests'] is not null;
    +-------------------+
    | name              |
    +-------------------+
    | Arkintoofle Minor |
    | Bartledan         |
    | Argabuthon        |
    +-------------------+
    SELECT 3 rows in set (... sec)

::

    cr> select count(*) from locations where name is not null;
    +----------+
    | count(*) |
    +----------+
    |       12 |
    +----------+
    SELECT 1 row in set (... sec)


.. _sql_dql_array_comparisons:

Array comparisons
=================

CrateDB supports a variety of :ref:`array comparisons <sql_array_comparisons>`.


.. _sql_dql_in:

``IN``
------

CrateDB supports the :ref:`operator <gloss-operator>` ``IN`` which allows you
to verify the membership of the left-hand operator operand in a right-hand set
of :ref:`expressions <gloss-expression>`. Returns ``true`` if any
:ref:`evaluated <gloss-evaluation>` expression value from a right-hand set
equals left-hand operand. Returns ``false`` otherwise::

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

The ``IN`` construct can be used in :ref:`subquery expressions
<sql_subquery_expressions>` or :ref:`array comparisons
<sql_array_comparisons>`.


.. _sql_dql_any_array:

``ANY (array)``
---------------

The ANY (or SOME) :ref:`operator <gloss-operator>` allows you to query elements
within :ref:`arrays <sql_dql_arrays>`.

For example, this query returns any row where the array
``inhabitants['interests']`` contains a ``netball`` element::

    cr> select inhabitants['name'], inhabitants['interests'] from locations
    ... where 'netball' = ANY(inhabitants['interests']);
    +---------------------+------------------------------+
    | inhabitants['name'] | inhabitants['interests']     |
    +---------------------+------------------------------+
    | Minories            | ["netball", "short stories"] |
    | Bartledannians      | ["netball"]                  |
    +---------------------+------------------------------+
    SELECT 2 rows in set (... sec)

This query combines the ``ANY`` operator with the :ref:`LIKE <sql_dql_like>`
operator::

    cr> select inhabitants['name'], inhabitants['interests'] from locations
    ... where '%stories%' LIKE ANY(inhabitants['interests']);
    +---------------------+------------------------------+
    | inhabitants['name'] | inhabitants['interests']     |
    +---------------------+------------------------------+
    | Minories            | ["netball", "short stories"] |
    +---------------------+------------------------------+
    SELECT 1 row in set (... sec)

This query passes a literal array value to the ``ANY`` operator::

    cr> select name, inhabitants['interests'] from locations
    ... where name = ANY(ARRAY['Bartledan', 'Algol'])
    ... order by name asc;
    +-----------+--------------------------+
    | name      | inhabitants['interests'] |
    +-----------+--------------------------+
    | Algol     | NULL                     |
    | Bartledan | ["netball"]              |
    +-----------+--------------------------+
    SELECT 2 rows in set (... sec)

This query selects any locations with at least one (i.e., :ref:`ANY
<sql_dql_any_array>`) population figure above 100::

    cr> select name, information['population'] from locations
    ... where 100 < ANY (information['population'])
    ... order by name;
    +-------------------+---------------------------+
    | name              | information['population'] |
    +-------------------+---------------------------+
    | North West Ripple | [12, 163]                 |
    | Outer Eastern Rim | [5673745846]              |
    +-------------------+---------------------------+
    SELECT 2 rows in set (... sec)

.. NOTE::

    It is possible to use ``ANY`` to compare values directly against the
    properties of object arrays, as above. However, this usage is discouraged
    as it cannot utilize the table index and requires the equivalent of a table
    scan.

The ``ANY`` operator can be used in :ref:`subquery expressions
<sql_subquery_expressions>` and :ref:`array comparisons
<sql_array_comparisons>`.


.. _sql_dql_negating_any:

Negating ``ANY``
~~~~~~~~~~~~~~~~

Negating the ``ANY`` operator does not behave like other comparison operators.

The following query negates ``ANY`` using ``!=`` to return all rows where
``inhabitants['interests']`` has *at least one* :ref:`array <sql_dql_arrays>`
element that is not ``netball``::

    cr> select inhabitants['name'], inhabitants['interests'] from locations
    ... where 'netball' != ANY(inhabitants['interests']);
    +---------------------+------------------------------+
    | inhabitants['name'] | inhabitants['interests']     |
    +---------------------+------------------------------+
    | Minories            | ["netball", "short stories"] |
    | Argabuthonians      | ["science", "reason"]        |
    +---------------------+------------------------------+
    SELECT 2 rows in set (... sec)

.. NOTE::

    When using the ``!= ANY(<array_col>))`` syntax, the default maximum size of
    the array can be 8192. To use larger arrays, you must configure the
    :ref:`indices.query.bool.max_clause_count
    <indices.query.bool.max_clause_count>` setting as appropriate on each node.

Negating the same query with a preceding ``not`` returns all rows where
``inhabitants['interests']`` has no ``netball`` element::

    cr> select inhabitants['name'], inhabitants['interests'] from locations
    ... where not 'netball' = ANY(inhabitants['interests']);
    +---------------------+--------------------------+
    | inhabitants['name'] | inhabitants['interests'] |
    +---------------------+--------------------------+
    | Argabuthonians      | ["science", "reason"]    |
    +---------------------+--------------------------+
    SELECT 1 row in set (... sec)

This behaviour applies to:

 - ``LIKE`` and ``NOT LIKE``

 - All other comparison operators (excluding ``IS NULL`` and ``IS NOT NULL``)

.. NOTE::

    When using the ``NOT`` with ``ANY``, the performance of the query may be
    poor because special handling is required to implement the `3-valued
    logic`_. For better performance, consider using the :ref:`ignore3vl
    <scalar-ignore3vl>` function.

    Additionally, When using ``NOT`` with ``LIKE ANY`` or ``NOT LIKE ANY``, the
    default maximum size of the array can be 8192. To use larger arrays, you
    must configure the :ref:`indices.query.bool.max_clause_count
    <indices.query.bool.max_clause_count>` setting as appropriate on each node.


.. _sql_dql_container:

Container data types
====================


.. _sql_dql_arrays:

Arrays
------

CrateDB supports :ref:`arrays <data-types-arrays>`. It is possible to select and
query array elements.

For example, you might :ref:`insert <dml-inserting-data>` an array like so::

    cr> insert into locations (id, name, position, kind, landmarks)
    ... values (14, 'Frogstar', 4, 'Star System',
    ...     ['Total Perspective Vortex', 'Milliways']
    ... );
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh locations

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

The result::

    cr> select name, landmarks from locations
    ... where name = 'Frogstar';
    +----------+-------------------------------------------+
    | name     | landmarks                                 |
    +----------+-------------------------------------------+
    | Frogstar | ["Total Perspective Vortex", "Milliways"] |
    +----------+-------------------------------------------+
    SELECT 1 row in set (... sec)

The individual array elements can be selected from the ``landmarks`` column
with ``landmarks[n]``, where ``n`` is the integer array index, like so::

    cr> select name, landmarks[1] from locations
    ... where name = 'Frogstar';
    +----------+--------------------------+
    | name     | landmarks[1]             |
    +----------+--------------------------+
    | Frogstar | Total Perspective Vortex |
    +----------+--------------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    The first index value is ``1``. The maximum array index is ``2147483648``.
    Using an index greater than the array size results in a NULL value.

Individual array elements can also be addressed in the :ref:`where clause
<sql_dql_where_clause>`, like so::

    cr> select name, landmarks from locations
    ... where landmarks[2] = 'Milliways';
    +----------+-------------------------------------------+
    | name     | landmarks                                 |
    +----------+-------------------------------------------+
    | Frogstar | ["Total Perspective Vortex", "Milliways"] |
    +----------+-------------------------------------------+
    SELECT 1 row in set (... sec)

When using the ``=`` :ref:`operator <gloss-operator>`, as above, the value of
the array element at index ``n`` is compared. To compare against *any* array
element, see :ref:`sql_dql_any_array`.


.. _sql_dql_objects:

Objects
-------

CrateDB supports :ref:`objects <data-types-objects>`. It is possible to select
and query object properties.

For example, you might insert an object like so::

    cr> insert into locations (id, name, position, kind, inhabitants)
    ... values (15, 'Betelgeuse', 2, 'Star System',
    ...     {name = 'Betelgeuseans',
    ...      description = 'Humanoids with two heads'}
    ... );
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh locations

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

The result::

    cr> select name, inhabitants from locations
    ... where name = 'Betelgeuse';
    +------------+----------------------------------------------------------------------+
    | name       | inhabitants                                                          |
    +------------+----------------------------------------------------------------------+
    | Betelgeuse | {"description": "Humanoids with two heads", "name": "Betelgeuseans"} |
    +------------+----------------------------------------------------------------------+
    SELECT 1 row in set (... sec)

The object properties can be selected from the ``inhabitants`` column with
``inhabitants['property']``, where ``property`` is the property name, like so::

    cr> select name, inhabitants['name'] from locations
    ... where name = 'Betelgeuse';
    +------------+---------------------+
    | name       | inhabitants['name'] |
    +------------+---------------------+
    | Betelgeuse | Betelgeuseans       |
    +------------+---------------------+
    SELECT 1 row in set (... sec)

Object property can also be addressed in the :ref:`where clause
<sql_dql_where_clause>`, like so::

    cr> select name, inhabitants from locations
    ... where inhabitants['name'] = 'Betelgeuseans';
    +------------+----------------------------------------------------------------------+
    | name       | inhabitants                                                          |
    +------------+----------------------------------------------------------------------+
    | Betelgeuse | {"description": "Humanoids with two heads", "name": "Betelgeuseans"} |
    +------------+----------------------------------------------------------------------+
    SELECT 1 row in set (... sec)


.. _sql_dql_nested:

Nested structures
-----------------

Objects may contain arrays and arrays may contain objects. These nested
structures can be selected and queried.

For example, you might insert something like this::

    cr> insert into locations (id, name, position, kind, inhabitants, information)
    ... values (16, 'Folfanga', 4, 'Star System',
    ...     {name = 'A-Rth-Urp-Hil-Ipdenu',
    ...      description = 'A species of small slug',
    ...      interests = ['lettuce', 'slime']},
    ...     [{evolution_level=42, population=1},
    ...     {evolution_level=6, population=3600001}]
    ... );
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh locations

    cr> refresh table locations;
    REFRESH OK, 1 row affected (... sec)

The query above includes:

.. rst-class:: open

* An array nested within an object. Specifically, the ``inhabitants`` column
  contains an *parent object* with an ``interests`` property set to a *child
  array* of strings (e.g., ``lettuce``).

* Objects nested within an array. Specifically, the ``information`` column
  contains a *parent array* with two *child objects* (e.g.,
  ``{evolution_level=42, population=1}``).


.. _sql_dql_object_arrays:

Arrays within objects
~~~~~~~~~~~~~~~~~~~~~

The *child array* (:ref:`above <sql_dql_nested>`) can be selected as a
:ref:`property <sql_dql_objects>` of the *parent object*::

      cr> select name, inhabitants['interests'] from locations
      ... where name = 'Folfanga';
      +----------+--------------------------+
      | name     | inhabitants['interests'] |
      +----------+--------------------------+
      | Folfanga | ["lettuce", "slime"]     |
      +----------+--------------------------+
      SELECT 1 row in set (... sec)

Individual elements of the *child array* can be selected by combining the
:ref:`array index <sql_dql_objects>` syntax with the object :ref:`property name
<sql_dql_objects>` syntax, like so::

      cr> select name, inhabitants[1]['interests'] from locations
      ... where name = 'Folfanga';
      +----------+-----------------------------+
      | name     | inhabitants[1]['interests'] |
      +----------+-----------------------------+
      | Folfanga | lettuce                     |
      +----------+-----------------------------+
      SELECT 1 row in set (... sec)

.. CAUTION::

    The example above might surprise you because the child array index comes
    before the parent object property name, which doesn't follow the usual
    left-to-right convention for addressing the contents of a nested structure.

    Due to an implementation quirk in early versions of CrateDB, the array
    index always comes first (see :ref:`the next subsection
    <sql_dql_object_arrays_limitations>` for more information). Support for a
    more traditional left-to-right syntax may be added in the future.


.. _sql_dql_object_arrays_limitations:

Limitations
^^^^^^^^^^^

There are two limitations to be aware of:

.. rst-class:: open

* You cannot directly nest an array within an array (i.e., ``array(array(...)``
  is not a valid column definition). You can, however, nest multiple arrays as
  long as an object comes between them (e.g., ``array(object as (array(...)))``
  is a valid).

* Using the standard syntax, you can only address the elements of one array in
  a single :ref:`expression <gloss-expression>`. If you do address the elements
  of an array, the array index must appear before any object property names
  (see :ref:`the previous admonition <sql_dql_object_arrays>` for more
  information).

.. TIP::

    If you want to address the elements of more than one array in a single
    expression, you can use the following non-standard syntax::

        select foo[n1]['bar']::text[][n2] from my_table;

    Here, ``n1`` is the index of the first array (column ``foo``) and ``n2`` is
    the index of the second array (object property ``bar``).

    This works by:

    1. :ref:`Type casting <data-types-casting>` the second array (i.e.,
       ``foo[n1]['bar']``) to a string using the ``<expression>::text`` syntax,
       which is equivalent to ``cast(<expression> as text)``

    2. Creating a temporary :ref:`array <data-types-arrays>` (in-memory and
       addressable) from that string using the ``<expression>[]`` syntax, which
       is equivalent to ``array(expression``)

    *Note: Because this syntax effectively circumvents the index, it may
    considerably degrade query performance.*


.. _sql_dql_array_objects:

Objects within arrays
~~~~~~~~~~~~~~~~~~~~~

An individual *child object* (:ref:`above <sql_dql_nested>`) can be selected
from a *parent array* as an array element using the :ref:`array index
<sql_dql_arrays>` syntax::

    cr> select name, information[1] from locations
    ... where name = 'Outer Eastern Rim';
    +-------------------+--------------------------------------------------+
    | name              | information[1]                                   |
    +-------------------+--------------------------------------------------+
    | Outer Eastern Rim | {"evolution_level": 2, "population": 5673745846} |
    +-------------------+--------------------------------------------------+
    SELECT 1 row in set (... sec)

Properties of individual *child objects* can be selected by combining the
:ref:`array index <sql_dql_objects>` syntax with the object :ref:`property name
<sql_dql_objects>` syntax, like so::

    cr> select name, information[1]['population'] from locations
    ... where name = 'Outer Eastern Rim';
    +-------------------+------------------------------+
    | name              | information[1]['population'] |
    +-------------------+------------------------------+
    | Outer Eastern Rim |                   5673745846 |
    +-------------------+------------------------------+
    SELECT 1 row in set (... sec)

Additionally, consider this data::

    cr> select name, information from locations
    ... where information['population'] is not null;
    +-------------------+-------------------------------------------------------------------------------------------+
    | name              | information                                                                               |
    +-------------------+-------------------------------------------------------------------------------------------+
    | North West Ripple | [{"evolution_level": 4, "population": 12}, {"evolution_level": 42, "population": 163}]    |
    | Outer Eastern Rim | [{"evolution_level": 2, "population": 5673745846}]                                        |
    | Folfanga          | [{"evolution_level": 42, "population": 1}, {"evolution_level": 6, "population": 3600001}] |
    +-------------------+-------------------------------------------------------------------------------------------+
    SELECT 3 rows in set (... sec)

If you're only interested in one property of each object (e.g., population),
you can select a virtual array containing all of the values for that property,
like so::

    cr> select name, information['population'] from locations
    ... where information['population'] is not null;
    +-------------------+---------------------------+
    | name              | information['population'] |
    +-------------------+---------------------------+
    | North West Ripple | [12, 163]                 |
    | Outer Eastern Rim | [5673745846]              |
    | Folfanga          | [1, 3600001]              |
    +-------------------+---------------------------+
    SELECT 3 rows in set (... sec)


.. _sql_dql_aggregation:

Aggregation
===========

CrateDB provides built-in :ref:`aggregation functions <aggregation>` that allow
you to calculate a single summary value for one or more columns::

    cr> select count(*) from locations;
    +----------+
    | count(*) |
    +----------+
    |       16 |
    +----------+
    SELECT 1 row in set (... sec)


Window functions
================

CrateDB supports the :ref:`OVER <window-definition-over>` clause to enable the
execution of :ref:`window functions <window-functions>`::

    cr> select sum(position) OVER() AS pos_sum, name from locations order by name;
    +---------+------------------------------------+
    | pos_sum | name                               |
    +---------+------------------------------------+
    |      48 |                                    |
    |      48 | Aldebaran                          |
    |      48 | Algol                              |
    |      48 | Allosimanius Syneca                |
    |      48 | Alpha Centauri                     |
    |      48 | Altair                             |
    |      48 | Argabuthon                         |
    |      48 | Arkintoofle Minor                  |
    |      48 | Bartledan                          |
    |      48 | Betelgeuse                         |
    |      48 | Folfanga                           |
    |      48 | Frogstar                           |
    |      48 | Galactic Sector QQ7 Active J Gamma |
    |      48 | North West Ripple                  |
    |      48 | Outer Eastern Rim                  |
    |      48 | NULL                               |
    +---------+------------------------------------+
    SELECT 16 rows in set (... sec)


.. _sql_dql_group_by:

``GROUP BY``
============

CrateDB supports the ``GROUP BY`` clause. This clause can be used to group the
resulting rows by the value(s) of one or more columns. That means that rows
that contain duplicate values will be merged.

This is useful if used in conjunction with :ref:`aggregation functions
<aggregation-functions>`::

    cr> select count(*), kind from locations
    ... group by kind order by count(*) desc, kind asc;
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    |        7 | Star System |
    |        5 | Planet      |
    |        4 | Galaxy      |
    +----------+-------------+
    SELECT 3 rows in set (... sec)

.. NOTE::

   All columns that are used either as result column or in the order by clause
   have to be used within the group by clause. Otherwise the statement won't
   execute.

   Grouping will be executed against the real table column when aliases that
   shadow the table columns are used.

   Grouping on array columns doesn't work, but arrays can be unnested in a
   :ref:`subquery <gloss-subquery>` using :ref:`unnest <unnest>`. It is then
   possible to use ``GROUP BY`` on the subquery.


.. _sql_dql_having:

``HAVING``
----------

The ``HAVING`` clause is the equivalent to the ``WHERE`` clause for the
resulting rows of a ``GROUP BY`` clause.

A simple ``HAVING`` clause example using an equality :ref:`operator
<gloss-operator>`::

    cr> select count(*), kind from locations
    ... group by kind having count(*) = 4 order by kind;
    +----------+--------+
    | count(*) | kind   |
    +----------+--------+
    |        4 | Galaxy |
    +----------+--------+
    SELECT 1 row in set (... sec)

The condition of the ``HAVING`` clause can refer to the resulting columns of
the ``GROUP BY`` clause.

It is also possible to use :ref:`aggregate functions <aggregation-functions>`
in the ``HAVING`` clause, like in the result columns::

    cr> select count(*), kind from locations
    ... group by kind having min(name) = 'Aldebaran';
    +----------+-------------+
    | count(*) | kind        |
    +----------+-------------+
    |        7 | Star System |
    +----------+-------------+
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

   Aliases are not supported in the ``HAVING`` clause.


.. _`3-valued logic`: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
.. _Lucene Regular Expressions: http://lucene.apache.org/core/4_9_0/core/org/apache/lucene/util/automaton/RegExp.html
.. _PCRE: https://en.wikipedia.org/wiki/Perl_Compatible_Regular_Expressions
.. _POSIX Extended Regular Expressions: http://en.wikipedia.org/wiki/Regular_expression#POSIX_extended
