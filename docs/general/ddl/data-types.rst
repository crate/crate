.. highlight:: psql

.. _data-types:

==========
Data types
==========

Data can be stored in different formats. CrateDB has different types that can
be specified if a table is created using the the :ref:`ref-create-table`
statement. Data types play a central role as they limit what kind of data can
be inserted, how it is stored and they also influence the behaviour when the
records are queried.

Data type names are reserved words and need to be escaped when used as column
names.

.. rubric:: Table of contents

.. contents::
   :local:

Classification
==============

.. _sql_ddl_datatypes_primitives:

Primitive types
---------------

Primitive types represent primitive values.

These are values that are atomic, not composed of separate parts, no containers
or collections.

* `boolean`_
* `char <special character types_>`_
* `smallint <numeric types_>`_
* `integer <numeric types_>`_
* `bigint <numeric types_>`_
* `real <numeric types_>`_
* `double precision <numeric types_>`_
* `text`_
* `ip`_
* `timestamp with time zone <timestamp with time zone_>`_
* `timestamp without time zone <timestamp without time zone_>`_
* `interval`_

.. _sql_ddl_datatypes_geographic:

Geographic types
----------------

Geographic types represent points or shapes in a 2d world:

* `geo_point`_
* `geo_shape`_

.. _sql_ddl_datatypes_compound:

Compound types
--------------

Compound types represent values that are composed out of distinct parts like
containers or collections:

* `object`_
* `array`_

``boolean``
===========

A basic boolean type. Accepting ``true`` and ``false`` as values. Example::

    cr> create table my_bool_table (
    ...   first_column boolean
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_bool_table;
    DROP OK, 1 row affected (... sec)

.. _data-type-text:

``text``
========

A text-based basic type containing one or more characters. All unicode
characters are allowed. Example::

    cr> create table my_table2 (
    ...   first_column text
    ... );
    CREATE OK, 1 row affected (... sec)

Columns of type string can also be analyzed. See :ref:`sql_ddl_index_fulltext`.

.. NOTE::

   Maximum indexed string length is restricted to 32766 bytes, when encoded
   with UTF-8 unless the string is analyzed using full text or indexing and
   the usage of the :ref:`ddl-storage-columnstore` is disabled.

Numeric types
=============

CrateDB supports a set of the following numeric data types:

+----------------------+----------+-----------------------------+-----------------------------+
| Name                 | Size     | Description                 | Range                       |
+======================+==========+=============================+=============================+
| ``smallint``         | 2 bytes  | small-range integer         | -32,768 to 32,767           |
+----------------------+----------+-----------------------------+-----------------------------+
| ``integer``          | 4 bytes  | integer                     | -2^31 to 2^31-1.            |
+----------------------+----------+-----------------------------+-----------------------------+
| ``bigint``           | 8 bytes  | large-range integer         | -2^63 to 2^63-1             |
+----------------------+----------+-----------------------------+-----------------------------+
| ``real``             | 4 bytes  | inexact, variable-precision | 6 decimal digits precision  |
+----------------------+----------+-----------------------------+-----------------------------+
| ``double precision`` | 8 bytes  | inexact, variable-precision | 15 decimal digits precision |
+----------------------+----------+-----------------------------+-----------------------------+

The ``real`` and ``double precision`` data types are inexact, variable-precision
numeric types. It means that these types are stored as an approximation.
Therefore, storage, calculation, and retrieval of the value will not always
result in an exact representation of the actual floating-point value.

For instance, the result of applying ``sum`` or ``avg`` aggregate functions may
slightly vary between query executions or comparing floating-point values for
equality might not always be correct.

Special floating point values
-----------------------------

CrateDB conforms to the `IEEE 754`_ standard concerning special values for
``real`` and ``double precision`` floating point data types. This means that
it also supports  ``NaN``, ``Infinity``, ``-Infinity`` (negative infinity),
and ``-0`` (signed zero).

::

    cr> SELECT 0.0 / 0.0, 1.0 / 0.0, 1.0 / -0.0;
    +-------------+-------------+---------------+
    | (0.0 / 0.0) | (1.0 / 0.0) | (1.0 / - 0.0) |
    +-------------+-------------+---------------+
    | NaN         | Infinity    | -Infinity     |
    +-------------+-------------+---------------+
    SELECT 1 row in set (... sec)

These special numeric values can also be inserted into a column of type
``real`` or ``double precision`` using a ``text`` literal.

::

    cr> create table my_table3 (
    ...   first_column integer,
    ...   second_column bigint,
    ...   third_column smallint,
    ...   fourth_column double precision,
    ...   fifth_column real,
    ...   sixth_column char
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table3 (fourth_column, fifth_column)
    ... VALUES ('NaN', 'Infinity');
    INSERT OK, 1 row affected (... sec)


``ip``
======

The ``ip`` type allows to store IPv4 and IPv6 addresses by inserting their string
representation. Internally it maps to a ``bigint`` allowing expected sorting,
filtering, and aggregation.

Example::

    cr> create table my_table_ips (
    ...   fqdn string,
    ...   ip_addr ip
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into my_table_ips (fqdn, ip_addr)
    ... values ('localhost', '127.0.0.1'),
    ...        ('router.local', '0:0:0:0:0:ffff:c0a8:64');
    INSERT OK, 2 rows affected (... sec)

::

    cr> insert into my_table_ips (fqdn, ip_addr)
    ... values ('localhost', 'not.a.real.ip');
    SQLActionException[ColumnValidationException: Validation failed for ip_addr: Cannot cast 'not.a.real.ip' to type ip]

.. _date-time-types:

Date/Time types
===============

+---------------------------------+---------+-------------------------+--------------------+
| Name                            | Size    | Description             | Range              |
+=================================+=========+=========================+====================+
| ``timestamp with time zone``    | 8 bytes | time and date with time | ``292275054BC``    |
|                                 |         | zone                    | to ``292278993AD`` |
+---------------------------------+---------+-------------------------+--------------------+
| ``timestamp without time zone`` | 8 bytes | time and date without   | ``292275054BC``    |
|                                 |         | time zone               | to ``292278993AD`` |
+---------------------------------+---------+-------------------------+--------------------+

Timestamps
----------

The timestamp types consist of the concatenation of a date and time, followed
by an optional time zone.

Internally, timestamp values are mapped to the UTC milliseconds since
``1970-01-01T00:00:00Z`` stored as ``bigint``.

Timestamps are always returned as ``bigint`` values.

The syntax for timestamp string literals is as follows:

.. code-block:: text

    date-element [time-separator [time-element [offset]]]

    time-separator: 'T' | ' '
    date-element:   yyyy-MM-dd
    time-element:   HH:mm:ss [fraction]
    fraction:       '.' digit+
    offset:         {+ | -} HH [:mm] | 'Z'

For more detailed information about the date and time elements, see
`pattern letters and symbols`_.

.. CAUTION::

    When inserting timestamps smaller than ``-999999999999999`` (equals to
    ``-29719-04-05T22:13:20.001Z``) or bigger than ``999999999999999`` (equals to
    ``33658-09-27T01:46:39.999Z``) rounding issues may occur.

.. NOTE::

    Due to internal date parsing, not the full ``bigint`` range is supported for
    timestamp values, but only dates between year ``292275054BC`` and
    ``292278993AD``, which is slightly smaller.

.. _datetime-with-time-zone:

``timestamp with time zone``
............................

A string literal that contain a timestamp value with the time zone will be
converted to UTC considering its offset for the time zone.

::

    cr> select '1970-01-02T00:00:00+0100'::timestamp with time zone as ts_z,
    ...        '1970-01-02T00:00:00Z'::timestamp with time zone ts_z,
    ...        '1970-01-02T00:00:00'::timestamp with time zone ts_z,
    ...        '1970-01-02 00:00:00'::timestamp with time zone ts_z_sql_format;
    +----------+----------+----------+-----------------+
    |     ts_z |     ts_z |     ts_z | ts_z_sql_format |
    +----------+----------+----------+-----------------+
    | 82800000 | 86400000 | 86400000 |        86400000 |
    +----------+----------+----------+-----------------+
    SELECT 1 row in set (... sec)


Timestamps will also accept a ``bigint`` representing UTC milliseconds since
the epoch or a ``real`` or ``double precision`` representing UTC seconds since
the epoch with milliseconds as fractions.

::

    cr> select 1.0::timestamp with time zone;
    +---------------------------------------+
    | CAST(1.0 AS timestamp with time zone) |
    +---------------------------------------+
    |                                  1000 |
    +---------------------------------------+
    SELECT 1 row in set (... sec)


.. _datetime-without-time-zone:

``timestamp without time zone``
...............................

A string literal that contain a timestamp value with the time zone will be
converted to UTC without considering the time zone indication.

::

    cr> select '1970-01-02T00:00:00+0200'::timestamp without time zone as ts,
    ...        '1970-01-02T00:00:00+0400'::timestamp without time zone as ts,
    ...        '1970-01-02T00:00:00Z'::timestamp without time zone as ts,
    ...        '1970-01-02 00:00:00Z'::timestamp without time zone as ts_sql_format;
    +----------+----------+----------+---------------+
    |       ts |       ts |       ts | ts_sql_format |
    +----------+----------+----------+---------------+
    | 86400000 | 86400000 | 86400000 |      86400000 |
    +----------+----------+----------+---------------+
    SELECT 1 row in set (... sec)


.. NOTE::

    If a column is dynamically created the type detection won't recognize
    date time types. That means date type columns must always be declared
    beforehand.

.. _timestamp-at-time-zone:

``timestamp with/without time zone AT TIME ZONE zone``
......................................................

AT TIME ZONE converts a timestamp without time zone to/from a timestamp with
time zone. It has the following variants:

.. csv-table::
   :header: "Expression", "Return Type", "Description"

   "timestamp without time zone AT TIME ZONE zone", "timestamp with time zone", "Treat \
   given time stamp without time zone as located in the specified time zone"
   "timestamp with time zone AT TIME ZONE zone", "timestamp without time zone", "Convert \
   given time stamp with time zone to the new time zone, with no time zone designation"

In these expressions, the desired time zone is specified as a string
(e.g., 'Europe/Madrid', '+02:00'). See :ref:`Timezone <date-format-timezone>`.

The scalar function :ref:`TIMEZONE <scalar-timezone>` (zone, timestamp) is
equivalent to the SQL-conforming construct timestamp AT TIME ZONE zone.

.. _interval_data_type:

Interval
--------


.. _interval-literal:

Interval literal
................

An interval literal represents a span of time and can be either
a :ref:`year-month-literal` or :ref:`day-time-literal` literal. The generic
literal synopsis defined as following

::

    <interval_literal> ::=
        INTERVAL [ <sign> ] <string_literal> <interval_qualifier>

    <interval_qualifier> ::=
        <start_field> [ TO <end_field>]

    <start_field> ::= <datetime_field>
    <end_field> ::= <datetime_field>

    <datetime_field> ::=
          YEAR
        | MONTH
        | DAY
        | HOUR
        | MINUTE
        | SECOND

.. _year-month-literal:

year-month
^^^^^^^^^^

A ``year-month`` literal includes either ``YEAR``, ``MONTH`` or a contiguous
subset of these fields.

::

    <year_month_literal> ::=
        INTERVAL [ {+ | -} ]'yy' <interval_qualifier> |
        INTERVAL [ {+ | -} ]'[ yy- ] mm' <interval_qualifier>

For example::

    cr> select INTERVAL '01-02' YEAR TO MONTH;
    +--------------------------------+
    | INTERVAL '01-02' YEAR TO MONTH |
    +--------------------------------+
    | 1 year 2 mons 00:00:00         |
    +--------------------------------+
    SELECT 1 row in set (... sec)

.. _day-time-literal:

day-time
^^^^^^^^

A ``day-time`` literal includes either ``DAY``, ``HOUR``, ``MINUTE``,
``SECOND`` or a contiguous subset of these fields.

When using ``SECOND``, it is possible to define more digits representing
a number of fractions of a seconds with ``.nn``. The allowed fractional
seconds precision of ``SECOND`` ranges from 0 to 6 digits.

::

    <day_time_literal> ::=
        INTERVAL [ {+ | -} ]'dd [ <space> hh [ :mm [ :ss ]]]' <interval_qualifier>
        INTERVAL [ {+ | -} ]'hh [ :mm [ :ss [ .nn ]]]' <interval_qualifier>
        INTERVAL [ {+ | -} ]'mm [ :ss [ .nn ]]' <interval_qualifier>
        INTERVAL [ {+ | -} ]'ss [ .nn ]' <interval_qualifier>

For example::

    cr> select INTERVAL '10 23:10' DAY TO MINUTE;
    +-----------------------------------+
    | INTERVAL '10 23:10' DAY TO MINUTE |
    +-----------------------------------+
    | 1 weeks 3 days 23:10:00           |
    +-----------------------------------+
    SELECT 1 row in set (... sec)


.. _string-literal:

string-literal
^^^^^^^^^^^^^^

An interval ``string-literal`` can be defined by a combination of
:ref:`day-time-literal <day-time-literal>` and
:ref:`year-month-literal <year-month-literal>`
or using the :ref:`iso-8601-format <iso-8601-format>` or
:ref:`PostgreSQL-format <postgresql-format>`.

For example::

    cr> select INTERVAL '1-2 3 4:5:6';
    +---------------------------------+
    | CAST('1-2 3 4:5:6' AS interval) |
    +---------------------------------+
    | 1 year 2 mons 3 days 04:05:06   |
    +---------------------------------+
    SELECT 1 row in set (... sec)


.. _iso-8601-format:

ISO-8601 format
"""""""""""""""

The iso-8601 format describes a duration of time using the
`ISO 8601 duration format`_ syntax.

For example::

    cr> select INTERVAL 'P1Y2M3DT4H5M6S';
    +------------------------------------+
    | CAST('P1Y2M3DT4H5M6S' AS interval) |
    +------------------------------------+
    | 1 year 2 mons 3 days 04:05:06      |
    +------------------------------------+
    SELECT 1 row in set (... sec)


.. _postgresql-format:

PostgreSQL format
"""""""""""""""""

The ``PostgreSQL`` format describes a duration of time using the `PostgreSQL interval format`_ syntax.

For example::

    cr> select INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';
    +------------------------------------------------------------------------+
    | CAST('1 year 2 months 3 days 4 hours 5 minutes 6 seconds' AS interval) |
    +------------------------------------------------------------------------+
    | 1 year 2 mons 3 days 04:05:06                                          |
    +------------------------------------------------------------------------+
    SELECT 1 row in set (... sec)


.. _temporal-arithmetic:

Temporal arithmetic
-------------------

The following table specifies the declared types of
:ref:`arithmetic <arithmetic>` expressions that involves temporal operands.

+---------------+----------+---------------+
|       Operand | Operator |       Operand |
+===============+==========+===============+
| ``timestamp`` |       \- | ``timestamp`` |
+---------------+----------+---------------+
|  ``interval`` |       \+ | ``timestamp`` |
+---------------+----------+---------------+
| ``timestamp`` | \+ or \- |  ``interval`` |
+---------------+----------+---------------+
|  ``interval`` | \+ or \- |  ``interval`` |
+---------------+----------+---------------+


.. _geo_point_data_type:

``geo_point``
=============

The ``geo_point`` type is used to store latitude and longitude geo coordinates.

Columns with the ``geo_point`` type are represented and inserted using an array
of doubles in the following format::

    [<lon_value>, <lat_value>]

Alternatively a `WKT`_ string can also be used to declare geo points::

    'POINT ( <lon_value> <lat_value> )'

.. NOTE::

    Empty geo points are not supported.

    Additionally, if a column is dynamically created the type detection won't
    recognize neither WKT strings nor double arrays. That means columns of type
    geo_point must always be declared beforehand.

Create table example::

    cr> create table my_table_geopoint (
    ...   id integer primary key,
    ...   pin geo_point
    ... ) with (number_of_replicas = 0)
    CREATE OK, 1 row affected (... sec)

.. _geo_shape_data_type:

``geo_shape``
=============

The ``geo_shape`` type is used to store geometric shapes defined as `GeoJSON
geometry objects`_.

A geo_shape column can store different kinds of `GeoJSON geometry objects`_.
Thus it is possible to store e.g. ``LineString`` and ``MultiPolygon`` shapes in
the same column.

.. NOTE::

    3D coordinates are not supported.

    Empty ``Polygon`` and ``LineString`` geo shapes are not supported.

Definition
----------

To define a geo_shape column::

    <columnName> geo_shape

A geographical index with default parameters is created implicitly to allow for
geographical queries.

The default definition for the column type is::

    <columnName> geo_shape INDEX USING geohash WITH (precision='50m', distance_error_pct=0.025)

There are two geographic index types: ``geohash`` (the default) and
``quadtree``. These indices are only allowed on geo_shape columns. For more
information, see :ref:`geo_shape_data_type_index`.

Both of these index types accept the following parameters:

:precision:
  (Default: ``50m``) Define the maximum precision of the used index and
  thus for all indexed shapes. Given as string containing a number and
  an optional distance unit (defaults to ``m``).

  Supported units are ``inch`` (``in``), ``yard`` (``yd``), ``miles``
  (``mi``), ``kilometers`` (``km``), ``meters`` (``m``), ``centimeters``
  (``cm``), ``millimeters`` (``mm``).

:distance_error_pct:
  (Default: ``0.025`` (2,5%)) The measure of acceptable error for shapes
  stored in this column expressed as a percentage value of the shape
  size The allowed maximum is ``0.5`` (50%).

  The percentage will be taken from the diagonal distance from the
  center of the bounding box enclosing the shape to the closest corner
  of the enclosing box. In effect bigger shapes will be indexed with
  lower precision than smaller shapes. The ratio of precision loss is
  determined by this setting, that means the higher the
  ``distance_error_pct`` the smaller the indexing precision.

  This will have the effect of increasing the indexed shape internally,
  so e.g. points that are not exactly inside this shape will end up
  inside it when it comes to querying as the shape has grown when
  indexed.

:tree_levels:
  Maximum number of layers to be used by the ``PrefixTree`` defined by
  the index type (either ``geohash`` or ``quadtree``. See
  :ref:`geo_shape_data_type_index`).

  This can be used to control the precision of the used index. Since
  this parameter requires a certain level of understanting of the
  underlying implementation, users may use the ``precision`` parameter
  instead. CrateDB uses the ``tree_levels`` parameter internally and
  this is what is returned via the ``SHOW CREATE TABLE`` statement even
  if you use the precision parameter. Defaults to the value which is
  ``50m`` converted to ``precision`` depending on the index type.

.. _geo_shape_data_type_index:

Geo shape index structure
-------------------------

Computations on very complex polygons and geometry collections are exact but
very expensive. To provide fast queries even on complex shapes, CrateDB uses a
different approach to store, analyze and query geo shapes.

The surface of the earth is represented as a number of grid layers each with
higher precision. While the upper layer has one grid cell, the layer below
contains many cells for the equivalent space.

Each grid cell on each layer is addressed in 2d space either by a `Geohash`_
for ``geohash`` trees or by tightly packed coordinates in a `Quadtree`_. Those
addresses conveniently share the same address-prefix between lower layers and
upper layers. So we are able to use a `Trie`_ to represent the grids, and
`Tries`_ can be queried efficiently as their complexity is determined by the
tree depth only.

A geo shape is transformed into these grid cells. Think of this transformation
process as dissecting a vector image into its pixelated counterpart, reasonably
accurately. We end up with multiple images each with a better resolution, up to
the configured precision.

Every grid cell that processed up to the configured precision is stored in an
inverted index, creating a mapping from a grid cell to all shapes that touch
it. This mapping is our geographic index.

The main difference is that the ``geohash`` supports higher precision than the
``quadtree`` tree. Both tree implementations support precision in order of
fractions of millimeters.

Representation
--------------

Columns with the ``geo_shape`` type are represented and inserted as object
containing a valid `GeoJSON`_ geometry object::

    {
      type = 'Polygon',
      coordinates = [
         [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
      ]
    }

Alternatively a `WKT`_ string can be used to represent a geo_shape as well::

    'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'

.. NOTE::

    It is not possible to detect a geo_shape type for a dynamically created
    column. Like with :ref:`geo_point_data_type` type, geo_shape columns need
    to be created explicitly using either :ref:`ref-create-table` or
    :ref:`ref-alter-table`.

.. _object_data_type:

``object``
==========

The object type allows to define nested documents instead of old-n-busted flat
tables.

An ``object`` can contain other fields of any type, even further object
columns. An ``object`` column can be either schemaless or enforce its defined
schema. It can even be used as a kind of json-blob.

Syntax::

    <columnName> OBJECT [ ({DYNAMIC|STRICT|IGNORED}) ] [ AS ( <columnDefinition>* ) ]

The only required part of this column definition is ``OBJECT``.

The column policy defining this objects behaviour is optional, if left out
``DYNAMIC`` will be used.

The list of subcolumns is optional as well, if left out, this object will have
no schema (with a schema created on the fly on first inserts in case of
``DYNAMIC``).

Example::

    cr> create table my_table11 (
    ...   title text,
    ...   col1 object,
    ...   col3 object(strict) as (
    ...     age integer,
    ...     name text,
    ...     col31 object as (
    ...       birthday timestamp with time zone
    ...     )
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table11;
    DROP OK, 1 row affected (... sec)

``strict``
----------

The column policy can be configured to be ``strict``, rejecting any subcolumn
that is not defined upfront in the schema. As you might have guessed, defining
``strict`` objects without subcolumns results in an unusable column that will
always be null, which is the most useless column one could create.

Example::

    cr> create table my_table12 (
    ...   title text,
    ...   author object(strict) as (
    ...     name text,
    ...     birthday timestamp with time zone
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table12;
    DROP OK, 1 row affected (... sec)

``dynamic``
-----------

Another option is ``dynamic``, which means that new subcolumns can be added in this object.

Note that adding new columns to an object with a ``dynamic`` policy will affect
the schema of the table. Once a column is added, it shows up in the
``information_schema.columns`` table and its type and attributes are fixed.
They will have the type that was guessed by their inserted/updated value and
they will always be ``not_indexed`` which means they are analyzed with the
``plain`` analyzer, which means as-is.

If a new column ``a`` was added with type ``integer``, adding strings to this
column will result in an error.

Examples::

    cr> create table my_table13 (
    ...   title text,
    ...   author object as (
    ...     name text,
    ...     birthday timestamp with time zone
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table13;
    DROP OK, 1 row affected (... sec)

which is exactly the same as::

    cr> create table my_table14 (
    ...   title text,
    ...   author object(dynamic) as (
    ...     name text,
    ...     birthday timestamp with time zone
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table14;
    DROP OK, 1 row affected (... sec)

New columns added to ``dynamic`` objects are, once added, usable as usual
subcolumns. One can retrieve them, sort by them and use them in where clauses.

``ignored``
-----------

The third option is ``ignored`` which results in an object that allows
inserting new subcolumns but this adding will not affect the schema, they are
not mapped according to their type, which is therefor not guessed as well. You
can in fact add any value to an added column of the same name. The first value
added does not determine what you can add further, like with ``dynamic``
objects.

An object configured like this will simply accept and return the columns
inserted into it, but otherwise ignore them.

::

    cr> create table my_table15 (
    ...   title text,
    ...   details object(ignored) as (
    ...     num_pages integer,
    ...     font_size real
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> drop table my_table15;
    DROP OK, 1 row affected (... sec)

.. NOTE::

   ``Ignored`` objects should be mainly used for storing and fetching.
   Filtering by and ordering on them is possible but very performance
   intensive. ``Ignored`` objects are a *black box* for the storage engine, so
   the filtering/ordering is done using an expensive table scan and a
   filter/order function outside of the storage engine. Using ``ignored``
   objects for grouping or aggregations is not possible at all and will result
   in an exception or ``NULL`` value if used with excplicit casts.

.. _data-type-object-literals:

Object literals
---------------

To insert values into object columns one can use object literals or parameters.

.. NOTE::

   Even though they look like JSON - object literals are not JSON
   compatible.

Object literals are given in curly brackets. Key value pairs are connected via
``=``.

Synopsis::

    { [ ident = expr [ , ... ] ] }

The *key* of a key-value pair is an SQL identifier. That means every unquoted
identifier in an object literal key will be lowercased.

The *value* of a key-value pair is another literal or a parameter.

An object literal can contain zero or more key value pairs

Examples
........

Empty object literal::

  {}

Boolean type::

  { my_bool_column = true }

String type::

  { my_str_col = 'this is a string value' }

Number types::

  { my_int_col = 1234, my_float_col = 5.6 }

Array type::

  { my_array_column = ['v', 'a', 'l', 'u', 'e'] }

Camel case keys must be quoted::

  { "CamelCaseColumn" = 'this is a string value' }

Nested object::

  { nested_obj_colmn = { int_col = 1234, str_col = 'string value' } }

You can even specify a placeholder parameter for a value::

  { my_other_column = ? }

Combined::

  { id = 1, name = 'foo', tags = ['apple', 'banana', 'pear'], size = 3.1415, valid = ? }

.. _data-type-array:

``array``
=========

CrateDB supports arrays.

An array is a collection of other data types. These are:

* boolean
* text
* ip
* all numeric types (integer, bigint, smallint, double precision, real)
* char
* timestamp with time zone
* object
* geo_point

Array types are defined as follows::

    cr> create table my_table_arrays (
    ...     tags array(text),
    ...     objects array(object as (age integer, name text))
    ... );
    CREATE OK, 1 row affected (... sec)

.. NOTE::

    Currently arrays cannot be nested. Something like array(array(string))
    won't work.

.. _data-type-array-literals:

Array constructor
-----------------

Arrays can be written using the array constructor ``ARRAY[]`` or short ``[]``.
The array constructor is an expression that accepts both literals and
expressions as its parameters. Parameters may contain zero or more elements.

Synopsis::

    [ ARRAY ] '[' element [ , ... ] ']'

All array elements must have the same data type, which determines the inner
type of the array. If an array contains no elements, its element type will be
inferred by the context in which it occurs, if possible.

Examples
........

Some valid arrays are::

    []
    [null]
    [1, 2, 3, 4, 5, 6, 7, 8]
    ['Zaphod', 'Ford', 'Arthur']
    [?]
    ARRAY[true, false]
    ARRAY[column_a, column_b]
    ARRAY[ARRAY[1, 2, 1 + 2], ARRAY[3, 4, 3 + 4]]

Array representation
....................

Arrays are always represented as zero or more literal elements inside square
brackets (``[]``), for example::

    [1, 2, 3]
    ['Zaphod', 'Ford', 'Arthur']

Special character types
=======================

+----------+--------+------------------+
| Name     | Size   | Description      |
+==========+========+==================+
| ``char`` | 1 byte | single-byte type |
+----------+--------+------------------+

.. _type_conversion:

Type conversion
===============

.. _type_cast:

``CAST``
--------

A type ``cast`` specifies a conversion from one data type to another. It will
only succeed if the value of the expression is convertible to the desired data
type, otherwise an error is thrown.

CrateDB supports two equivalent syntaxes for type casts:

::

   cast(expression as type)
   expression::type

Example usages:

::

    cr> select cast(port['http'] as boolean) from sys.nodes limit 1;
    +-------------------------------+
    | CAST(port['http'] AS boolean) |
    +-------------------------------+
    | TRUE                          |
    +-------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select (2+10)/2::text;
    +------------------------------+
    | ((2 + 10) / CAST(2 AS text)) |
    +------------------------------+
    |                            6 |
    +------------------------------+
    SELECT 1 row in set (... sec)

It is also possible to convert array structures to different data types, e.g.
converting an array of integer values to a boolean array.

::

    cr> select cast([0,1,5] as array(boolean)) as
    ... active_threads from sys.nodes limit 1;
    +---------------------+
    | active_threads      |
    +---------------------+
    | [false, true, true] |
    +---------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

   It is not possible to cast to or from ``object`` and ``geopoint``, or to
   ``geoshape`` data type.

``TRY_CAST``
------------

While ``cast`` throws an error for incompatible type casts, ``try_cast``
returns ``null`` in this case. Otherwise the result is the same as with
``cast``.

::

   try_cast(expression as type)

Example usages:

::

    cr> select try_cast('true' as boolean) from sys.nodes limit 1;
    +-----------------------------+
    | TRY_CAST('true' AS boolean) |
    +-----------------------------+
    | TRUE                        |
    +-----------------------------+
    SELECT 1 row in set (... sec)

Trying to cast a ``text`` to ``integer``, will fail with ``cast`` if
``text`` is no valid integer but return ``null`` with ``try_cast``:

::

    cr> select try_cast(name as integer) from sys.nodes limit 1;
    +---------------------------+
    | TRY_CAST(name AS integer) |
    +---------------------------+
    | NULL                      |
    +---------------------------+
    SELECT 1 row in set (... sec)

.. _type_cast_from_string_literal:

``type 'string'``
-----------------

This cast operation is applied to a string literal and it effectively
initializes a constant of an arbitrary type.

Example usages, initializing an ``integer`` and a ``timestamp`` constant:

::

    cr> select integer '25';
    +-----------------------+
    | CAST('25' AS integer) |
    +-----------------------+
    |                    25 |
    +-----------------------+
    SELECT 1 row in set (... sec)

::

    cr> select timestamp with time zone '2029-12-12T11:44:00.24446';
    +---------------------------------------------------------------+
    | CAST('2029-12-12T11:44:00.24446' AS timestamp with time zone) |
    +---------------------------------------------------------------+
    |                                                 1891770240244 |
    +---------------------------------------------------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

  This cast operation is limited to :ref:`sql_ddl_datatypes_primitives` only.
  For complex types such as ``array`` or ``object`` use the
  :ref:`type_cast` syntax.

.. _data-type-aliases:

Type aliases
============

For compatibility with PostgreSQL we include some type aliases which can be
used instead of the CrateDB specific type names.

For example, in a type cast::

  cr> select 10::int2;
  +------------------+
  | CAST(10 AS int2) |
  +------------------+
  |               10 |
  +------------------+
  SELECT 1 row in set (... sec)


See the table below for a full list of aliases:

+-------------+--------------------------+
| Alias       | Crate Type               |
+=============+==========================+
| int2        | smallint                 |
+-------------+--------------------------+
| short       | smallint                 |
+-------------+--------------------------+
| int         | integer                  |
+-------------+--------------------------+
| int4        | integer                  |
+-------------+--------------------------+
| int8        | bigint                   |
+-------------+--------------------------+
| long        | bigint                   |
+-------------+--------------------------+
| string      | text                     |
+-------------+--------------------------+
| name        | text                     |
+-------------+--------------------------+
| byte        | char                     |
+-------------+--------------------------+
| float       | real                     |
+-------------+--------------------------+
| double      | double precision         |
+-------------+--------------------------+
| timestamp   | timestamp with time zone |
+-------------+--------------------------+
| timestamptz | timestamp with time zone |
+-------------+--------------------------+

.. NOTE::

   The :ref:`PG_TYPEOF <pg_typeof>` system function can be used to resolve the
   data type of any expression.

.. _pattern letters and symbols:
    https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html
.. _WKT: http://en.wikipedia.org/wiki/Well-known_text
.. _GeoJSON: http://geojson.org/
.. _GeoJSON geometry objects: https://tools.ietf.org/html/rfc7946#section-3.1
.. _Geohash: https://en.wikipedia.org/wiki/Geohash
.. _Quadtree: https://en.wikipedia.org/wiki/Quadtree
.. _Trie: https://en.wikipedia.org/wiki/Trie
.. _Tries: https://en.wikipedia.org/wiki/Trie
.. _IEEE 754: http://ieeexplore.ieee.org/document/30711/?arnumber=30711&filter=AND(p_Publication_Number:2355)
.. _PostgreSQL interval format: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
.. _ISO 8601 duration format: https://en.wikipedia.org/wiki/ISO_8601#Durations

