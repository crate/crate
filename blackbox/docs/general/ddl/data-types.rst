.. highlight:: psql

.. _data-types:

==========
Data Types
==========

Data can be stored in different formats. CrateDB has different types that can
be specified if a table is created using the the :ref:`ref-create-table`
statement. Data types play a central role as they limit what kind of data can
be inserted, how it is stored and they also influence the behaviour when the
records are queried.

Data type names are reserved words and need to be escaped when used as column
names.

.. rubric:: Table of Contents

.. contents::
   :local:

Classification
==============

.. _sql_ddl_datatypes_primitives:

Primitive Types
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
* `timestamp with time zone <date/time types_>`_

.. _sql_ddl_datatypes_geographic:

Geographic Types
----------------

Geographic types represent points or shapes in a 2d world:

* `geo_point`_
* `geo_shape`_

.. _sql_ddl_datatypes_compound:

Compound Types
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

Numeric Types
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

Special Floating Point Values
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

Date/Time Types
===============

+------------------------------+---------+------------------------------+------------------------------------+
| Name                         | Size    | Description                  | Range                              |
+==============================+=========+==============================+====================================+
| ``timestamp with time zone`` | 8 bytes | time and date with time zone | ``292275054BC`` to ``292278993AD`` |
+------------------------------+---------+------------------------------+------------------------------------+

``timestamp with time zone``
----------------------------

The timestamp type is a special type which maps to a formatted string.
Internally it maps to the UTC milliseconds since ``1970-01-01T00:00:00Z``
stored as ``bigint``. Timestamps are always returned as ``bigint`` values.

The default timestamp format is dateOptionalTime_ and cannot be changed
currently. Formatted string without timezone offset information will be treated
as UTC.

::

    cr> create table ts_table (ts_zone timestamp with time zone);
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into ts_table (ts_zone) values ('1970-01-02T00:00:00');
    INSERT OK, 1 row affected (... sec)

.. Hidden:

    cr> refresh table ts_table;
    REFRESH OK, 1 row affected  (... sec)

::

    cr> select * from ts_table;
    +----------+
    |  ts_zone |
    +----------+
    | 86400000 |
    +----------+
    SELECT 1 row in set (... sec)

Formatted date strings containing timezone offset information will be converted
to UTC.::

    cr> select '1970-01-02T00:00:00+0100'::timestamp with time zone;
    +--------------------------------------------------------------+
    | CAST('1970-01-02T00:00:00+0100' AS timestamp with time zone) |
    +--------------------------------------------------------------+
    |                                                     82800000 |
    +--------------------------------------------------------------+
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

Due to internal date parsing, not the full ``bigint`` range is supported for
timestamp values, but only dates between year ``292275054BC`` and
``292278993AD``, which is slightly smaller.


.. CAUTION::

    When inserting timestamps smaller than ``-999999999999999`` (equals to
    ``-29719-04-05T22:13:20.001Z``) or bigger than ``999999999999999`` (equals to
    ``33658-09-27T01:46:39.999Z``) rounding issues may occur.

.. NOTE::

    If a column is dynamically created the type detection won't recognize
    timestamps. That means columns of type timestamp must always be declared
    beforehand.

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

Geo Shape Index Structure
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

Object Literals
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

Array Constructor
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

Array Representation
....................

Arrays are always represented as zero or more literal elements inside square
brackets (``[]``), for example::

    [1, 2, 3]
    ['Zaphod', 'Ford', 'Arthur']

Special Character Types
=======================

+----------+--------+------------------+
| Name     | Size   | Description      |
+==========+========+==================+
| ``char`` | 1 byte | single-byte type |
+----------+--------+------------------+

.. _type_conversion:

Type Conversion
===============

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
    | CAST(((2 + 10) / 2) AS text) |
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

+----------+------------------+
| Alias    | Crate Type       |
+==========+==================+
| int2     | smallint         |
+----------+------------------+
| short    | smallint         |
+----------+------------------+
| int      | integer          |
+----------+------------------+
| int4     | integer          |
+----------+------------------+
| int8     | bigint           |
+----------+------------------+
| long     | bigint           |
+----------+------------------+
| string   | text             |
+----------+------------------+
| name     | text             |
+----------+------------------+
| byte     | char             |
+----------+------------------+
| float    | real             |
+----------+------------------+
| double   | double precision |
+----------+------------------+


.. _dateOptionalTime: http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateOptionalTimeParser()
.. _WKT: http://en.wikipedia.org/wiki/Well-known_text
.. _GeoJSON: http://geojson.org/
.. _GeoJSON geometry objects: https://tools.ietf.org/html/rfc7946#section-3.1
.. _Geohash: https://en.wikipedia.org/wiki/Geohash
.. _Quadtree: https://en.wikipedia.org/wiki/Quadtree
.. _Trie: https://en.wikipedia.org/wiki/Trie
.. _Tries: https://en.wikipedia.org/wiki/Trie
.. _IEEE 754: http://ieeexplore.ieee.org/document/30711/?arnumber=30711&filter=AND(p_Publication_Number:2355)
