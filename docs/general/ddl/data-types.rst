.. highlight:: psql

.. _data-types:

==========
Data types
==========

Data can be stored in different formats. CrateDB has different types that can
be specified if a table is created using the the :ref:`sql-create-table`
statement. Data types play a central role as they limit what kind of data can
be inserted, how it is stored and they also influence the behaviour when the
records are queried.

Data type names are reserved words and need to be escaped when used as column
names.

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


.. _data-types-primitive:

Primitive types
===============

Primitive types are types with :ref:`scalar <gloss-scalar>` values:

.. contents::
   :local:
   :depth: 2


.. _data-types-boolean-values:

Boolean values
--------------

.. _type-boolean:

``BOOLEAN``
'''''''''''

A basic boolean type accepting ``true`` and ``false`` as values.

Example::

    cr> CREATE TABLE my_bool_table (
    ...   first_column BOOLEAN
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE my_bool_table;
    DROP OK, 1 row affected (... sec)


.. _data-types-character-data:

Character data
--------------

Character types are general purpose strings of character data.

CrateDB supports the following character types:

.. contents::
   :local:
   :depth: 1

.. NOTE::

    Only character data types without specified length can be :ref:`analyzed
    for full text search <sql_ddl_index_fulltext>`.

    By default, the :ref:`plain <plain-analyzer>` analyzer is used.


.. _type-varchar:

``VARCHAR(n)``
''''''''''''''

The ``VARCHAR(n)`` (or ``CHARACTER VARYING(n)``) type represents variable
length strings. All unicode characters are allowed.

The optional length specification ``n`` is a positive :ref:`integer
<data-type-numeric>` that defines the maximum length, in characters, of the
values that have to be stored or cast. The minimum length is ``1``. The maximum
length is defined by the upper :ref:`integer <data-type-numeric>` range.

An attempt to store a string literal that exceeds the specified length
of the character data type results in an error.

::

    cr> CREATE TABLE users (id VARCHAR, name VARCHAR(6));
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO users (id, name) VALUES ('1361', 'john doe');
    SQLParseException['john doe' is too long for the text type of length: 6]

If the excess characters are all spaces, the string literal will be truncated
to the specified length.

::

    cr> INSERT INTO users (id, name) VALUES ('1', 'john     ');
    INSERT OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE users;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT id, name, char_length(name) AS name_length FROM users;
    +----+------+-------------+
    | id | name | name_length |
    +----+------+-------------+
    | 1  | john |           6 |
    +----+------+-------------+
    SELECT 1 row in set (... sec)

If a value is explicitly cast to ``VARCHAR(n)``, then an over-length value
will be truncated to ``n`` characters without raising an error.

::

    cr> SELECT 'john doe'::VARCHAR(4) AS name;
    +------+
    | name |
    +------+
    | john |
    +------+
    SELECT 1 row in set (... sec)

``CHARACTER VARYING`` and ``VARCHAR`` without the length specifier are
aliases for the :ref:`text <data-type-text>` data type,
see also :ref:`type aliases <data-type-aliases>`.

.. HIDE:

    cr> DROP TABLE users;
    DROP OK, 1 row affected (... sec)


.. _type-text:

``TEXT``
''''''''

A text-based basic type containing one or more characters. All unicode
characters are allowed.

::

    cr> CREATE TABLE users (name text);
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE users;
    DROP OK, 1 row affected (... sec)

.. NOTE::

   Maximum indexed string length is restricted to 32766 bytes, when encoded
   with UTF-8 unless the string is analyzed using full text or indexing and
   the usage of the :ref:`ddl-storage-columnstore` is disabled.

.. NOTE::
   There is no difference in storage costs among all character data types.


.. _data-type-bit:

``BIT(n)``
''''''''''

A string representation of a a bit sequence, useful for visualizing a `bit
mask`_.

Values of the type can be created using the bit string literal syntax. A bit
string starts with the ``B`` prefix, followed by a sequence of ``0`` or ``1``
digits quoted within single quotes ``'``.

An example:

::

  B'00010010'


::

  cr> CREATE TABLE metrics (bits BIT(4));
  CREATE OK, 1 row affected (... sec)


  cr> INSERT INTO metrics (bits) VALUES (B'0110');
  INSERT OK, 1 row affected  (... sec)


Inserting values that are either too short or too long results in an error:

::

  cr> INSERT INTO metrics (bits) VALUES (B'00101');
  SQLParseException[bit string length 5 does not match type bit(4)]


.. hide:

    cr> REFRESH TABLE metrics;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT bits FROM metrics;
    +---------+
    | bits    |
    +---------+
    | B'0110' |
    +---------+
    SELECT 1 row in set (... sec)


.. hide:

    cr> DROP TABLE metrics;
    DROP OK, 1 row affected (... sec)


.. _data-types-numeric:

Numeric data
------------

CrateDB supports the following numeric types:

.. contents::
   :local:
   :depth: 1


.. _type-smallint:

``SMALLINT``
''''''''''''

small-range integer

size: 2 bytes

range: -32,768 to 32,767


.. _type-integer:

``INTEGER``
'''''''''''

typical choice for integer

size: 4 bytes

range: -2^31 to 2^31-1


.. _type-bigint:

``BIGINT``
''''''''''

large-range integer

size: 8 bytes

range: -2^63 to 2^63-1


.. _type-numeric:

``NUMERIC``
'''''''''''

user-specified precision, exact

size: variable

range: up to 131072 digits before the decimal point; up to 16383 digits after
the decimal point



Arbitrary Precision Numbers:

.. NOTE::

    The storage of the ``NUMERIC`` data type is not supported. Therefore,
    it is not possible to create tables with ``NUMERIC`` fields.

The ``NUMERIC`` type literals store exact numeric data values and
perform exact calculations on them.

This type is usually used when it is important to preserve exact precision
or handle values that exceed the range of the numeric types of the fixed
length. The aggregations and arithmetic operations on numeric values are
much slower compared to operations on the integer or floating-point types.

The ``NUMERIC`` type can be configured with the precision and scale. The
``precision`` of a numeric is the total count of significant digits in the
unscaled numeric value.  The ``scale`` of a numeric is the count of decimal
digits in the fractional part, to the right of the decimal point. For example,
the number 123.45 has a precision of 5 and a scale of 2. Integers have a scale
of zero.

To declare the ``numeric`` type with the precision and scale use the syntax::

    NUMERIC(precision, scale)

Alternatively, only the precision can be specified, the scale will be zero
or positive integer in this case::

    NUMERIC(precision)

Without configuring the precision and scale the ``NUMERIC`` type value will be
represented by an unscaled value of the unlimited precision::

    NUMERIC

The ``NUMERIC`` type backed internally by the Java ``BigDecimal`` class. For
more detailed information about its behaviour, see `BigDecimal documentation`_.


.. _type-real:

``REAL``
''''''''

inexact, variable-precision

size: 4 bytes

range: 6 decimal digits precision

Floating-point types: The ``REAL`` and ``DOUBLE PRECISION`` data types are inexact,
variable-precision numeric types. It means that these types are stored as an
approximation.  Therefore, storage, calculation, and retrieval of the value
will not always result in an exact representation of the actual floating-point
value.

For instance, the result of applying ``SUM`` or ``AVG`` :ref:`aggregate
functions <aggregation-functions>` may slightly vary between query executions
or comparing floating-point values for equality might not always match.

.. SEEALSO::

    `Wikipedia: Single-precision floating-point format`_

    `Wikipedia: Double-precision floating-point format`_

Special floating-point values: CrateDB conforms to the `IEEE 754`_ standard concerning special values for
``REAL`` and ``DOUBLE PRECISION`` floating point data types. This means that
it also supports  ``NaN``, ``Infinity``, ``-Infinity`` (negative infinity),
and ``-0`` (signed zero).

::

    cr> SELECT 0.0 / 0.0 AS a, 1.0 / 0.0 as B, 1.0 / -0.0 AS c;
    +-----+----------+-----------+
    | a   | b        | c         |
    +-----+----------+-----------+
    | NaN | Infinity | -Infinity |
    +-----+----------+-----------+
    SELECT 1 row in set (... sec)

These special numeric values can also be inserted into a column of type
``REAL`` or ``DOUBLE PRECISION`` using a ``TEXT`` literal.

::

    cr> CREATE TABLE my_table3 (
    ...   first_column INTEGER,
    ...   second_column BIGINT,
    ...   third_column SMALLINT,
    ...   fourth_column DOUBLE PRECISION,
    ...   fifth_column REAL,
    ...   sixth_column CHAR
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table3 (fourth_column, fifth_column)
    ... VALUES ('NaN', 'Infinity');
    INSERT OK, 1 row affected (... sec)


.. _type-double-precision:

``DOUBLE PRECISION``
''''''''''''''''''''

inexact, variable-precision

size: 8 bytes

range: 15 decimal digits precision

Floating-point types: see "REAL"

Special floating-point values: see "REAL"


.. _data-types-dates-times:

Dates and times
---------------

CrateDB supports the following types for dates and times:

.. contents::
   :local:
   :depth: 1

The following table specifies the declared types of :ref:`arithmetic
expressions <arithmetic>` that involve temporal :ref:`operands
<gloss-operand>`:


+---------------+----------------+---------------+
|       Operand | Operator       |       Operand |
+===============+================+===============+
| ``TIMESTAMP`` |          ``-`` | ``TIMESTAMP`` |
+---------------+----------------+---------------+
|  ``INTERVAL`` |          ``+`` | ``TIMESTAMP`` |
+---------------+----------------+---------------+
| ``TIMESTAMP`` | ``+`` or ``-`` |  ``INTERVAL`` |
+---------------+----------------+---------------+
|  ``INTERVAL`` | ``+`` or ``-`` |  ``INTERVAL`` |
+---------------+----------------+---------------+


.. _data-types-timestamp:

``TIMESTAMP``
'''''''''''''

+---------------------------------+----------+-------------------------+------------------------+
| Name                            | Size     | Description             | Range                  |
+=================================+==========+=========================+========================+
| ``TIMESTAMP WITH TIME ZONE``    | 8 bytes  | time and date with time | ``292275054BC``        |
|                                 |          | zone                    | to ``292278993AD``     |
+---------------------------------+----------+-------------------------+------------------------+
| ``TIMESTAMP WITHOUT TIME ZONE`` | 8 bytes  | time and date without   | ``292275054BC``        |
|                                 |          | time zone               | to ``292278993AD``     |
+---------------------------------+----------+-------------------------+------------------------+

The timestamp types consist of the concatenation of a date and time, followed
by an optional time zone.

Internally, timestamp values are mapped to the UTC milliseconds since
``1970-01-01T00:00:00Z`` stored as ``BIGINT``.

Timestamps are always returned as ``BIGINT`` values.

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

    Due to internal date parsing, not the full ``BIGINT`` range is supported for
    timestamp values, but only dates between year ``292275054BC`` and
    ``292278993AD``, which is slightly smaller.


.. _type-datetime-with-tz:

``TIMESTAMP WITH TIME ZONE``
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


Timestamps will also accept a ``BIGINT`` representing UTC milliseconds since
the epoch or a ``REAL`` or ``DOUBLE PRECISION`` representing UTC seconds since
the epoch with milliseconds as fractions.

::

    cr> SELECT 1.0::TIMESTAMP WITH TIME ZONE AS ts;
    +------+
    |   ts |
    +------+
    | 1000 |
    +------+
    SELECT 1 row in set (... sec)


.. _type-datetime-without-tz:

``TIMESTAMP WITHOUT TIME ZONE``
...............................

A string literal that contain a timestamp value with the time zone will be
converted to UTC without considering the time zone indication.

::

    cr> SELECT '1970-01-02T00:00:00+0200'::TIMESTAMP WITHOUT TIME ZONE AS ts,
    ...        '1970-01-02T00:00:00+0400'::TIMESTAMP WITHOUT TIME ZONE AS ts,
    ...        '1970-01-02T00:00:00Z'::TIMESTAMP WITHOUT TIME ZONE AS ts,
    ...        '1970-01-02 00:00:00Z'::TIMESTAMP WITHOUT TIME ZONE AS ts_sql_format;
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

.. _type-timestamp-at-tz:

``TIMESTAMP WITH/WITHOUT TIME ZONE AT TIME ZONE zone``
......................................................

AT TIME ZONE converts a timestamp without time zone to/from a timestamp with
time zone. It has the following variants:

.. csv-table::
   :header: "Expression", "Return Type", "Description"

   "TIMESTAMP WITHOUT TIME ZONE AT TIME ZONE zone", "TIMESTAMP WITH TIME ZONE", "Treat \
   given time stamp without time zone as located in the specified time zone"
   "TIMESTAMP WITH TIME ZONE AT TIME ZONE zone", "TIMESTAMP WITHOUT TIME ZONE", "Convert \
   given time stamp with time zone to the new time zone, with no time zone designation"

In these :ref:`expressions <gloss-expression>`, the desired time zone is
specified as a string (e.g., 'Europe/Madrid', '+02:00').

The scalar function :ref:`TIMEZONE <scalar-timezone>` (zone, timestamp) is
equivalent to the SQL-conforming timestamp construct ``AT TIME ZONE zone``.


.. _type-time:

``TIME``
''''''''

+---------------------------------+----------+-------------------------+------------------------+
| Name                            | Size     | Description             | Range                  |
+=================================+==========+=========================+========================+
| ``TIME WITH TIME zone``         | 12 bytes | time with time zone     | ``00:00:00.000000``    |
| ``TIMETZ``                      |          |                         | to ``23:59:59.999999`` |
|                                 |          |                         | zone: -18:00 to 18:00  |
+---------------------------------+----------+-------------------------+------------------------+


.. _type-time-with-tz:

``TIME WITH TIME ZONE``
.......................

The time type consists of time followed by an optional time zone.

``TIMETZ`` is an alias for `time with time zone`.

`TIME WITH TIME ZONE` literals can be constructed using a string literal
and a cast. The syntax for string literal is as follows:

.. code-block:: text

    time-element [offset]

    time-element: time-only [fraction]
    time-only:    HH[[:][mm[:]ss]]
    fraction:     '.' digit+
    offset:       {+ | -} time-only | geo-region
    geo-region:   As defined by ISO 8601.


Where ``time-only`` can contain optional seconds, or optional minutes and
seconds, and can use ``:`` as a separator optionally.

`fraction` accepts up to 6 digits, as precision is in micro seconds.

Time zone syntax as defined by `ISO 8601 time zone designators`_.

.. NOTE::

    This type cannot be used in `CREATE TABLE` or `ALTER` statements.

::

    cr> SELECT '13:59:59.999999'::timetz;
    +------------------+
    | 13:59:59.999999  |
    +------------------+
    | [50399999999, 0] |
    +------------------+
    SELECT 1 row in set (... sec)

    cr> SELECT '13:59:59.999999+02:00'::timetz;
    +-----------------------+
    | 13:59:59.999999+02:00 |
    +-----------------------+
    | [50399999999, 7200]   |
    +-----------------------+
    SELECT 1 row in set (... sec)


.. _type-date:

``DATE``
''''''''

+---------------------------------+----------+-------------------------+------------------------+
| Name                            | Size     | Description             | Range                  |
+=================================+==========+=========================+========================+
| ``DATE``                        | 8 bytes  | date in utc             | ``292275054BC``        |
|                                 |          |                         | to ``292278993AD``     |
+---------------------------------+----------+-------------------------+------------------------+

.. NOTE::

    The storage of the ``DATE`` data type is not supported. Therefore,
    it is not possible to create tables with ``DATE`` fields.

The ``DATE`` data type can be used to represent values with a year, month and a day.

To construct values of the type ``DATE`` you can cast a string literal
or a numeric literal to ``DATE``. If a numeric value is used, it must contain
the number of milliseconds since ``1970-01-01T00:00:00Z``.

The string format for dates is `YYYY-MM-DD`. For example `2021-03-09`.
This format is the only currently supported for PostgreSQL clients.

::

    cr> SELECT '2021-03-09'::date AS cd;
    +---------------+
    |            cd |
    +---------------+
    | 1615248000000 |
    +---------------+
    SELECT 1 row in set (... sec)


.. _type-interval:

``INTERVAL``
''''''''''''

NOTE: type literal only. not valid for column definitions

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


.. _type-interval-year-month:

year-month
..........

A ``year-month`` literal includes either ``YEAR``, ``MONTH`` or a contiguous
subset of these fields.

::

    <year_month_literal> ::=
        INTERVAL [ {+ | -} ]'yy' <interval_qualifier> |
        INTERVAL [ {+ | -} ]'[ yy- ] mm' <interval_qualifier>

For example::

    cr> SELECT INTERVAL '01-02' YEAR TO MONTH AS result;
    +------------------------+
    | result                 |
    +------------------------+
    | 1 year 2 mons 00:00:00 |
    +------------------------+
    SELECT 1 row in set (... sec)


.. _type-interval-day-time:

day-time
........

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

    cr> SELECT INTERVAL '10 23:10' DAY TO MINUTE AS result;
    +-------------------------+
    | result                  |
    +-------------------------+
    | 1 weeks 3 days 23:10:00 |
    +-------------------------+
    SELECT 1 row in set (... sec)


.. _type-interval-string:

string-literal
..............

An interval ``string-literal`` can be defined by a combination of
:ref:`day-time-literal <day-time-literal>` and
:ref:`year-month-literal <year-month-literal>`
or using the :ref:`iso-8601-format <iso-8601-format>` or
:ref:`PostgreSQL-format <postgresql-format>`.

For example::

    cr> SELECT INTERVAL '1-2 3 4:5:6' AS result;
    +-------------------------------+
    | result                        |
    +-------------------------------+
    | 1 year 2 mons 3 days 04:05:06 |
    +-------------------------------+
    SELECT 1 row in set (... sec)

.. _iso-8601-format:

**ISO-8601 format**
  The iso-8601 format describes a duration of time using the
  `ISO 8601 duration format`_ syntax.

  For example::

      cr> SELECT INTERVAL 'P1Y2M3DT4H5M6S' AS result;
      +-------------------------------+
      | result                        |
      +-------------------------------+
      | 1 year 2 mons 3 days 04:05:06 |
      +-------------------------------+
      SELECT 1 row in set (... sec)

.. _postgresql-format:

**PostgreSQL format**
  The ``PostgreSQL`` format describes a duration of time using the `PostgreSQL
  interval format`_ syntax.

  For example::

      cr> SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds' AS result;
      +-------------------------------+
      | result                        |
      +-------------------------------+
      | 1 year 2 mons 3 days 04:05:06 |
      +-------------------------------+
      SELECT 1 row in set (... sec)


.. _data-types-ip-addresses:

IP addresses
------------


.. _type-ip:

``IP``
''''''

The ``IP`` type allows to store IPv4 and IPv6 addresses by inserting their
string representation. Internally IP addresses are stored as ``BIGINT``
allowing expected sorting, filtering and aggregation.

Example::

    cr> CREATE TABLE my_table_ips (
    ...   fqdn TEXT,
    ...   ip_addr IP
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table_ips (fqdn, ip_addr)
    ... VALUES ('localhost', '127.0.0.1'),
    ...        ('router.local', '0:0:0:0:0:ffff:c0a8:64');
    INSERT OK, 2 rows affected (... sec)

::

    cr> INSERT INTO my_table_ips (fqdn, ip_addr)
    ... VALUES ('localhost', 'not.a.real.ip');
    SQLParseException[Cannot cast `'not.a.real.ip'` of type `text` to type `ip`]

IP addresses support the :ref:`operator <gloss-operator>` ``<<``, which checks
for subnet inclusion using `CIDR notation`_. The left-hand :ref:`operand
<gloss-operand>` must be of type :ref:`ip <ip-type>` and the right-hand must be
of type :ref:`text <data-type-text>` (e.g., ``'192.168.1.5' <<
'192.168.1/24'``).


.. _data-types-container:

Container types
===============

Container types are types with :ref:`nonscalar <gloss-nonscalar>` values that
can contain other values:

.. contents::
   :local:
   :depth: 3


.. _data-types-objects:

Objects
-------


.. _type-object:

``OBJECT``
''''''''''

An object is a :ref:`container data type <data-types-container>` and is
structured as a collection of key-values.

An object can contain any other type, including further child objects. An
``OBJECT`` column can be schemaless or can have a defined (i.e., enforced)
schema.

Objects are not the same as JSON objects, although they share a lot of
similarities. However, objects can be :ref:`inserted as JSON strings
<data-type-object-json>`.

Syntax::

    <columnName> OBJECT [ ({DYNAMIC|STRICT|IGNORED}) ] [ AS ( <columnDefinition>* ) ]

The only required part of this column definition is ``OBJECT``.

The column policy defining this objects behaviour is optional, if left out
``DYNAMIC`` will be used.

The list of subcolumns is optional as well, if left out, this object will have
no schema (with a schema created on the fly on first inserts in case of
``DYNAMIC``).

Example::

    cr> CREATE TABLE my_table11 (
    ...   title TEXT,
    ...   col1 OBJECT,
    ...   col3 OBJECT(STRICT) as (
    ...     age INTEGER,
    ...     name TEXT,
    ...     col31 OBJECT AS (
    ...       birthday TIMESTAMP WITH TIME ZONE
    ...     )
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> DROP TABLE my_table11;
    DROP OK, 1 row affected (... sec)

``STRICT``
..........

The column policy can be configured to be ``STRICT``, rejecting any subcolumn
that is not defined upfront in the schema. As you might have guessed, defining
``STRICT`` objects without subcolumns results in an unusable column that will
always be null, which is the most useless column one could create.

Example::

    cr> CREATE TABLE my_table12 (
    ...   title TEXT,
    ...   author OBJECT(STRICT) AS (
    ...     name TEXT,
    ...     birthday TIMESTAMP WITH TIME ZONE
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> DROP TABLE my_table12;
    DROP OK, 1 row affected (... sec)

``DYNAMIC``
...........

Another option is ``DYNAMIC``, which means that new subcolumns can be added in
this object.

Note that adding new columns to an object with a ``DYNAMIC`` policy will affect
the schema of the table. Once a column is added, it shows up in the
``information_schema.columns`` table and its type and attributes are fixed.
They will have the type that was guessed by their inserted/updated value and
they will always be analyzed as-is with the :ref:`plain <plain-analyzer>`,
which means the column will be indexed but not tokenized in the case of
``TEXT`` columns.

If a new column ``a`` was added with type ``INTEGER``, adding strings to this
column will result in an error.

Examples::

    cr> CREATE TABLE my_table13 (
    ...   title TEXT,
    ...   author OBJECT AS (
    ...     name TEXT,
    ...     birthday TIMESTAMP WITH TIME ZONE
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE my_table13;
    DROP OK, 1 row affected (... sec)

which is exactly the same as::

    cr> CREATE TABLE my_table14 (
    ...   title TEXT,
    ...   author OBJECT(DYNAMIC) AS (
    ...     name TEXT,
    ...     birthday TIMESTAMP WITH TIME ZONE
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE my_table14;
    DROP OK, 1 row affected (... sec)

New columns added to ``DYNAMIC`` objects are, once added, usable as usual
subcolumns. One can retrieve them, sort by them and use them in where clauses.


``IGNORED``
...........

The third option is ``IGNORED``. Explicitly defined columns within an
``IGNORED`` object behave the same as those within object columns declared as
``DYNAMIC`` or ``STRICT`` (e.g., column constraints are still enforced, columns
that would be indexed are still indexed, and so on). The difference is that
with ``IGNORED``, dynamically added columns do not result in a schema update
and the values won't be indexed. This allows you to store values with a mixed
type under the same key.

An example::

    cr> CREATE TABLE metrics (
    ...   id TEXT PRIMARY KEY,
    ...   payload OBJECT(IGNORED) as (
    ...     tag TEXT
    ...   )
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO metrics (id, payload) VALUES ('1', {"tag"='AT', "value"=30});
    INSERT OK, 1 row affected (... sec)

::

    cr> INSERT INTO metrics (id, payload) VALUES ('2', {"tag"='AT', "value"='str'});
    INSERT OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE metrics;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT payload FROM metrics ORDER BY id;
    +-------------------------------+
    | payload                       |
    +-------------------------------+
    | {"tag": "AT", "value": 30}    |
    | {"tag": "AT", "value": "str"} |
    +-------------------------------+
    SELECT 2 rows in set (... sec)

.. NOTE::

    Given that dynamically added sub-columns of an ``IGNORED`` objects are not
    indexed, filter operations on these columns cannot utilize the index and
    instead a value lookup is performed for each matching row. This can be
    mitigated by combining a filter using the ``AND`` clause with other
    predicates on indexed columns.

    Futhermore, values for dynamically added sub-columns of an ``IGNORED``
    objects aren't stored in a column store, which means that ordering on these
    columns or using them with aggregates is also slower than using the same
    operations on regular columns. For some operations it may also be necessary
    to add an explicit type cast because there is no type information available
    in the schema.

    An example:
    ::

     cr> SELECT id, payload FROM metrics ORDER BY payload['value']::TEXT DESC;
     +----+-------------------------------+
     | id | payload                       |
     +----+-------------------------------+
     | 2  | {"tag": "AT", "value": "str"} |
     | 1  | {"tag": "AT", "value": 30}    |
     +----+-------------------------------+
     SELECT 2 rows in set (... sec)

    Given that it is possible have values of different types within the same
    sub-column of an ignored objects, aggregations may fail at runtime:

    ::

     cr> SELECT SUM(payload['value']::BIGINT) FROM metrics;
     SQLParseException[Cannot cast value `str` to type `bigint`]

.. HIDE:

    cr> DROP TABLE metrics;
    DROP OK, 1 row affected (... sec)


.. _data-types-inserting-objects:

Inserting objects
'''''''''''''''''


.. _data-types-object-literals:

Object literals
...............

You can insert objects using object literals. Object literals are delimited
using curly brackets and key-value pairs are connected via ``=``.

Synopsis::

    { [ ident = expr [ , ... ] ] }

Here, ``ident`` is the key and ``expr`` is the value. The key must be a
lowercase column identifier or a quoted mixed-case column identifier. The value
must be a value literal (object literals are permitted and can be nested in
this way).

Empty object literal::

    {}

Boolean type::

    { my_bool_column = true }

Text type::

    { my_str_col = 'this is a text value' }

Number types::

    { my_int_col = 1234, my_float_col = 5.6 }

Array type::

    { my_array_column = ['v', 'a', 'l', 'u', 'e'] }

Camel case keys must be quoted::

    { "CamelCaseColumn" = 'this is a text value' }

Nested object::

    { nested_obj_colmn = { int_col = 1234, str_col = 'text value' } }

You can even specify a :ref:`placeholder parameter <sql-parameter-reference>`
for a value::

    { my_other_column = ? }

Combined::

    { id = 1, name = 'foo', tags = ['apple', 'banana', 'pear'], size = 3.1415, valid = ? }

.. NOTE::

   Even though they look like JSON, object literals are not JSON. If you want
   to use JSON, skip to the next subsection.

.. SEEALSO::

    :ref:`Selecting values from inner objects and nested objects <sql_dql_objects>`


.. _data-types-object-json:

JSON
....

You can insert objects using JSON strings. To do this, you must :ref:`type cast
<type_cast>` the string to an object with an implicit cast (i.e., passing a
string into an object column) or an explicit cast (i.e., using the ``::OBJECT``
syntax).

.. TIP::

    Explicit casts can improve query readability.

Below you will find examples from the previous subsection rewritten to use JSON
strings with explicit casts.

Empty object literal::

    '{}'::object

Boolean type::

    '{ "my_bool_column": true }'::object

Text type::

    '{ "my_str_col": "this is a text value" }'::object

Number types::

    '{ "my_int_col": 1234, "my_float_col": 5.6 }'::object

Array type::

    '{ "my_array_column": ["v", "a", "l", "u", "e"] }'::object

Camel case keys::

    '{ "CamelCaseColumn": "this is a text value" }'::object

Nested object::

    '{ "nested_obj_colmn": { "int_col": 1234, "str_col": "text value" } }'::object

.. NOTE::

    You cannot use :ref:`placeholder parameters <sql-parameter-reference>`
    inside a JSON string.


.. _data-types-arrays:

Arrays
------

.. _type-array:

``ARRAY``
'''''''''

An array is a :ref:`container data type <data-types-container>` and is
structured as a collection of other data types. Arrays can contain the
following:

* :ref:`Primitive types <data-types-primitive>`
* :ref:`Objects <type-object>`
* :ref:`Geographic types <data-types-geo>`

Array types are defined as follows::

    cr> CREATE TABLE my_table_arrays (
    ...     tags ARRAY(TEXT),
    ...     objects ARRAY(OBJECT AS (age INTEGER, name TEXT))
    ... );
    CREATE OK, 1 row affected (... sec)


An alternative is the following syntax to refer to arrays::

    <typeName>[]

This means ``TEXT[]`` is equivalent to ``ARRAY(text)``.


.. NOTE::

    Currently arrays cannot be nested. Something like ``ARRAY(ARRAY(TEXT))``
    won't work.


.. _data-types-array-literals:

Array constructor
'''''''''''''''''

Arrays can be written using the array constructor ``ARRAY[]`` or short ``[]``.
The array constructor is an :ref:`expression <gloss-expression>` that accepts
both literals and expressions as its parameters. Parameters may contain zero or
more elements.

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


An alternative way to define arrays is to use string literals and casts to
arrays. This requires a string literal that contains the elements separated by
comma and enclosed with curly braces::

    '{ val1, val2, val3 }'

::

    cr> SELECT '{ab, CD, "CD", null, "null"}'::ARRAY(TEXT) AS arr;
    +----------------------------------+
    | arr                              |
    +----------------------------------+
    | ["ab", "CD", "CD", null, "null"] |
    +----------------------------------+
    SELECT 1 row in set (... sec)


``null`` elements are interpreted as ``null`` (none, absent), if you want the
literal ``null`` string, it has to be enclosed in double quotes.


This variant primarily exists for compatibility with PostgreSQL. The array
constructor syntax explained further above is the preferred way to define
constant array values.


Array representation
''''''''''''''''''''

Arrays are always represented as zero or more literal elements inside square
brackets (``[]``), for example::

    [1, 2, 3]
    ['Zaphod', 'Ford', 'Arthur']


.. _data-types-geo:

Geographic types
================

:ref:`Geographic types <data-types-geo>` are types with :ref:`nonscalar
<gloss-nonscalar>` values representing points or shapes in a 2D world:

.. contents::
   :local:
   :depth: 3


.. _data-types-geo-point:

Geometric points
----------------


.. _type-geo-point:

``GEO_POINT``
'''''''''''''

A ``GEO_POINT`` is a :ref:`geographic data type <data-types-geo>` used to store
latitude and longitude coordinates.

Columns with the ``GEO_POINT`` type are represented and inserted using an array
of doubles in the following format::

    [<lon_value>, <lat_value>]

Alternatively a `WKT`_ string can also be used to declare geo points::

    'POINT ( <lon_value> <lat_value> )'

.. NOTE::

    Empty geo points are not supported.

    Additionally, if a column is dynamically created the type detection won't
    recognize neither WKT strings nor double arrays. That means columns of type
    ``GEO_POINT`` must always be declared beforehand.

Create table example::

    cr> CREATE TABLE my_table_geopoint (
    ...   id INTEGER PRIMARY KEY,
    ...   pin GEO_POINT
    ... ) WITH (number_of_replicas = 0)
    CREATE OK, 1 row affected (... sec)


.. _data-type-geo-shape:

Geometric shapes
----------------


.. _type-geo-shape:

``GEO_SHAPE``
'''''''''''''

A ``geo_shape`` is a :ref:`geographic data type <data-types-geo>` used to store
2D shapes defined as `GeoJSON geometry objects`_.

A ``GEO_SHAPE`` column can store different kinds of `GeoJSON geometry
objects`_.  Thus it is possible to store e.g. ``LineString`` and
``MultiPolygon`` shapes in the same column.

.. NOTE::

    3D coordinates are not supported.

    Empty ``Polygon`` and ``LineString`` geo shapes are not supported.


Definition
''''''''''

To define a ``GEO_SHAPE`` column::

    <columnName> geo_shape

A geographical index with default parameters is created implicitly to allow for
geographical queries.

The default definition for the column type is::

    <columnName> GEO_SHAPE INDEX USING geohash WITH (precision='50m', distance_error_pct=0.025)

There are two geographic index types: ``geohash`` (the default) and
``quadtree``. These indices are only allowed on ``geo_shape`` columns. For more
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


.. _data-types-geo-shape-index:

Geo shape index structure
'''''''''''''''''''''''''

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
''''''''''''''

Columns with the ``GEO_SHAPE`` type are represented and inserted as object
containing a valid `GeoJSON`_ geometry object::

    {
      type = 'Polygon',
      coordinates = [
         [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
      ]
    }

Alternatively a `WKT`_ string can be used to represent a ``GEO_SHAPE`` as
well::

    'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'

.. NOTE::

    It is not possible to detect a ``GEO_SHAPE`` type for a dynamically created
    column. Like with :ref:`type-geo-point` type, ``GEO_SHAPE`` columns need to
    be created explicitly using either :ref:`sql-create-table` or
    :ref:`sql-alter-table`.


.. _data-types-postgres:

PostgreSQL compatibility
========================

.. contents::
   :local:
   :depth: 1


.. _data-types-postgres-aliases:

Type aliases
------------

For compatibility with PostgreSQL we include some type aliases which can be
used instead of the CrateDB specific type names.

For example, in a type cast::

  cr> SELECT 10::INT2 AS INT2;
  +------+
  | INT2 |
  +------+
  |   10 |
  +------+
  SELECT 1 row in set (... sec)


See the table below for a full list of aliases:

+-------------+--------------------------+
| Alias       | CrateDB Type             |
+=============+==========================+
| SHORT       | SMALLINT                 |
+-------------+--------------------------+
| INT         | INTEGER                  |
+-------------+--------------------------+
| INT2        | SMALLINT                 |
+-------------+--------------------------+
| INT4        | INTEGER                  |
+-------------+--------------------------+
| INT8        | BIGINT                   |
+-------------+--------------------------+
| LONG        | BIGINT                   |
+-------------+--------------------------+
| STRING      | TEXT                     |
+-------------+--------------------------+
| VARCHAR     | TEXT                     |
+-------------+--------------------------+
| CHARACTER   | TEXT                     |
| VARYING     |                          |
+-------------+--------------------------+
| NAME        | TEXT                     |
+-------------+--------------------------+
| REGPROC     | TEXT                     |
+-------------+--------------------------+
| BYTE        | CHAR                     |
+-------------+--------------------------+
| FLOAT       | REAL                     |
+-------------+--------------------------+
| DOUBLE      | DOUBLE PRECISION         |
+-------------+--------------------------+
| TIMESTAMP   | TIMESTAMP WITH TIME ZONE |
+-------------+--------------------------+
| TIMESTAMPTZ | TIMESTAMP WITH TIME ZONE |
+-------------+--------------------------+

.. NOTE::

   The :ref:`PG_TYPEOF <pg_typeof>` system :ref:`function <gloss-function>` can
   be used to resolve the data type of any :ref:`expression
   <gloss-expression>`.

.. _data-types-postgres-internal:

Internal-use types
------------------

.. _type-char:

``CHAR``
  A one-byte character used internally as for enumeration in the
  :ref:`PostgreSQL system catalogs <postgres_pg_catalog>`.

  Specified as a signed integer in the range -128 to 127.

.. _type-oid:

``OID``
  An *Object Identifier* (OID). OIDS are used internally as primary keys in the
  :ref:`PostgreSQL system catalogs <postgres_pg_catalog>`.

  The ``OID`` type is mapped to the :ref:`integer
  <data-type-numeric>` data type.

.. _type-regproc:

``REGPROC``
  An alias for the :ref:`oid <type-oid>` type.

  The ``REGPROC`` type is used by tables in the :ref:`postgres_pg_catalog`
  schema to reference functions in the `pg_proc`_ table.

  :ref:`Casting <type_cast>` a ``REGPROC`` type to a :ref:`data-type-text` or
  :ref:`integer <data-type-numeric>` type will result in the corresponding
  function name or ``oid`` value, respectively.

.. _type-regclass:

``REGCLASS``
  An alias for the :ref:`oid <type-oid>` type.

  The ``REGCLASS`` type is used by tables in the :ref:`postgres_pg_catalog`
  schema to reference relations in the `pg_class`_ table.

  :ref:`Casting <type_cast>` a ``REGCLASS`` type to a :ref:`data-type-text` or
  :ref:`integer <data-type-numeric>` type will result in the corresponding
  relation name or ``oid`` value, respectively.

.. _type-oidvector:

``OIDVECTOR``
  The ``OIDVECTOR`` type is used to represent one or more :ref:`oid <type-oid>`
  values.

  This type is similar to an :ref:`array <data-type-array>` of integers.
  However, you cannot use it with any :ref:`scalar functions
  <scalar-functions>` or :ref:`expressions <gloss-expression>`.

.. SEEALSO::

    :ref:`PostgreSQL: Object Identifier (OID) types <postgres_pg_oid>`


.. _data-types-casting:

Type casting
============

A type ``CAST`` specifies a conversion from one data type to another. It will
only succeed if the value of the :ref:`expression <gloss-expression>` is
convertible to the desired data type, otherwise an error is returned.

CrateDB supports two equivalent syntaxes for type casts:

::

   CAST(expression AS TYPE)
   expression::TYPE

.. contents::
   :local:
   :depth: 1


.. _data-types-casting-exp:

Cast expressions
----------------


::

   CAST(expression AS TYPE)
   expression::TYPE


.. _data-types-casting-fn:

Cast functions
--------------


.. _fn-cast:

``CAST``
''''''''

Example usages:

::

    cr> SELECT CAST(port['http'] AS BOOLEAN) FROM sys.nodes LIMIT 1;
    +-------------------------------+
    | CAST(port['http'] AS BOOLEAN) |
    +-------------------------------+
    | TRUE                          |
    +-------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> SELECT (2+10)/2::TEXT AS col;
    +-----+
    | col |
    +-----+
    |   6 |
    +-----+
    SELECT 1 row in set (... sec)

It is also possible to convert array structures to different data types, e.g.
converting an array of integer values to a boolean array.

::

    cr> SELECT CAST([0,1,5] AS ARRAY(BOOLEAN)) AS active_threads ;
    +---------------------+
    | active_threads      |
    +---------------------+
    | [false, true, true] |
    +---------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

   It is not possible to cast to or from ``OBJECT``,``GEO_POINT``, and
   ``GEO_SHAPE`` types.


.. _fn-try-cast:

``TRY_CAST``
''''''''''''

While ``CAST`` throws an error for incompatible type casts, ``TRY_CAST``
returns ``null`` in this case. Otherwise the result is the same as with
``CAST``.

::

   TRY_CAST(expression AS TYPE)

Example usages:

::

    cr> SELECT TRY_CAST('true' AS BOOLEAN) AS col;
    +------+
    | col  |
    +------+
    | true |
    +------+
    SELECT 1 row in set (... sec)

Trying to cast a ``TEXT`` to ``INTEGER``, will fail with ``CAST`` if
``TEXT`` is no valid integer but return ``null`` with ``TRY_CAST``:

::

    cr> SELECT TRY_CAST(name AS INTEGER) AS name_as_int FROM sys.nodes LIMIT 1;
    +-------------+
    | name_as_int |
    +-------------+
    |        null |
    +-------------+
    SELECT 1 row in set (... sec)


.. _data-types-casting-str:

Cast from string literals
-------------------------

This cast operation is applied to a string literal and it effectively
initializes a constant of an arbitrary type.

Example usages, initializing an ``INTEGER`` and a ``TIMESTAMP`` constant:

::

    cr> SELECT INTEGER '25' AS int;
    +-----+
    | int |
    +-----+
    |  25 |
    +-----+
    SELECT 1 row in set (... sec)

::

    cr> SELECT TIMESTAMP WITH TIME ZONE '2029-12-12T11:44:00.24446' AS ts;
    +---------------+
    | ts            |
    +---------------+
    | 1891770240244 |
    +---------------+
    SELECT 1 row in set (... sec)

.. NOTE::

  This cast operation is limited to :ref:`primitive data types
  <data-types-primitive>` only.  For complex types such as ``ARRAY`` or
  ``OBJECT`` use the :ref:`type_cast` syntax.


.. _BigDecimal documentation: https://docs.oracle.com/en/java/javase/15/docs/api/java.base/java/math/BigDecimal.html
.. _bit mask: https://en.wikipedia.org/wiki/Mask_(computing)
.. _CIDR notation: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation
.. _Geohash: https://en.wikipedia.org/wiki/Geohash
.. _GeoJSON geometry objects: https://tools.ietf.org/html/rfc7946#section-3.1
.. _GeoJSON: https://geojson.org/
.. _IEEE 754: https://ieeexplore.ieee.org/document/30711/?arnumber=30711&filter=AND(p_Publication_Number:2355)
.. _ISO 8601 duration format: https://en.wikipedia.org/wiki/ISO_8601#Durations
.. _ISO 8601 time zone designators: https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators
.. _pattern letters and symbols: https://docs.oracle.com/en/java/javase/15/docs/api/java.base/java/time/format/DateTimeFormatter.html
.. _pg_class: https://www.postgresql.org/docs/10/static/catalog-pg-class.html
.. _pg_proc: https://www.postgresql.org/docs/10/static/catalog-pg-proc.html
.. _PostgreSQL interval format: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
.. _Quadtree: https://en.wikipedia.org/wiki/Quadtree
.. _Trie: https://en.wikipedia.org/wiki/Trie
.. _Tries: https://en.wikipedia.org/wiki/Trie
.. _Wikipedia\: Double-precision floating-point format: https://en.wikipedia.org/wiki/Double-precision_floating-point_format
.. _Wikipedia\: Single-precision floating-point format: https://en.wikipedia.org/wiki/Single-precision_floating-point_format
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
