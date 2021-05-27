.. highlight:: psql

.. _data-types:

==========
Data types
==========

Data can be stored in different formats. CrateDB has different types that can
be specified if a table is created using the the :ref:`sql-create-table`
statement.

Data types play a central role as they limit what kind of data can be inserted,
how it is stored and they also influence the behaviour when the records are
queried.

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


.. _data-types-nulls:

Null values
-----------


.. _type-null:

``NULL``
''''''''

A ``NULL`` represents a missing value.

.. NOTE::

    ``NULL`` values are not the same as ``0``, an empty string (``''``), an
    empty object (``{}``), an empty array (``[]``), or any other kind of empty
    or zeroed data type.


You can use ``NULL`` values when inserting records to indicate the absence of a
data point (i.e., the value for a specific column is not known).

Similarly, CrateDB will produce ``NULL`` values when, for example, data is
missing from a :ref:`outer left-join <join-types-outer>` operation (i.e., when
a row from one relation has no corresponding row in the joined relation).

If you insert a record without specifying the value for a particular column,
CrateDB will insert a ``NULL`` value for that column.

For example::

    cr> CREATE TABLE users (
    ...     first_name TEXT,
    ...     surname TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

Insert a record without specifying ``surname``::

    cr> INSERT INTO users (
    ...     first_name
    ... ) VALUES (
    ...     'Alice'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

The resulting row will have a ``NULL`` value for ``surname``::

    cr> SELECT
    ...     first_name,
    ...     surname
    ... FROM users
    ... WHERE first_name = 'Alice';
    +------------+---------+
    | first_name | surname |
    +------------+---------+
    | Alice      | NULL    |
    +------------+---------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE users;
    DROP OK, 1 row affected (... sec)

You can prevent ``NULL`` values being inserted altogether with a :ref:`NOT NULL
constraint <not_null_constraint>`, like so::

    cr> CREATE TABLE users_with_surnames (
    ...     first_name TEXT,
    ...     surname TEXT NOT NULL
    ... );
    CREATE OK, 1 row affected (... sec)

Now, when you try to insert a user without a surname, it will produce an
error::

    cr> INSERT INTO users_with_surnames (
    ...     first_name
    ... ) VALUES (
    ...     'Alice'
    ... );
    SQLParseException["surname" must not be null]

.. HIDE:

    cr> DROP TABLE users_with_surnames;
    DROP OK, 1 row affected (... sec)


.. _data-types-boolean-values:

Boolean values
--------------

.. _type-boolean:

``BOOLEAN``
'''''''''''

A basic boolean type accepting ``true`` and ``false`` as values.

Example::

    cr> CREATE TABLE my_table (
    ...     first_column BOOLEAN
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO my_table (
    ...     TODO
    ... ) VALUES (
    ...     'TODO'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM my_table;
    +------------+
    | TODO       |
    +------------+
    | Alice      |
    +------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
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

    cr> CREATE TABLE users (
    ...     id VARCHAR,
    ...     name VARCHAR(6)
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO users (
    ...     id,
    ...     name
    ... ) VALUES (
    ...     '1',
    ...     'Alice Smith'
    ... );
    SQLParseException['Alice Smith' is too long for the text type of length: 6]

If the excess characters are all spaces, the string literal will be truncated
to the specified length.

::

    cr> INSERT INTO users (
    ...     id,
    ...     name
    ... ) VALUES (
    ...     '2',
    ...     'Bob     '
    ... );
    INSERT OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE users;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT
    ...    id,
    ...    name,
    ...    char_length(name) AS name_length
    ... FROM users;
    +----+------+-------------+
    | id | name | name_length |
    +----+------+-------------+
    | 1  | Bob  |           3 |
    +----+------+-------------+
    SELECT 1 row in set (... sec)

If a value is explicitly cast to ``VARCHAR(n)``, then an over-length value
will be truncated to ``n`` characters without raising an error.

::

    cr> SELECT 'Alice Smith'::VARCHAR(5) AS name;
    +-------+
    | name  |
    +-------+
    | Alice |
    +-------+
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

    cr> CREATE TABLE users (
    ...     name TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO users (
    ...     name
    ... ) VALUES (
    ...     'Alice'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE users;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM users;
    +-------+
    | name  |
    +-------+
    | Alice |
    +-------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE users;
    DROP OK, 1 row affected (... sec)

.. NOTE::

   The maximum indexed string length is restricted to 32766 bytes when encoded
   with UTF-8 unless the string is analyzed using full text or indexing and the
   usage of the :ref:`ddl-storage-columnstore` is disabled.

   There is no difference in storage costs among all character data types.


.. _data-type-bit:

``BIT(n)``
''''''''''

A string representation of a a bit sequence, useful for visualizing a `bit
mask`_.

Values of this type can be created using the bit string literal syntax. A bit
string starts with the ``B`` prefix, followed by a sequence of ``0`` or ``1``
digits quoted within single quotes ``'``.

The optional length specification ``n`` is a positive :ref:`integer
<data-type-numeric>` that defines the maximum length, in characters, of the
values that have to be stored or cast. The minimum length is ``1``. The maximum
length is defined by the upper :ref:`integer <data-type-numeric>` range.

For example::

  cr> CREATE TABLE my_table (
  ...     bit_mask BIT(4)
  ... );
  CREATE OK, 1 row affected (... sec)

::

  cr> INSERT INTO my_table (
  ...     bit_mask
  ... ) VALUES (
  ...     B'0110'
  ... );
  INSERT OK, 1 row affected  (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT bit_mask FROM my_table;
    +----------+
    | bit_mask |
    +----------+
    | B'0110'  |
    +----------+
    SELECT 1 row in set (... sec)

Inserting values that are either too short or too long results in an error::

  cr> INSERT INTO my_table (
  ...     bit_mask
  ... ) VALUES (
  ...    B'00101'
  ... );
  SQLParseException[bit string length 5 does not match type bit(4)]

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _data-type-json:

``json``
''''''''

A type representing a JSON string.

This type only exists for compatibility and interoperability with PostgreSQL. It cannot to be
used in data definition statements and it is not possible to use it to store data.
To store JSON data use the existing :ref:`OBJECT <object_data_type>` type. It is a more powerful
alternative that offers more flexibility and delivering the same benefits.

The JSON types primary use is in :ref:`type casting <type_cast>` for interoperability with PostgreSQL clients which may use the ``JSON`` type.
The following type casts are example of supported usage of the ``JSON`` data type:

Casting from ``STRING`` to ``JSON``::

    cr> SELECT '{"x": 10}'::json;
    +-------------+
    | '{"x": 10}' |
    +-------------+
    | {"x": 10}   |
    +-------------+
    SELECT 1 row in set (... sec)

Casting from ``JSON`` to ``OBJECT``::

    cr> SELECT ('{"x": 10}'::json)::object;
    +-----------+
    | {"x"=10}  |
    +-----------+
    | {"x": 10} |
    +-----------+
    SELECT 1 row in set (... sec)


Casting from ``OBJECT`` to ``JSON``::

    cr> SELECT {x=10}::json;
    +------------+
    | '{"x":10}' |
    +------------+
    | {"x":10}   |
    +------------+
    SELECT 1 row in set (... sec)


.. _data-types-numeric:

Numeric data
------------

CrateDB supports the following numeric types:

.. contents::
   :local:
   :depth: 1


.. _data-types-floating-point:

.. NOTE::

    The :ref:`REAL <type-real>` and :ref:`DOUBLE PRECISION
    <type-double-precision>` data types are inexact, variable-precision
    floating-poin t types, meaning that these types are stored as an
    approximation.

    Accordingly, storage, calculation, and retrieval of the value will not
    always result in an exact representation of the actual floating-point
    value. For instance, the result of applying :ref:`SUM <aggregation-sum>`
    or :ref:`AVG <aggregation-avg>` aggregate functions may slightly vary
    between query executions or comparing floating-point values for equality
    might not always match.

    CrateDB conforms to the `IEEE 754`_ standard concerning special values for
    floating-point data types, meaning that ``NaN``, ``Infinity``,
    ``-Infinity`` (negative infinity), and ``-0`` (signed zero) are all
    supported::

        cr> SELECT
        ...     0.0 / 0.0 AS a,
        ...     1.0 / 0.0 AS B,
        ...     1.0 / -0.0 AS c;
        +-----+----------+-----------+
        | a   | b        | c         |
        +-----+----------+-----------+
        | NaN | Infinity | -Infinity |
        +-----+----------+-----------+
        SELECT 1 row in set (... sec)

    These special numeric values can also be inserted into a column of type
    ``REAL`` or ``DOUBLE PRECISION`` using a :ref:`TEXT <type-text>` literal.

    For instance::

        cr> CREATE TABLE my_table (
        ...     column_1 INTEGER,
        ...     column_2 BIGINT,
        ...     column_3 SMALLINT,
        ...     column_4 DOUBLE PRECISION,
        ...     column_5 REAL,
        ...     column_6 CHAR
        ... );
        CREATE OK, 1 row affected (... sec)

    ::

        cr> INSERT INTO my_table (
        ...     column_4,
        ...     column_5
        ... ) VALUES (
        ...     'NaN',
        ...     'Infinity'
        ... );
        INSERT OK, 1 row affected (... sec)

    ::

        cr> SELECT
        ...     column_4,
        ...     column_5
        ... FROM my_table;
        +----------+----------+
        | column_4 | column_5 |
        +----------+----------+
        | NaN      | Infinity |
        +----------+----------+
        SELECT 1 row in set (... sec)

    .. HIDE:

        cr> DROP TABLE my_table;
        DROP OK, 1 row affected (... sec)


.. _type-smallint:

``SMALLINT``
''''''''''''

A small integer.

Limited to two bytes, with a range from -32,768 to 32,767.

Example::

    cr> CREATE TABLE my_table (
    ...     number SMALLINT
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     32767
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT number FROM my_table;
    +--------+
    | number |
    +--------+
    | 32767  |
    +--------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _type-integer:

``INTEGER``
'''''''''''

An integer.

Limited to four bytes, with a range from -2^31 to 2^31-1.

Example::

    cr> CREATE TABLE my_table (
    ...     number INTEGER
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     2147483647
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT number FROM my_table;
    +------------+
    | number     |
    +------------+
    | 2147483647 |
    +------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _type-bigint:

``BIGINT``
''''''''''

A large integer.

Limited to eight bytes, with a range from -2^63 to 2^63-1.

Example:

::

    cr> CREATE TABLE my_table (
    ...     number BIGINT
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     9223372036854775807
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT number FROM my_table;
    +---------------------+
    | number              |
    +---------------------+
    | 9223372036854775807 |
    +---------------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _type-numeric:

``NUMERIC(precision, scale)``
'''''''''''''''''''''''''''''

An exact number with an arbitrary, user-specified precision.

Variable size, with up to 131072 digits before the decimal point and up to
16383 digits after the decimal point.

For example, using a :ref:`cast from a string literal
<data-types-casting-str>`::

    cr> SELECT NUMERIC(5, 2) '123.45' AS number;
    +--------+
    | number |
    +--------+
    | 123.45 |
    +--------+
    SELECT 1 row in set (... sec)

.. NOTE::

    The ``NUMERIC`` type is only supported as a type literal (i.e., for use in
    SQL :ref:`expressions <gloss-expression>`, like a :ref:`type cast
    <data-types-casting-exp>`, as above).

    You cannot create table columns of type ``NUMERIC``.

This type is usually used when it is important to preserve exact precision
or handle values that exceed the range of the numeric types of the fixed
length. The aggregations and arithmetic operations on numeric values are
much slower compared to operations on the integer or floating-point types.

The ``NUMERIC`` type can be configured with the ``precision`` and
``scale``. The ``precision`` value of a numeric is the total count of
significant digits in the unscaled numeric value. The ``scale`` value of a
numeric is the count of decimal digits in the fractional part, to the right of
the decimal point. For example, the number 123.45 has a precision of ``5`` and
a scale of ``2``. Integers have a scale of zero.

To declare the ``NUMERIC`` type with the precision and scale, use the syntax::

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

An inexact `single-precision floating-point`_ value.

Limited to four bytes, with six to nine decimal digits precision.

Example:

::

    cr> CREATE TABLE my_table (
    ...     number REAL
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     3.4028235e+38
    ... );
    CREATE OK, 1 row affected (... sec)

.. TIP::

    ``3.4028235+38`` represents the value 3.4028235 × 10\ :sup:`38`

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT number FROM my_table;
    +---------------+
    | number        |
    +---------------+
    | 3.4028235e+38 |
    +---------------+
    SELECT 1 row in set (... sec)

You can insert values which exceed the maximum precision, like so::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     3.4028234664e+38
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

However, the recorded value will be an approximation of the original (i.e., the
additional precision is lost)::

    cr> SELECT number FROM my_table;
    +---------------+
    | number        |
    +---------------+
    | 3.4028235e+38 |
    +---------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

.. SEEALSO::

    :ref:`CrateDB floating-point values <data-types-floating-point>`


.. _type-double-precision:

``DOUBLE PRECISION``
''''''''''''''''''''

An inexact number with variable precision supporting `double-precision
floating-point`_ values.

Limited to eight bytes, with 15 to 17 decimal digits precision.

Example:

::

    cr> CREATE TABLE my_table (
    ...     number DOUBLE PRECISION
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     1.7976931348623157e+308
    ... );
    CREATE OK, 1 row affected (... sec)

.. TIP::

    ``1.7976931348623157e+308`` represents the value 1.7976931348623157 × 10\
    :sup:`308`

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT number FROM my_table;
    +-------------------------+
    | TODO                    |
    +-------------------------+
    | 1.7976931348623157e+308 |
    +-------------------------+
    SELECT 1 row in set (... sec)

You can insert values which exceed the maximum precision, like so::

    cr> INSERT INTO my_table (
    ...     number
    ... ) VALUES (
    ...     1.79769313486231572014e+308
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

However, the recorded value will be an approximation of the original (i.e., the
additional precision is lost)::

    cr> SELECT number FROM my_table;
    +-------------------------+
    | number                  |
    +-------------------------+
    | 1.7976931348623157e+308 |
    +-------------------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

.. SEEALSO::

    :ref:`CrateDB floating-point values <data-types-floating-point>`


.. _data-types-dates-times:

Dates and times
---------------

CrateDB supports the following types for dates and times:

.. contents::
   :local:
   :depth: 2

With a few exceptions (noted below) the ``+`` and ``-`` :ref:`operators
<gloss-operator>` can be used to create :ref:`arithmetic expressions
<arithmetic>` with temporal operands:

+---------------+----------------+---------------+
| Operand       | Operator       | Operand       |
+===============+================+===============+
| ``TIMESTAMP`` |          ``-`` | ``TIMESTAMP`` |
+---------------+----------------+---------------+
|  ``INTERVAL`` |          ``+`` | ``TIMESTAMP`` |
+---------------+----------------+---------------+
| ``TIMESTAMP`` | ``+`` or ``-`` |  ``INTERVAL`` |
+---------------+----------------+---------------+
|  ``INTERVAL`` | ``+`` or ``-`` |  ``INTERVAL`` |
+---------------+----------------+---------------+

.. NOTE::

    If an object column is :ref:`dynamically created
    <type-object-columns-dynamic>`, the type detection will not recognize date
    and time types, meaning that date and time type columns must always be
    declared beforehand.


.. _type-timestamp:

``TIMESTAMP``
'''''''''''''

A timestamp expresses a specific date and time as the number of milliseconds
since the `Unix epoch`_ (i.e., ``1970-01-01T00:00:00Z``).

Timestamps can be expressed as string literals (e.g.,
``'1970-01-02T00:00:00'``) with the following syntax:

.. code-block:: text

    date-element [time-separator [time-element [offset]]]

    date-element:   yyyy-MM-dd
    time-separator: 'T' | ' '
    time-element:   HH:mm:ss [fraction]
    fraction:       '.' digit+
    offset:         {+ | -} HH [:mm] | 'Z'

.. SEEALSO::

    For more information about date and time formatting, see `Java 15\:
    Patterns for Formatting and Parsing`_.

    Time zone syntax as defined by `ISO 8601 time zone designators`_.

Internally, CrateDB stores timestamps as :ref:`BIGINT <type-bigint>`
values, which are limited to eight bytes.

If you cast a :ref:`BIGINT <type-bigint>` to a ``TIMEZONE``, the integer value
will be interpreted as the number of milliseconds since the Unix epoch.

Using the :ref:`date_format() <scalar-date_format>` function, for readability::

    cr> SELECT
    ...     date_format(0::TIMESTAMP) AS ts_0,
    ...     date_format(1000::TIMESTAMP) AS ts_1;
    +-----------------------------+-----------------------------+
    | ts_0                        | ts_1                        |
    +-----------------------------+-----------------------------+
    | 1970-01-01T00:00:00.000000Z | 1970-01-01T00:00:01.000000Z |
    +-----------------------------+-----------------------------+
    SELECT 1 row in set (... sec)

If you cast a :ref:`REAL <type-real>` or a :ref:`DOUBLE PRECISION
<type-double-precision>` to a ``TIMESTAMP``, the numeric value wil be
interpreted as the number of seconds since the Unix epoch, with fractional
values approximated to the nearest millisecond::

    cr> SELECT
    ...     date_format(0::TIMESTAMP) AS ts_0,
    ...     date_format(1.5::TIMESTAMP) AS ts_1;
    +-----------------------------+-----------------------------+
    | ts_0                        | ts_1                        |
    +-----------------------------+-----------------------------+
    | 1970-01-01T00:00:00.000000Z | 1970-01-01T00:00:01.500000Z |
    +-----------------------------+-----------------------------+
    SELECT 1 row in set (... sec)

.. CAUTION::

    Due to internal date parsing, the full ``BIGINT`` range is not supported
    for timestamp values. The valid range of dates is from ``292275054BC`` to
    ``292278993AD``.

    When inserting timestamps smaller than ``-999999999999999`` (equal to
    ``-29719-04-05T22:13:20.001Z``) or bigger than ``999999999999999`` (equal
    to ``33658-09-27T01:46:39.999Z``) rounding issues may occur.

A ``TIMESTAMP`` can be further defined as:

.. contents::
   :local:
   :depth: 1


.. _type-timestamp-with-tz:

``WITH TIME ZONE``
..................

If you define a timestamp as ``TIMESTAMP WITH TIME ZONE``, CrateDB will convert
string literals to `Coordinated Universal Time`_ (UTC) using the ``offset``
value (e.g., ``+01:00`` for plus one hour or ``Z`` for UTC).

Example::

    cr> CREATE TABLE my_table (
    ...     ts_tz_1 TIMESTAMP WITH TIME ZONE,
    ...     ts_tz_2 TIMESTAMP WITH TIME ZONE
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     ts_tz_1,
    ...     ts_tz_2
    ... ) VALUES (
    ...     '1970-01-02T00:00:00',
    ...     '1970-01-02T00:00:00+01:00'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT
    ...     ts_tz_1,
    ...     ts_tz_2
    ... FROM my_table;
    +----------+----------+
    |  ts_tz_1 |  ts_tz_2 |
    +----------+----------+
    | 86400000 | 82800000 |
    +----------+----------+
    SELECT 1 row in set (... sec)

You can use :ref:`data_format() <scalar-date_format>` to to make the output
easier to read::

    cr> SELECT
    ...     date_format('%Y-%m-%dT%H:%i', ts_tz_1) AS ts_tz_1,
    ...     date_format('%Y-%m-%dT%H:%i', ts_tz_2) AS ts_tz_2
    ... FROM my_table;
    +------------------+------------------+
    | ts_tz_1          | ts_tz_2          |
    +------------------+------------------+
    | 1970-01-02T00:00 | 1970-01-01T23:00 |
    +------------------+------------------+
    SELECT 1 row in set (... sec)

Notice that ``ts_tz_2`` is smaller than ``ts_tz_1`` by one hour. CrateDB used
the ``+01:00`` offset (i.e., *ahead of UTC by one hour*) to convert the second
timestamp into UTC prior to insertion. Contrast this with the behavior of
:ref:`WITHOUT TIME ZONE <type-timestamp-without-tz>`.

.. HIDE:

    cr> DROP TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

.. NOTE::

    ``TIMESTAMPTZ`` is an alias for ``TIMESTAMP WITH TIME ZONE``.

.. CAUTION::

    In the absence of an explicit :ref:`WITH TIME ZONE
    <type-timestamp-with-tz>` or :ref:`WITHOUT TIME ZONE
    <type-timestamp-without-tz>`, CrateDB will interpret ``TIMEZONE`` as an
    alias for ``TIMESTAMP WITH TIME ZONE``.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. CrateDB 4.0 :ref:`deprecated this alias <v4.0.0-deprecations>`
    and behavior may change in a future version of CrateDB (see `tracking issue
    #11491`_). To avoid issued, we recommend that you always specify ``WITH
    TIME ZONE`` or ``WITHOUT TIME ZONE``.


.. _type-timestamp-without-tz:

``WITHOUT TIME ZONE``
.....................

If you define a timestamp as ``TIMESTAMP WITHOUT TIME ZONE``, CrateDB will
convert string literals to `Coordinated Universal Time`_ (UTC) without using
the ``offset`` value (i.e., any time zone information present is stripped prior
to insertion).

Example::

    cr> CREATE TABLE my_table (
    ...     ts_1 TIMESTAMP WITHOUT TIME ZONE,
    ...     ts_2 TIMESTAMP WITHOUT TIME ZONE
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     ts_1,
    ...     ts_2
    ... ) VALUES (
    ...     '1970-01-02T00:00:00',
    ...     '1970-01-02T00:00:00+01:00'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

Using the :ref:`data_format() <scalar-date_format>` function, for readability::

    cr> SELECT
    ...     date_format('%Y-%m-%dT%H:%i', ts_1) AS ts_1,
    ...     date_format('%Y-%m-%dT%H:%i', ts_2) AS ts_2
    ... FROM my_table;
    +------------------+------------------+
    | ts_1             | ts_2             |
    +------------------+------------------+
    | 1970-01-02T00:00 | 1970-01-02T00:00 |
    +------------------+------------------+
    SELECT 1 row in set (... sec)

Notice that ``ts_1`` and ``ts_2`` are identical. CrateDB ignored the ``+01:00``
offset (i.e., *ahead of UTC by one hour*) when processing the second string
literal. Contrast this with the behavior of :ref:`WITH TIME ZONE
<type-timestamp-with-tz>`.

.. HIDE:

    cr> DROP TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

.. CAUTION::

    In the absence of an explicit :ref:`WITH TIME ZONE
    <type-timestamp-with-tz>` or :ref:`WITHOUT TIME ZONE
    <type-timestamp-without-tz>`, CrateDB will interpret ``TIMEZONE`` as an
    alias for ``TIMESTAMP WITH TIME ZONE``.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. CrateDB 4.0 :ref:`deprecated this alias <v4.0.0-deprecations>`
    and behavior may change in a future version of CrateDB (see `tracking issue
    #11491`_). To avoid issued, we recommend that you always specify ``WITH
    TIME ZONE`` or ``WITHOUT TIME ZONE``.


.. _type-timestamp-at-tz:

``AT TIME ZONE``
................

You can use the ``AT TIME ZONE`` clause to modify a timestamp in one of two
different ways:

.. contents::
   :local:
   :depth: 1

.. NOTE::

    The ``AT TIME ZONE`` type is only supported as a type literal (i.e., for
    use in SQL :ref:`expressions <gloss-expression>`, like a :ref:`type cast
    <data-types-casting-exp>`, as below).

    You cannot create table columns of type ``NUMERIC``.


.. _type-timestamp-tz-at-tz-convert:

Convert a timestamp time zone
`````````````````````````````

If you use ``AT TIME ZONE tz`` with a ``TIMESTAMP WITH TIME ZONE``, CrateDB
will convert timestamp to time zone ``tz`` and cast the return value as a
:ref:`TIMESTAMP WITHOUT TIME ZONE <type-timestamp-without-tz>` (which discards
the the time zone information). This process effectively allows you to correct
the offset used to calculate UTC.

Example::

    cr> CREATE TABLE my_table (
    ...     ts_tz TIMESTAMP WITH TIME ZONE
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     ts_tz
    ... ) VALUES (
    ...     '1970-01-02T00:00:00'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

Using the :ref:`data_format() <scalar-date_format>` function, for readability::

    cr> SELECT date_format(
    ...     '%Y-%m-%dT%H:%i', ts_tz AT TIME ZONE '+01:00'
    ... ) AS ts
    ... FROM my_table;
    +------------------+
    | ts               |
    +------------------+
    | 1970-01-02T01:00 |
    +------------------+
    SELECT 1 row in set (... sec)

.. TIP::

    The ``AT TIME ZONE`` clause does the same as the :ref:`timezone()
    <scalar-timezone>` function::

        cr> SELECT date_format(
        ...     '%Y-%m-%dT%H:%i', timezone('+01:00', ts_tz)
        ... ) AS ts
        ... FROM my_table;
        +------------------+------------------+
        | ts_1             | ts_2             |
        +------------------+------------------+
        | 1970-01-02T01:00 | 1970-01-02T01:00 |
        +------------------+------------------+
        SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    REFRESH OK, 1 row affected (... sec)


.. _type-timestamp-at-tz-add:

Add a timestamp time zone
`````````````````````````

If you use ``AT TIME ZONE`` with a :ref:`TIMESTAMP WITHOUT TIME ZONE
<type-timestamp-with-tz>`, CrateDB will add the missing time zone information,
recalculate the timestamp in UTC, and cast the return value as a
:ref:`TIMESTAMP WITH TIME ZONE <type-timestamp-without-tz>`.

Example::

    cr> CREATE TABLE my_table (
    ...     ts TIMESTAMP WITHOUT TIME ZONE
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     ts
    ... ) VALUES (
    ...     '1970-01-02T00:00:00'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

Using the :ref:`data_format() <scalar-date_format>` function, for readability::

    cr> SELECT date_format(
    ...     '%Y-%m-%dT%H:%i', ts AT TIME ZONE '+01:00'
    ... ) AS ts_tz
    ... FROM my_table;
    +------------------+
    | ts_tz            |
    +------------------+
    | 1970-01-01T23:00 |
    +------------------+
    SELECT 1 row in set (... sec)

.. TIP::

    The ``AT TIME ZONE`` clause does the same as the :ref:`timezone()
    <scalar-timezone>` function::

        cr> SELECT date_format(
        ...     '%Y-%m-%dT%H:%i', timezone('+01:00', ts)
        ... ) AS ts_tz
        ... FROM my_table;
        +------------------+
        | ts_tz            |
        +------------------+
        | 1970-01-01T23:00 |
        +------------------+
        SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    REFRESH OK, 1 row affected (... sec)


.. _type-time:

``TIME``
''''''''

A ``TIME`` expresses a specific time as the number of milliseconds
since midnight along with a time zone offset.

Limited to 12 bytes, with a time range from ``00:00:00.000000`` to
``23:59:59.999999`` and a time zone range from ``-18:00`` to ``18:00``.

.. CAUTION::

    CrateDB does not support ``TIME`` by itself or ``TIME WITHOUT TIME ZONE``.
    You must always specify ``TIME WITH TIME ZONE`` or its alias ``TIMETZ``.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. This behavior may change in a future version of CrateDB (see
    `tracking issue #11491`_).

.. NOTE::

    The ``DATE`` type is only supported as a type literal (i.e., for use in
    SQL :ref:`expressions <gloss-expression>`, like a :ref:`type cast
    <data-types-casting-exp>`, as below).

Times can be expressed as string literals (e.g., ``'13:00:00'``) with the
following syntax:

.. code-block:: text

    time-element [offset]

    time-element: time-only [fraction]
    time-only:    HH[[:][mm[:]ss]]
    fraction:     '.' digit+
    offset:       {+ | -} time-only | geo-region
    geo-region:   As defined by ISO 8601.

Above, ``fraction`` accepts up to six digits, with a precision in microseconds.

.. SEEALSO::

    For more information about time formatting, see `Java 15\: Patterns for
    Formatting and Parsing`_.

    Time zone syntax as defined by `ISO 8601 time zone designators`_.

For example::

    cr> SELECT '13:00:00'::TIMETZ AS t_tz;
    +------------------+
    | t_tz             |
    +------------------+
    | [46800000000, 0] |
    +------------------+
    SELECT 1 row in set (... sec)

The value of first element is the number of milliseconds since midnight. The
value of the second element is the number of seconds corresponding to the time
zone offset (zero in this instance, as no time zone was specified).

For example, with a ``+01:00`` time zone::

    cr> SELECT '13:00:00+01:00'::TIMETZ AS t_tz;
    +---------------------+
    | t_tz                |
    +---------------------+
    | [46800000000, 3600] |
    +---------------------+
    SELECT 1 row in set (... sec)

The time zone offset is calculated as 3600 seconds, which is equivalent to an
hour.

Negative time zone offsets will return negative seconds::

    cr> SELECT '13:00:00-01:00'::TIMETZ AS t_tz;
    +----------------------+
    | t_tz                 |
    +----------------------+
    | [46800000000, -3600] |
    +----------------------+
    SELECT 1 row in set (... sec)

Here's an example that uses fractional seconds::

    cr> SELECT '13:59:59.999999'::TIMETZ as t_tz;
    +------------------+
    | 13:59:59.999999  |
    +------------------+
    | [50399999999, 0] |
    +------------------+
    SELECT 1 row in set (... sec)

.. CAUTION::

    The current implementation of the ``TIME`` type has the following
    limitations:

    .. rst-class:: open

    - ``TIME`` types cannot be :ref:`cast <data-types-casting-exp>` to any
      other types (including :ref:`TEXT <type-text>`)

    - ``TIME`` types cannot be used in :ref:`arithmetic expressions
      <arithmetic>` (e.g., with ``TIME``, ``DATE``, and
      ``INTERVAL`` types)

    - ``TIME`` types cannot be used with time and date scalar functions (e.g.,
      :ref:`date_format() <scalar-date_format>` and :ref:`extract()
      <scalar-extract>`)

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. This behavior may change in a future version of CrateDB (see
    `tracking issue #11528`_).


.. _type-date:

``DATE``
''''''''

A ``DATE`` expresses a specific year, month and a day in `UTC`_.

Internally, CrateDB stores dates as :ref:`BIGINT <type-bigint>` values, which
are limited to eight bytes.

If you cast a :ref:`BIGINT <type-bigint>` to a ``DATE``, the integer value will
be interpreted as the number of milliseconds since the Unix epoch. If you cast
a :ref:`REAL <type-real>` or a :ref:`DOUBLE PRECISION <type-double-precision>`
to a ``DATE``, the numeric value wil be interpreted as the number of seconds
since the Unix epoch.

.. CAUTION::

    Due to internal date parsing, the full ``BIGINT`` range is not supported
    for timestamp values. The valid range of dates is from ``292275054BC`` to
    ``292278993AD``.

    When inserting dates smaller than ``-999999999999999`` (equal to
    ``-29719-04-05``) or bigger than ``999999999999999`` (equal
    to ``33658-09-27``) rounding issues may occur.

.. _type-date-warning:

.. WARNING::

    The ``DATE`` type was not designed to allow time-of-day information (i.e.,
    it is supposed to have a resolution of one day).

    However, CrateDB allows you violate that constraint by casting any number
    of milliseconds within limits to a ``DATE`` type. The result is then
    returned as a :ref:`TIMESTAMP <type-timestamp>`. When used in conjunction
    with :ref:`arithmetic expressions <arithmetic>`, these ``TIMESTAMP`` values
    may produce unexpected results.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. This behavior may change in a future version of CrateDB (see
    `tracking issue #11528`_).

.. CAUTION::

    The current implementation of the ``DATE`` type has the following
    limitations:

    .. rst-class:: open

    - ``DATE`` types cannot be added or subtracted to or from other ``DATE``
      types as expected (i.e., to calculate the difference between the two in
      a number of days).

      Doing so will convert both ``DATE`` values into ``TIMESTAMP`` values
      before performing the operation, resulting in a ``TIMESTAMP`` value
      corresponding to a full date and time (see :ref:`WARNING
      <type-date-warning>` above).

    - :ref:`Numeric data types <data-types-numeric>` cannot be added to or
      subtracted from ``DATE`` types as expected (e.g., to increase the date by
      ``n`` days).

      Doing so will, for example, convert the ``DATE`` into a ``TIMESTAMP`` and
      increase the value by ``n`` milliseconds (see :ref:`WARNING
      <type-date-warning>` above).

    - :ref:`TIME <type-time>` types cannot be added to or subtracted from
      ``DATE`` types.

    - :ref:`INTERVAL <type-interval>` types cannot be added to or subtracted
      from ``DATE`` types.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. This behavior may change in a future version of CrateDB (see
    `tracking issue #11528`_).

.. NOTE::

    The ``DATE`` type is only supported as a type literal (i.e., for use in
    SQL :ref:`expressions <gloss-expression>`, like a :ref:`type cast
    <data-types-casting-exp>`, as below).

Dates can be expressed as string literals (e.g., ``'2021-03-09'``) with the
following syntax:

.. code-block:: text

    yyyy-MM-dd

.. SEEALSO::

    For more information about date and time formatting, see `Java 15\:
    Patterns for Formatting and Parsing`_.

For example, using the :ref:`data_format() <scalar-date_format>` function, for
readability::


    cr> SELECT
    ...    date_format(
    ...        '%Y-%m-%d',
    ...        '2021-03-09'::DATE
    ...    ) AS date;
    +------------+
    | date       |
    +------------+
    | 2021-03-09 |
    +------------+
    SELECT 1 row in set (... sec)


.. _type-interval:

``INTERVAL``
''''''''''''

An ``INTERVAL`` represents a span of time.

.. NOTE::

    The ``INTERVAL`` type is only supported as a type literal (i.e., for use in
    SQL :ref:`expressions <gloss-expression>`, like a :ref:`type cast
    <data-types-casting-exp>`, as above).

    You cannot create table columns of type ``INTERVAL``.

The basic syntax is::

    INTERVAL <quantity> <unit>

Where ``unit`` can be any of the following:

- ``YEAR``
- ``MONTH``
- ``DAY``
- ``HOUR``
- ``MINUTE``
- ``SECOND``

For example::

    cr> SELECT INTERVAL '1' DAY AS result;
    +----------------+
    | result         |
    +----------------+
    | 1 day 00:00:00 |
    +----------------+
    SELECT 1 row in set (... sec)

Intervals can be positive or negative::

    cr> SELECT INTERVAL -'1' DAY AS result;
    +-----------------+
    | result          |
    +-----------------+
    | -1 day 00:00:00 |
    +-----------------+
    SELECT 1 row in set (... sec)

When using ``SECOND``, you can define fractions of a seconds (with a precision
of zero to six digits)::

    cr> SELECT INTERVAL '1.5' SECOND AS result;
    +--------------+
    | result       |
    +--------------+
    | 00:00:01.500 |
    +--------------+
    SELECT 1 row in set (... sec)

.. CAUTION::

    The ``INTERVAL`` data type does not currently support the input units
    ``MILLENNIUM``, ``CENTURY``, ``DECADE``, ``MILLISECOND``, or
    ``MICROSECOND``.

    This behaviour does not comply with standard SQL and is incompatible with
    PostgreSQL. This behavior may change in a future version of CrateDB (see
    `tracking issue #11490`_).

You can also use the following syntax to express an interval::

    INTERVAL <string>

Where ``string`` describes the interval using one of the recognized formats:

+----------------------+-----------------------+---------------------+
| Description          | Example               | Equivalent          |
+======================+=======================+=====================+
| Standard SQL format  | ``1-2``               | 1 year 2 months     |
| (year-month)         |                       |                     |
+----------------------+-----------------------+---------------------+
| Standard SQL format  | ``1-2 3 4:05:06``     | 1 year 2 months     |
|                      |                       | 3 days 4 hours      |
|                      |                       | 5 minutes 6 seconds |
+----------------------+-----------------------+---------------------+
| Standard SQL format  | ``3 4:05:06``         | 3 days 4 hours      |
| (day-time)           |                       | 5 minutes 6 seconds |
+----------------------+-----------------------+---------------------+
| `PostgreSQL interval | ``1 year 2 months     | 1 year 2 months     |
| format`_             | 3 days 4 hours        | 3 days 4 hours      |
|                      | 5 minutes 6 seconds`` | 5 minutes 6 seconds |
+----------------------+-----------------------+---------------------+
| `ISO 8601 duration   | ``P1Y2M3DT4H5M6S``    | 1 year 2 months     |
| format`_             |                       | 3 days 4 hours      |
|                      |                       | 5 minutes 6 seconds |
+----------------------+-----------------------+---------------------+

For example::

    cr> SELECT INTERVAL '1-2 3 4:05:06' AS result;
    +-------------------------------+
    | result                        |
    +-------------------------------+
    | 1 year 2 mons 3 days 04:05:06 |
    +-------------------------------+
    SELECT 1 row in set (... sec)

You can limit the precision of an interval by specifying ``<unit> TO
<unit>`` after the interval ``string``.

For example, you can use ``YEAR TO MONTH`` to limit an interval to a day-month
value::

    cr> SELECT INTERVAL '1-2 3 4:05:06' YEAR TO MONTH AS result;
    +------------------------+
    | result                 |
    +------------------------+
    | 1 year 2 mons 00:00:00 |
    +------------------------+
    SELECT 1 row in set (... sec)

You can use ``DAY TO HOUR``, as another example, to limit a day-time interval
to days and hours::

    cr> SELECT INTERVAL '3 4:05:06' DAY TO HOUR AS result;
    +-----------------+
    | result          |
    +-----------------+
    | 3 days 04:00:00 |
    +-----------------+
    SELECT 1 row in set (... sec)

.. TIP::

    You can use intervals in combination with :ref:`CURRENT_TIMESTAMP
    <current_timestamp>` to calculate values that are offset relative to the
    current date and time.

    For example, to calculate a timestamp corresponding to exactly one day ago,
    use::

        cr>  SELECT CURRENT_TIMESTAMP - INTERVAL '1' DAY AS result;
        +---------------+
        | result        |
        +---------------+
        | 1623847247282 |
        +---------------+
        SELECT 1 row in set (... sec)


.. _data-types-ip-addresses:

IP addresses
------------


.. _type-ip:

``IP``
''''''

An ``IP`` is a string representation of an `IP adddress`_ (IPv4 or IPv6).

Internally IP addresses are stored as ``BIGINT`` values, allowing expected
sorting, filtering, and aggregation.

For example::

    cr> CREATE TABLE my_table (
    ...     fqdn TEXT,
    ...     ip_addr IP
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     fqdn,
    ...     ip_addr
    ... ) VALUES (
    ...     'localhost',
    ...     '127.0.0.1'
    ... ), (
    ...     'router.local',
    ...     '0:0:0:0:0:ffff:c0a8:64'
    ... );
    INSERT OK, 2 rows affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT fqdn, ip_addr FROM my_table;
    +--------------+---------------+
    | fqdn         | ip_addr       |
    +--------------+---------------+
    | router.local | 192.168.0.100 |
    | localhost    | 127.0.0.1     |
    +--------------+---------------+
    SELECT 2 rows in set (... sec)

The ``fqdn`` column (see `Fully Qualified Domain Name`_) will accept any value
because it was specified as :ref:`TEXT <type-text>`. However, trying to insert
``fake.ip`` won't work, because it is not a correctly formatted ``IP``
address::

    cr> INSERT INTO my_table (
    ...     fqdn,
    ...     ip_addr
    ... ) VALUES (
    ...     'localhost',
    ...     'fake.ip'
    ... );
    SQLParseException[Cannot cast `'fake.ip'` of type `text` to type `ip`]

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

IP addresses support the ``<<`` :ref:`operator <gloss-operator>`, which checks
for subnet inclusion using `CIDR notation`_. The left-hand :ref:`operand
<gloss-operand>` must an :ref:`IP <ip-type>` type and the right-hand must be
:ref:`TEXT <data-type-text>` type (e.g., ``'192.168.1.5' << '192.168.1/24'``).


.. _data-types-container:

Container types
===============

Container types are types with :ref:`nonscalar <gloss-nonscalar>` values that
may contain other values:

.. contents::
   :local:
   :depth: 3


.. _data-types-objects:

Objects
-------


.. _type-object:

``OBJECT``
''''''''''

An object is structured as a collection of key-values.

An object can contain any other type, including further child objects. An
``OBJECT`` column can be schemaless or can have a defined (i.e., enforced)
schema.

Objects are not the same as JSON objects, although they share a lot of
similarities. However, objects can be :ref:`inserted as JSON strings
<data-type-object-json>`.

Syntax::

    <columnName> OBJECT
        [ ({DYNAMIC|STRICT|IGNORED}) ]
        [ AS ( <columnDefinition>* ) ]

The only required syntax is ``OBJECT``.

The column policy (``DYNAMIC``, ``STRICT``, or ``IGNORED``) is optional and
defaults to :ref:`DYNAMIC <type-object-columns-dynamic>`.

If the optional list of subcolumns (``columnDefinition``) is omitted, the
object will have no schema. CrateDB will create a schema for :ref:`DYNAMIC
<type-object-columns-dynamic>` objects upon first insert.

Example::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     quotation OBJECT,
    ...     protagonist OBJECT(STRICT) AS (
    ...         age INTEGER,
    ...         first_name TEXT,
    ...         details OBJECT AS (
    ...             birthday TIMESTAMP WITH TIME ZONE
    ...         )
    ...     )
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     title,
    ...     quotation,
    ...     protagonist
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "words" = 'Curiouser and curiouser!',
    ...         "length" = 3
    ...     },
    ...     {
    ...         "age" = '10',
    ...         "first_name" = 'Alice',
    ...         "details" = {
    ...             "birthday" = '1852-05-04T00:00Z'::TIMESTAMPTZ
    ...         }
    ...     }
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT
    ...     protagonist['first_name'] AS name,
    ...     date_format(
    ...         '%D %b %Y',
    ...         'GMT',
    ...         protagonist['details']['birthday']
    ...      ) AS born,
    ...     protagonist['age'] AS age
    ... FROM my_table;
    +-------+--------------+-----+
    | name  | born         | age |
    +-------+--------------+-----+
    | Alice | 4th May 1852 |  10 |
    +-------+--------------+-----+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _type-object-column-policy:

Object column policy
....................


.. _type-object-columns-strict:

``STRICT``
``````````

If the column policy is configured as ``STRICT``, CrateDB will reject any
subcolumn that is not defined upfront by ``columnDefinition``.

Example::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     protagonist OBJECT(STRICT) AS (
    ...         name TEXT
    ...     )
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     title,
    ...     protagonist
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "age" = '10',
    ...     }
    ... );
    SQLParseException[line 8:5: no viable alternative at input 'VALUES (\n    'Alice in Wonderland',\n    {\n        "age" = '10',\n    }']

The insert above failed because the ``protagonist`` column defines a ``name``
column and does not define an ``age`` column.

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

.. NOTE::

    Objects with a ``STRICT`` column policy and no ``columnDefinition`` will be
    have one unusable column that will always be null.


.. _type-object-columns-dynamic:

``DYNAMIC``
```````````

If the column policy is configured as ``DYNAMIC`` (the default), inserts may
dynamically add new subcolumns to the object definition.

Example::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     quotation OBJECT
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

The following statement is equivalent to the above::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     quotation OBJECT(DYNAMIC)
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)

The following statement is also equivalent to the above::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     quotation OBJECT(DYNAMIC) AS (
    ...         words TEXT,
    ...         length SMALLINT
    ...     )
    ... );
    CREATE OK, 1 row affected (... sec)

You can insert using the existing columns::

    cr> INSERT INTO my_table (
    ...     title,
    ...     quotation
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "words" = 'Curiouser and curiouser!',
    ...         "length" = 3
    ...     }
    ... );
    CREATE OK, 1 row affected (... sec)

Or you can add new columns::

    cr> INSERT INTO my_table (
    ...     title,
    ...     quotation
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "words" = 'DRINK ME',
    ...         "length" = 2,
    ...         "chapter" = 1
    ...     }
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

All rows have the same columns (including newly added columns), but missing
records will be returned as :ref:`NULL <type-null>` values::

    cr> SELECT
    ...     quotation['chapter'] as chapter,
    ...     quotation['words'] as quote
    ... FROM my_table
    ... ORDER BY chapter ASC;
    +---------+--------------------------+
    | chapter | quote                    |
    +---------+--------------------------+
    |       1 | DRINK ME                 |
    |    NULL | Curiouser and curiouser! |
    +---------+--------------------------+
    SELECT 2 rows in set (0.114 sec)

New columns are usable like any other subcolumn. You can retrieve them, sort by
them, and use them in where clauses.

.. HIDE:

    cr> DROP TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

.. NOTE::

    Adding new columns to an object with a ``DYNAMIC`` policy will affect the
    schema of the table.

    Once a column is added, it shows up in the ``information_schema.columns``
    table and its type and attributes are fixed. If a new column ``a`` was
    added with type ``INTEGER``, adding strings to the column will result in an
    error.

    Dynamically added columns will always be analyzed as-is with the
    :ref:`plain analyzer <plain-analyzer>`, which means the column will be
    indexed but not tokenized in the case of ``TEXT`` columns.


.. _type-object-columns-ignored:

``IGNORED``
```````````

If the column policy is configured as ``IGNORED``, inserts may dynamically add
new subcolumns to the object definition. However, dynamically added subcolumns
do not cause a schema update and the values contained will not be indexed.

Because dynamically created columns are not recorded in the schema, you can
insert mixed types into them. For example, one row may insert an integer and
the next row may insert an object. Objects with a :ref:`STRICT
<type-object-columns-strict>` or :ref:`DYNAMIC <type-object-columns-dynamic>`
column policy do not allow this.

Example::

    cr> CREATE TABLE my_table (
    ...     title TEXT,
    ...     protagonist OBJECT(IGNORED) AS (
    ...         name TEXT,
    ...         chapter SMALLINT
    ...     )
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> INSERT INTO my_table (
    ...     title,
    ...     protagonist
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "name" = 'Alice',
    ...         "chapter" = 1,
    ...         "size" = {
    ...             "value" = 10,
    ...             "units" = 'inches'
    ...         }
    ...     }
    ... );

::

    cr> INSERT INTO my_table (
    ...     title,
    ...     protagonist
    ... ) VALUES (
    ...     'Alice in Wonderland',
    ...     {
    ...         "name" = 'Alice',
    ...         "chapter" = 2,
    ...         "size" = 'As big as a room'
    ...     }
    ... );

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT
    ...     protagonist['name'] as name,
    ...     protagonist['chapter'] as chapter,
    ...     pg_typeof(protagonist['size']) as size
    ... FROM my_table
    ... ORDER BY protagonist['chapter'] ASC;
    +-------------------------------+
    | payload                       |
    +-------------------------------+
    | {"tag": "AT", "value": 30}    |
    | {"tag": "AT", "value": "str"} |
    +-------------------------------+
    SELECT 2 rows in set (... sec)

::

    cr> CREATE TABLE my_table (
    ...     TODO TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO my_table (
    ...     TODO
    ... ) VALUES (
    ...     'TODO'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM my_table;
    +--------------+
    | TODO         |
    +--------------+
    | TODO         |
    +--------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


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

    { id = 1, name = 'foo', tags = ['apple'], size = 3.1415, valid = ? }

.. NOTE::

   Even though they look like JSON, object literals are not JSON. If you want
   to use JSON, skip to the next subsection.

.. SEEALSO::

    :ref:`Selecting values from inner objects and nested objects
    <sql_dql_objects>`


.. _data-types-object-json:

Inserting objects as JSON
.........................

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

    '{ "nested_obj_col": { "int_col": 1234, "str_col": "foo" } }'::object

.. NOTE::

    You cannot use :ref:`placeholder parameters <sql-parameter-reference>`
    inside a JSON string.


.. _data-types-arrays:

Arrays
------


.. _type-array:

``ARRAY``
'''''''''

An array is structured as a collection of other data types.

Arrays can contain the following:

* :ref:`Primitive types <data-types-primitive>`
* :ref:`Objects <type-object>`
* :ref:`Geographic types <data-types-geo>`

Array types are defined as follows::

    cr> CREATE TABLE my_table_arrays (
    ...     tags ARRAY(TEXT),
    ...     objects ARRAY(OBJECT AS (age INTEGER, name TEXT))
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> CREATE TABLE my_table (
    ...     TODO TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO my_table (
    ...     TODO
    ... ) VALUES (
    ...     'TODO'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM my_table;
    +--------------+
    | TODO         |
    +--------------+
    | TODO         |
    +--------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


An alternative is the following syntax to refer to arrays::

    <typeName>[]

This means ``TEXT[]`` is equivalent to ``ARRAY(text)``.

.. NOTE::

    Currently arrays cannot be nested. Something like ``ARRAY(ARRAY(TEXT))``
    won't work.

Arrays are always represented as zero or more literal elements inside square
brackets (``[]``), for example::

    [1, 2, 3]
    ['Zaphod', 'Ford', 'Arthur']


.. _data-types-array-literals:

Array literals
..............

Arrays can be written using the array constructor ``ARRAY[]`` or short ``[]``.
The array constructor is an :ref:`expression <gloss-expression>` that accepts
both literals and expressions as its parameters. Parameters may contain zero or
more elements.

Synopsis::

    [ ARRAY ] '[' element [ , ... ] ']'

All array elements must have the same data type, which determines the inner
type of the array. If an array contains no elements, its element type will be
inferred by the context in which it occurs, if possible.

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


.. _type-geo_point:

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

::

    cr> CREATE TABLE my_table (
    ...     TODO TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO my_table (
    ...     TODO
    ... ) VALUES (
    ...     'TODO'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM my_table;
    +--------------+
    | TODO         |
    +--------------+
    | TODO         |
    +--------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)



.. _data-type-geo-shape:

Geometric shapes
----------------


.. _type-geo_shape:

``GEO_SHAPE``
'''''''''''''

A ``geo_shape`` is a :ref:`geographic data type <data-types-geo>` used to store
2D shapes defined as `GeoJSON geometry objects`_.

A ``GEO_SHAPE`` column can store different kinds of `GeoJSON geometry
objects`_. Thus it is possible to store e.g. ``LineString`` and
``MultiPolygon`` shapes in the same column.

.. NOTE::

    3D coordinates are not supported.

    Empty ``Polygon`` and ``LineString`` geo shapes are not supported.

::

    cr> CREATE TABLE my_table (
    ...     TODO TEXT
    ... );
    CREATE OK, 1 row affected (... sec)

TODO::

    cr> INSERT INTO my_table (
    ...     TODO
    ... ) VALUES (
    ...     'TODO'
    ... );
    CREATE OK, 1 row affected (... sec)

.. HIDE:

    cr> REFRESH TABLE my_table;
    REFRESH OK, 1 row affected (... sec)

TODO::

    cr> SELECT * FROM my_table;
    +--------------+
    | TODO         |
    +--------------+
    | TODO         |
    +--------------+
    SELECT 1 row in set (... sec)

.. HIDE:

    cr> DROP TABLE my_table;
    DROP OK, 1 row affected (... sec)


.. _type-geo_shape-definition:

Geo shape column definition
...........................

To define a ``GEO_SHAPE`` column::

    <columnName> geo_shape

A geographical index with default parameters is created implicitly to allow for
geographical queries.

The default definition for the column type is::

    <columnName> GEO_SHAPE INDEX USING geohash
        WITH (precision='50m', distance_error_pct=0.025)

There are two geographic index types: ``geohash`` (the default) and
``quadtree``. These indices are only allowed on ``geo_shape`` columns. For more
information, see :ref:`geo_shape_data_type_index`.

Both of these index types accept the following parameters:

``precision``
  (Default: ``50m``) Define the maximum precision of the used index and
  thus for all indexed shapes. Given as string containing a number and
  an optional distance unit (defaults to ``m``).

  Supported units are ``inch`` (``in``), ``yard`` (``yd``), ``miles``
  (``mi``), ``kilometers`` (``km``), ``meters`` (``m``), ``centimeters``
  (``cm``), ``millimeters`` (``mm``).

``distance_error_pct``
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

``tree_levels``
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


.. _type-geo_shape-index:

Geo shape index structure
.........................

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


.. _type-geo_shape-literals:

Geo shape literals
..................

Columns with the ``GEO_SHAPE`` type are represented and inserted as object
containing a valid `GeoJSON`_ geometry object::

    {
        type = 'Polygon',
        coordinates = [
            [
                [100.0, 0.0],
                [101.0, 0.0],
                [101.0, 1.0],
                [100.0, 1.0],
                [100.0, 0.0]
            ]
        ]
    }

Alternatively a `WKT`_ string can be used to represent a ``GEO_SHAPE`` as
well::

    'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'

.. NOTE::

    It is not possible to detect a ``GEO_SHAPE`` type for a dynamically created
    column. Like with :ref:`type-geo_point` type, ``GEO_SHAPE`` columns need to
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

+-----------------------+------------------------------+
| Alias                 | CrateDB Type                 |
+=======================+==============================+
| ``SHORT``             | ``SMALLINT``                 |
+-----------------------+------------------------------+
| ``INT``               | ``INTEGER``                  |
+-----------------------+------------------------------+
| ``INT2``              | ``SMALLINT``                 |
+-----------------------+------------------------------+
| ``INT4``              | ``INTEGER``                  |
+-----------------------+------------------------------+
| ``INT8``              | ``BIGINT``                   |
+-----------------------+------------------------------+
| ``LONG``              | ``BIGINT``                   |
+-----------------------+------------------------------+
| ``STRING``            | ``TEXT``                     |
+-----------------------+------------------------------+
| ``VARCHAR``           | ``TEXT``                     |
+-----------------------+------------------------------+
| ``CHARACTER VARYING`` | ``TEXT``                     |
+-----------------------+------------------------------+
| ``NAME``              | ``TEXT``                     |
+-----------------------+------------------------------+
| ``REGPROC``           | ``TEXT``                     |
+-----------------------+------------------------------+
| ``BYTE``              | ``CHAR``                     |
+-----------------------+------------------------------+
| ``FLOAT``             | ``REAL``                     |
+-----------------------+------------------------------+
| ``FLOAT4``            | ``REAL``                     |
+-----------------------+------------------------------+
| ``FLOAT8``            | ``DOUBLE PRECISION``         |
+-----------------------+------------------------------+
| ``DOUBLE``            | ``DOUBLE PRECISION``         |
+-----------------------+------------------------------+
| ``TIMESTAMP``         | ``TIMESTAMP WITH TIME ZONE`` |
+-----------------------+------------------------------+
| ``TIMESTAMPTZ``       | ``TIMESTAMP WITH TIME ZONE`` |
+-----------------------+------------------------------+

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

   It is not possible to cast to or from ``OBJECT``, ``GEO_POINT``, and
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
  <data-types-primitive>` only. For complex types such as ``ARRAY`` or
  ``OBJECT``, use the :ref:`type_cast` syntax.


.. _BigDecimal documentation: https://docs.oracle.com/en/java/javase/15/docs/api/java.base/java/math/BigDecimal.html
.. _bit mask: https://en.wikipedia.org/wiki/Mask_(computing)
.. _CIDR notation: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation
.. _Coordinated Universal Time: https://en.wikipedia.org/wiki/Coordinated_Universal_Time
.. _double-precision floating-point: https://en.wikipedia.org/wiki/Double-precision_floating-point_format
.. _Geohash: https://en.wikipedia.org/wiki/Geohash
.. _GeoJSON geometry objects: https://tools.ietf.org/html/rfc7946#section-3.1
.. _GeoJSON: https://geojson.org/
.. _IEEE 754: https://ieeexplore.ieee.org/document/30711/?arnumber=30711&filter=AND(p_Publication_Number:2355)
.. _ISO 8601 duration format: https://en.wikipedia.org/wiki/ISO_8601#Durations
.. _ISO 8601 time zone designators: https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators
.. _Java 15\: Patterns for Formatting and Parsing: https://docs.oracle.com/en/java/javase/15/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns
.. _pg_class: https://www.postgresql.org/docs/10/static/catalog-pg-class.html
.. _pg_proc: https://www.postgresql.org/docs/10/static/catalog-pg-proc.html
.. _PostgreSQL interval format: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
.. _Quadtree: https://en.wikipedia.org/wiki/Quadtree
.. _single-precision floating-point: https://en.wikipedia.org/wiki/Single-precision_floating-point_format
.. _Trie: https://en.wikipedia.org/wiki/Trie
.. _Tries: https://en.wikipedia.org/wiki/Trie
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text
.. _UTC: `Coordinated Universal Time`_
.. _Coordinated Universal Time: https://en.wikipedia.org/wiki/Coordinated_Universal_Time
.. _Unix epoch: https://en.wikipedia.org/wiki/Unix_time
.. _tracking issue #11491: https://github.com/crate/crate/issues/11491
.. _tracking issue #11490: https://github.com/crate/crate/issues/11490
.. _tracking issue #11528: https://github.com/crate/crate/issues/11528
.. _The PostgreSQL DATE type: https://www.postgresql.org/docs/current/datatype-datetime.html
.. _Fully Qualified Domain Name: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
.. _IP adddress: https://en.wikipedia.org/wiki/IP_address
