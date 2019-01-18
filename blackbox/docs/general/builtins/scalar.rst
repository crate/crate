.. highlight:: psql
.. _scalar:

================
Scalar Functions
================

Scalar functions return a single value (not a table).

.. rubric:: Table of Contents

.. contents::
   :local:

String Functions
================

``concat('first_arg', second_arg, [ parameter , ... ])``
--------------------------------------------------------

Concatenates a variable number of arguments into a single string. It ignores
``NULL`` values.

Returns: ``string``

::

    cr> select concat('foo', null, 'bar');
    +----------------------------+
    | concat('foo', NULL, 'bar') |
    +----------------------------+
    | foobar                     |
    +----------------------------+
    SELECT 1 row in set (... sec)

You can also use the ``||`` operator::

    cr> select 'foo' || 'bar';
    +----------------------+
    | concat('foo', 'bar') |
    +----------------------+
    | foobar               |
    +----------------------+
    SELECT 1 row in set (... sec)

``format('format_string', parameter, [ parameter , ... ])``
-----------------------------------------------------------

Formats a string similar to the C function ``printf``. For details about the
format string syntax, see `formatter`_

Returns: ``string``

::

    cr> select format('%s.%s', schema_name, table_name) from sys.shards
    ... where table_name = 'locations'
    ... limit 1;
    +------------------------------------------+
    | format('%s.%s', schema_name, table_name) |
    +------------------------------------------+
    | doc.locations                            |
    +------------------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select format('%tY', date) from locations
    ... group by format('%tY', date)
    ... order by 1;
    +---------------------+
    | format('%tY', date) |
    +---------------------+
    |                1979 |
    |                2013 |
    +---------------------+
    SELECT 2 rows in set (... sec)

``substr('string', from, [ count ])``
-------------------------------------

Extracts a part of a string. ``from`` specifies where to start and ``count``
the length of the part.

Returns: ``string``

::

    cr> select substr('crate.io', 3, 2);
    +--------------------------+
    | substr('crate.io', 3, 2) |
    +--------------------------+
    | at                       |
    +--------------------------+
    SELECT 1 row in set (... sec)

.. _scalar_char_length:

``char_length('string')``
-------------------------

Counts the number of characters in a string.

Returns: ``integer``

::

    cr> select char_length('crate.io');
    +-------------------------+
    | char_length('crate.io') |
    +-------------------------+
    |                       8 |
    +-------------------------+
    SELECT 1 row in set (... sec)

Each character counts only once, regardless of its byte size.

::

    cr> select char_length('©rate.io');
    +-------------------------+
    | char_length('©rate.io') |
    +-------------------------+
    |                       8 |
    +-------------------------+
    SELECT 1 row in set (... sec)

.. _scalar_bit_length:

``bit_length('string')``
------------------------

Counts the number of bits in a string.

Returns: ``integer``

.. NOTE::

    CrateDB uses UTF-8 encoding internally, which uses between 1 and 4 bytes
    per character.

::

    cr> select bit_length('crate.io');
    +------------------------+
    | bit_length('crate.io') |
    +------------------------+
    |                     64 |
    +------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select bit_length('©rate.io');
    +------------------------+
    | bit_length('©rate.io') |
    +------------------------+
    |                     72 |
    +------------------------+
    SELECT 1 row in set (... sec)

.. _scalar_octet_length:

``octet_length('string')``
--------------------------

Counts the number of bytes (octets) in a string.

Returns: ``integer``

::

    cr> select octet_length('crate.io');
    +--------------------------+
    | octet_length('crate.io') |
    +--------------------------+
    |                        8 |
    +--------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select octet_length('©rate.io');
    +--------------------------+
    | octet_length('©rate.io') |
    +--------------------------+
    |                        9 |
    +--------------------------+
    SELECT 1 row in set (... sec)

``lower('string')``
-------------------

Converts all characters to lowercase. ``lower`` does not perform
locale-sensitive or context-sensitive mappings.

Returns: ``string``

::

    cr> select lower('TransformMe');
    +----------------------+
    | lower('TransformMe') |
    +----------------------+
    | transformme          |
    +----------------------+
    SELECT 1 row in set (... sec)

``upper('string')``
-------------------

Converts all characters to uppercase. ``upper`` does not perform
locale-sensitive or context-sensitive mappings.

Returns: ``string``

::

    cr> select upper('TransformMe');
    +----------------------+
    | upper('TransformMe') |
    +----------------------+
    | TRANSFORMME          |
    +----------------------+
    SELECT 1 row in set (... sec)

``initcap('string')``
---------------------

Converts the first letter of each word to upper case and the rest to lower case
(*capitalize letters*).

Returns: ``string``

::

   cr> select initcap('heLlo WORLD');
    +------------------------+
    | initcap('heLlo WORLD') |
    +------------------------+
    | Hello World            |
    +------------------------+
    SELECT 1 row in set (... sec)

.. _sha1:

``sha1('string')``
------------------

Returns: ``string``

Computes the SHA1 checksum of the given string.

::

    cr> select sha1('foo');
    +------------------------------------------+
    | sha1('foo')                              |
    +------------------------------------------+
    | 0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33 |
    +------------------------------------------+
    SELECT 1 row in set (... sec)

``md5('string')``
-----------------

Returns: ``string``

Computes the MD5 checksum of the given string.

See :ref:`sha1 <sha1>` for an example.

``replace(text, from, to)``
---------------------------

Replaces all occurrences of ``from`` in ``text`` with ``to``.

::

   cr> select replace('Hello World', 'World', 'Stranger');
   +---------------------------------------------+
   | replace('Hello World', 'World', 'Stranger') |
   +---------------------------------------------+
   | Hello Stranger                              |
   +---------------------------------------------+
   SELECT 1 row in set (... sec)


Date and Time Functions
=======================

.. _scalar-date-trunc:

``date_trunc('interval', ['timezone',] timestamp)``
---------------------------------------------------

Returns: ``timestamp``

Limits a timestamps precision to a given interval.

Valid intervals are:

* ``second``

* ``minute``

* ``hour``

* ``day``

* ``week``

* ``month``

* ``quarter``

* ``year``

Valid values for ``timezone`` are either the name of a time zone (for example
'Europe/Vienna') or the UTC offset of a time zone (for example '+01:00'). To
get a complete overview of all possible values take a look at the `available
time zones`_ supported by `Joda-Time`_.

The following example shows how to use the date_trunc function to generate a
day based histogram in the ``Europe/Moscow`` timezone::

    cr> select
    ... date_trunc('day', 'Europe/Moscow', date) as day,
    ... count(*) as num_locations
    ... from locations
    ... group by date_trunc('day', 'Europe/Moscow', date)
    ... order by date_trunc('day', 'Europe/Moscow', date);
    +---------------+---------------+
    | day           | num_locations |
    +---------------+---------------+
    | 308523600000  | 4             |
    | 1367352000000 | 1             |
    | 1373918400000 | 8             |
    +---------------+---------------+
    SELECT 3 rows in set (... sec)

If you don't specify a time zone, ``truncate`` uses UTC time::

    cr> select date_trunc('day', date) as day, count(*) as num_locations
    ... from locations
    ... group by date_trunc('day', date)
    ... order by date_trunc('day', date);
    +---------------+---------------+
    | day           | num_locations |
    +---------------+---------------+
    | 308534400000  | 4             |
    | 1367366400000 | 1             |
    | 1373932800000 | 8             |
    +---------------+---------------+
    SELECT 3 rows in set (... sec)

``extract(field from source)``
------------------------------

``extract`` is a special expression that translates to a function which
retrieves subfields such as day, hour or minute from a timestamp.

The return type depends on the used ``field``.

Synopsis
........

::

    EXTRACT( field FROM expression )

:field:
  An identifier or string literal which identifies the part of the timestamp
  that should be extracted.

:expression:
  An expression that resolves to a timestamp or is castable to timestamp.

::

    cr> select extract(day from '2014-08-23');
    +--------------------------------+
    | EXTRACT(DAY FROM '2014-08-23') |
    +--------------------------------+
    |                             23 |
    +--------------------------------+
    SELECT 1 row in set (... sec)

``source`` must be an expression that returns a timestamp. In case the
expression has a different return type but is known to be castable to timestamp
an implicit cast will be attempted.

``field`` is an identifier that selects which part of the timestamp to extract.
The following fields are supported:

``CENTURY``
  | *Return type:* ``integer``
  | century of era

  Returns the ISO representation which is a straight split of the date.

  Year 2000 century 20 and year 2001 is also century 20. This is different to
  the GregorianJulian (GJ) calendar system where 2001 would be century 21.

``YEAR``
  | *Return type:* ``integer``
  | the year field

``QUARTER``
  | *Return type:* ``integer``
  | the quarter of the year (1 - 4)

``MONTH``
  | *Return type:* ``integer``
  | the month of the year

``WEEK``
  | *Return type:* ``integer``
  | the week of the year

``DAY``
  | *Return type:* ``integer``
  | the day of the month

``DAY_OF_MONTH``
  | *Return type:* ``integer``
  | same as ``day``

``DAY_OF_WEEK``
  | *Return type:* ``integer``
  | day of the week. Starting with Monday (1) to Sunday (7)

``DOW``
  | *Return type:* ``integer``
  | same as ``day_of_week``

``DAY_OF_YEAR``
  | *Return type:* ``integer``
  | the day of the year (1 - 365 / 366)

``DOY``
  | *Return type:* ``integer``
  | same as ``day_of_year``

``HOUR``
  | *Return type:* ``integer``
  | the hour field

``MINUTE``
  | *Return type:* ``integer``
  | the minute field

``SECOND``
  | *Return type:* ``integer``
  | the second field

``EPOCH``
  | *Return type:* ``double``
  | The number of seconds since Jan 1, 1970.
  | Can be negative if earlier than Jan 1, 1970.

.. _`available time zones`: http://www.joda.org/joda-time/timezones.html
.. _`Joda-Time`: http://www.joda.org/joda-time/


.. _current_timestamp:

``CURRENT_TIMESTAMP``
---------------------

The ``CURRENT_TIMESTAMP`` expression returns the timestamp in milliseconds
since epoch at the time the SQL statement was handled. Therefore, the same
timestamp value is returned for every invocation of a single statement.

.. NOTE::

    If the ``CURRENT_TIMESTAMP`` function is used in
    :ref:`sql-ddl-generated-columns` it behaves slightly different in
    ``UPDATE`` operations. In such a case the actual timestamp of each row
    update is returned.

synopsis::

    CURRENT_TIMESTAMP [ ( precision ) ]

``precision`` must be a positive integer between 0 and 3. The default value is
3. It determines the number of fractional seconds to output. A value of 0 means
the timestamp will have second precision, no fractional seconds (milliseconds)
are given.

.. NOTE::

   The ``CURRENT_TIMESTAMP`` will be evaluated  using javas
   ``System.currentTimeMillis()``. So its actual result depends on the
   underlying operating system.

``date_format([format_string, [timezone,]] timestamp)``
-------------------------------------------------------

The ``date_format`` function formats a timestamp as string according to the
(optional) format string.

Returns: ``string``

Synopsis
........

::

    DATE_FORMAT( [ format_string, [ timezone, ] ] timestamp )

The only mandatory argument is the ``timestamp`` value to format. It can be any
expression that is safely convertible to timestamp.

Format
......

The syntax for the ``format_string`` is 100% compatible to the syntax of the
`MySQL date_format`_ function. For reference, the format is listed in detail
below [#MySQL-Docs]_:

.. csv-table:: date_format Format
   :header: "Format Specifier", "Description"

   ``%a``,	"Abbreviated weekday name (Sun..Sat)"
   ``%b``,	"Abbreviated month name (Jan..Dec)"
   ``%c``,	"Month in year, numeric (0..12)"
   ``%D``,	"Day of month as ordinal number (1st, 2nd, ... 24th)"
   ``%d``,	"Day of month, padded to 2 digits (00..31)"
   ``%e``,	"Day of month (0..31)"
   ``%f``,	"Microseconds, padded to 6 digits (000000..999999)"
   ``%H``,	"Hour in 24-hour clock, padded to 2 digits (00..23)"
   ``%h``,	"Hour in 12-hour clock, padded to 2 digits (01..12)"
   ``%I``,	"Hour in 12-hour clock, padded to 2 digits (01..12)"
   ``%i``,	"Minutes, numeric (00..59)"
   ``%j``,	"Day of year, padded to 3 digits (001..366)"
   ``%k``,	"Hour in 24-hour clock (0..23)"
   ``%l``,	"Hour in 12-hour clock (1..12)"
   ``%M``,	"Month name (January..December)"
   ``%m``,	"Month in year, numeric, padded to 2 digits (00..12)"
   ``%p``,	"AM or PM"
   ``%r``,	"Time, 12-hour (hh:mm:ss followed by AM or PM)"
   ``%S``,	"Seconds, padded to 2 digits (00..59)"
   ``%s``,	"Seconds, padded to 2 digits (00..59)"
   ``%T``,	"Time, 24-hour (hh:mm:ss)"
   ``%U``,	"Week number, sunday as first day of the week, first week of the year (01) is the one starting in this year, week 00 starts in last year (00..53)"
   ``%u``,	"Week number, monday as first day of the week, first week of the year (01) is the one with at least 4 days in this year (00..53)"
   ``%V``,	"Week number, sunday as first day of the week, first week of the year (01) is the one starting in this year, uses the week number of the last year, if the week started in last year (01..53)"
   ``%v``,	"Week number, monday as first day of the week, first week of the year (01) is the one with at least 4 days in this year, uses the week number of the last year, if the week started in last year (01..53)"
   ``%W``,	"Weekday name (Sunday..Saturday)"
   ``%w``,	"Day of the week (0=Sunday..6=Saturday)"
   ``%X``,	"weekyear, sunday as first day of the week, numeric, four digits; used with %V"
   ``%x``,	"weekyear, monday as first day of the week, numeric, four digits; used with %v"
   ``%Y``,	"Year, numeric, four digits"
   ``%y``,	"Year, numeric, two digits"
   ``%%``,	"A literal '%' character"
   ``%x``,	"x, for any 'x' not listed above"

If no ``format_string`` is given the default format will be used::

    %Y-%m-%dT%H:%i:%s.%fZ

::

    cr> select date_format('1970-01-01') as epoque;
    +-----------------------------+
    | epoque                      |
    +-----------------------------+
    | 1970-01-01T00:00:00.000000Z |
    +-----------------------------+
    SELECT 1 row in set (... sec)

Timezone
........

Valid values for ``timezone`` are either the name of a time zone (for example
'Europe/Vienna') or the UTC offset of a time zone (for example '+01:00'). To
get a complete overview of all possible values take a look at the `available
time zones`_ supported by `Joda-Time`_.

The ``timezone`` will be ``UTC`` if not provided::

    cr> select date_format('%W the %D of %M %Y %H:%i %p', 0) as epoque;
    +-------------------------------------------+
    | epoque                                    |
    +-------------------------------------------+
    | Thursday the 1st of January 1970 00:00 AM |
    +-------------------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select date_format('%Y/%m/%d %H:%i', 'EST',  0) as est_epoque;
    +------------------+
    | est_epoque       |
    +------------------+
    | 1969/12/31 19:00 |
    +------------------+
    SELECT 1 row in set (... sec)

Geo Functions
=============

.. _scalar_distance:

``distance(geo_point1, geo_point2)``
------------------------------------

Returns: ``double``

The ``distance`` function can be used to calculate the distance between two
points on earth. It uses the `Haversine formula`_ which gives great-circle
distances between 2 points on a sphere based on their latitude and longitude.

The return value is the distance in meters.

Below is an example of the distance function where both points are specified
using WKT. See :ref:`geo_point_data_type` for more information on the implicit
type casting of geo points::

    cr> select distance('POINT (10 20)', 'POINT (11 21)');
    +--------------------------------------------+
    | distance('POINT (10 20)', 'POINT (11 21)') |
    +--------------------------------------------+
    |                          152354.3209044634 |
    +--------------------------------------------+
    SELECT 1 row in set (... sec)

This scalar function can always be used in both the ``WHERE`` and ``ORDER BY``
clauses. With the limitation that one of the arguments must be a literal and
the other argument must be a column reference.

.. NOTE::

   The algorithm of the calculation which is used when the distance
   function is used as part of the result column list has a different
   precision than what is stored inside the index which is utilized if
   the distance function is part of a WHERE clause.

   For example if ``select distance(...)`` returns 0.0 an equality check
   with ``where distance(...) = 0`` might not yield anything at all due
   to the precision difference.

.. _scalar_within:

``within(shape1, shape2)``
--------------------------

Returns: ``boolean``

The ``within`` function returns true if ``shape1`` is within ``shape2``. If
that is not the case false is returned.

``shape1`` can either be a ``geo_shape`` or a ``geo_point``. ``shape2`` must be
a ``geo_shape``.

Below is an example of the within function which makes use of the implicit type
casting from strings to geo point and geo shapes::

    cr> select within(
    ...   'POINT (10 10)',
    ...   'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'
    ... );
    +--------------------------------------------------------------------+
    | within('POINT (10 10)', 'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))') |
    +--------------------------------------------------------------------+
    | TRUE                                                               |
    +--------------------------------------------------------------------+
    SELECT 1 row in set (... sec)

This function can always be used within the ``WHERE`` clause.

.. _scalar_intersects:

``intersects(geo_shape, geo_shape)``
------------------------------------

Returns: ``boolean``

The ``intersects`` function returns true if both argument shapes share some
points or area, they *overlap*. This also includes two shapes where one lies
:ref:`within <scalar_within>` the other.

If ``false`` is returned, both shapes are considered *disjoint*.

Example::

    cr> select
    ... intersects(
    ...   {type='Polygon', coordinates=[
    ...         [[13.4252, 52.7096],[13.9416, 52.0997],
    ...          [12.7221, 52.1334],[13.4252, 52.7096]]]},
    ...   'LINESTRING(13.9636 52.6763, 13.2275 51.9578,
    ...               12.9199 52.5830, 11.9970 52.6830)'
    ... ) as intersects,
    ... intersects(
    ...   {type='Polygon', coordinates=[
    ...         [[13.4252, 52.7096],[13.9416, 52.0997],
    ...          [12.7221, 52.1334],[13.4252, 52.7096]]]},
    ...   'LINESTRING (11.0742 49.4538, 11.5686 48.1367)'
    ... ) as disjoint;
    +------------+----------+
    | intersects | disjoint |
    +------------+----------+
    | TRUE       | FALSE    |
    +------------+----------+
    SELECT 1 row in set (... sec)

Due to a limitation on the :ref:`geo_shape_data_type` datatype this function
cannot be used in the :ref:`sql_reference_order_by`.

``latitude(geo_point)`` and ``longitude(geo_point)``
----------------------------------------------------

Returns: ``double``

The ``latitude`` and ``longitude`` function return the coordinates of latitude
or longitude of a point, or ``NULL`` if not available. The input must be a
column of type ``geo_point``, a valid WKT string or a double-array. See
:ref:`geo_point_data_type` for more information on the implicit type casting of
geo points.

Example::

    cr> select mountain, height, longitude(coordinates) as "lon", latitude(coordinates) as "lat"
    ... from sys.summits order by height desc limit 1;
    +------------+--------+---------+---------+
    | mountain   | height |     lon |     lat |
    +------------+--------+---------+---------+
    | Mont Blanc |   4808 | 6.86444 | 45.8325 |
    +------------+--------+---------+---------+
    SELECT 1 row in set (... sec)

Below is an example of the latitude/longitude functions which make use of the
implicit type casting from strings to geo point::

    cr> select latitude('POINT (10 20)'), longitude([10.0, 20.0]);
    +---------------------------+-------------------------+
    | latitude('POINT (10 20)') | longitude([10.0, 20.0]) |
    +---------------------------+-------------------------+
    |                      20.0 |                    10.0 |
    +---------------------------+-------------------------+
    SELECT 1 row in set (... sec)

``geohash(geo_point)``
----------------------

Returns: ``string``

Returns a `GeoHash <http://en.wikipedia.org/wiki/Geohash>`_ representation
based on full precision (12 characters) of the input point, or ``NULL`` if not
available. The input has to be a column of type ``geo_point``, a valid WKT
string or a double-array.See :ref:`geo_point_data_type` for more information of
the implicit type casting of geo points.

Example::

    cr> select mountain, height, geohash(coordinates) as "geohash" from sys.summits
    ... order by height desc limit 1;
    +------------+--------+--------------+
    | mountain   | height | geohash      |
    +------------+--------+--------------+
    | Mont Blanc |   4808 | u0huspw99j1r |
    +------------+--------+--------------+
    SELECT 1 row in set (... sec)

.. _mathematical_functions:

Mathematical Functions
======================

All mathematical functions can be used within ``WHERE`` and ``ORDER BY``
clauses.

``abs(number)``
---------------

Returns the absolute value of the given number in the datatype of the given
number::

    cr> select abs(214748.0998), abs(0), abs(-214748);
    +------------------+--------+---------------+
    | abs(214748.0998) | abs(0) | abs(- 214748) |
    +------------------+--------+---------------+
    |      214748.0998 |      0 |        214748 |
    +------------------+--------+---------------+
    SELECT 1 row in set (... sec)

.. _scalar-ceil:

``ceil(number)``
----------------

Returns the smallest integer or long value that is not less than the argument.

Returns: ``long`` or ``integer``

Return value will be of type integer if the input value is an integer or float.
If the input value is of type long or double the return value will be of type
long::

    cr> select ceil(29.9);
    +------------+
    | ceil(29.9) |
    +------------+
    |         30 |
    +------------+
    SELECT 1 row in set (... sec)

.. _scalar-floor:

``floor(number)``
-----------------

Returns the largest integer or long value that is not greater than the
argument.

Returns: ``long`` or ``integer``

Return value will be an integer if the input value is an integer or a float. If
the input value is of type long or double the return value will be of type
long.

See below for an example::

    cr> select floor(29.9);
    +-------------+
    | floor(29.9) |
    +-------------+
    |          29 |
    +-------------+
    SELECT 1 row in set (... sec)

``ln(number)``
--------------

Returns the natural logarithm of given ``number``.

Returns: ``double``

See below for an example::

    cr> SELECT ln(1);
    +-------+
    | ln(1) |
    +-------+
    |   0.0 |
    +-------+
    SELECT 1 row in set (... sec)

.. NOTE::

    An error is returned for arguments which lead to undefined or illegal
    results. E.g. ln(0) results in ``minus infinity``, and therefore, an error
    is returned.

``log(x : number, b : number)``
-------------------------------

Returns the logarithm of given ``x`` to base ``b``.

Returns: ``double``

See below for an example, which essentially is the same as above::

    cr> SELECT log(100, 10);
    +--------------+
    | log(100, 10) |
    +--------------+
    |          2.0 |
    +--------------+
    SELECT 1 row in set (... sec)

The second argument (``b``) is optional. If not present, base 10 is used::

    cr> SELECT log(100);
    +----------+
    | log(100) |
    +----------+
    |      2.0 |
    +----------+
    SELECT 1 row in set (... sec)

.. NOTE::

    An error is returned for arguments which lead to undefined or illegal
    results. E.g. log(0) results in ``minus infinity``, and therefore, an error
    is returned.

    The same is true for arguments which lead to a ``division by zero``, as
    e.g. log(10, 1) does.

``power(a: number, b: number)``
-------------------------------

Returns the given argument ``a`` raised to the power of argument ``b``.

Returns: ``double``

The return type of the power function is always double, even when both the
inputs are integral types, in order to be consistent across positive and
negative exponents (which will yield decimal types)

See below for an example::

    cr> SELECT power(2,3);
    +-------------+
    | power(2, 3) |
    +-------------+
    |         8.0 |
    +-------------+
    SELECT 1 row in set (... sec)

``random()``
------------

The ``random`` function returns a random value in the range 0.0 <= X < 1.0.

Returns: ``double``

.. NOTE::

    Every call to ``random`` will yield a new random number.

.. _scalar-round:

``round(number)``
-----------------

If the input is of type double or long the result is the closest long to the
argument, with ties rounding up.

If the input is of type float or integer the result is the closest integer to
the argument, with ties rounding up.

Returns: ``long`` or ``integer``

See below for an example::

    cr> select round(42.2);
    +-------------+
    | round(42.2) |
    +-------------+
    |          42 |
    +-------------+
    SELECT 1 row in set (... sec)

``sqrt(number)``
----------------

Returns the square root of the argument.

Returns: ``double``

See below for an example::

    cr> select sqrt(25.0);
    +------------+
    | sqrt(25.0) |
    +------------+
    |        5.0 |
    +------------+
    SELECT 1 row in set (... sec)

``sin(number)``
---------------

Returns the sine of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT sin(1);
    +--------------------+
    |             sin(1) |
    +--------------------+
    | 0.8414709848078965 |
    +--------------------+
    SELECT 1 row in set (... sec)

``asin(number)``
----------------

Returns the arcsine of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT asin(1);
    +--------------------+
    |            asin(1) |
    +--------------------+
    | 1.5707963267948966 |
    +--------------------+
    SELECT 1 row in set (... sec)

``cos(number)``
---------------

Returns the cosine of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT cos(1);
    +--------------------+
    |             cos(1) |
    +--------------------+
    | 0.5403023058681398 |
    +--------------------+
    SELECT 1 row in set (... sec)

``acos(number)``
----------------

Returns the arccosine of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT acos(-1);
    +-------------------+
    |         acos(- 1) |
    +-------------------+
    | 3.141592653589793 |
    +-------------------+
    SELECT 1 row in set (... sec)

``tan(number)``
---------------

Returns the tangent of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT tan(1);
    +--------------------+
    |             tan(1) |
    +--------------------+
    | 1.5574077246549023 |
    +--------------------+
    SELECT 1 row in set (... sec)

``atan(number)``
----------------

Returns the arctangent of the argument.

Returns: ``double``

See below for an example::

    cr> SELECT atan(1);
    +--------------------+
    |            atan(1) |
    +--------------------+
    | 0.7853981633974483 |
    +--------------------+
    SELECT 1 row in set (... sec)

.. _scalar-regexp:

Regular Expression Functions
============================

The regular expression functions in CrateDB use `Java Regular Expressions`_.

See the api documentation for more details.

.. NOTE::

   Be aware that, in contrast to the functions, the :ref:`regular expression
   operator <sql_ddl_regexp>` is using `Lucene Regular Expressions`_.

.. _Lucene Regular Expressions: http://lucene.apache.org/core/4_9_0/core/org/apache/lucene/util/automaton/RegExp.html

.. _scalar-regexp-matches:

``regexp_matches(source, pattern [, flags])``
---------------------------------------------

This function uses the regular expression pattern in ``pattern`` to match
against the ``source`` string.

Returns: ``string_array``

If ``source`` matches, an array of the matched regular expression groups is
returned.

If no regular expression group was used, the whole pattern is used as a group.

If ``source`` does not match, this function returns ``NULL``.

A regular expression group is formed by a subexpression that is surrounded by
parentheses.The position of a group is determined by the position of its
opening parenthesis.

For example when matching the pattern ``\b([A-Z])`` a match for the
subexpression ``([A-Z])`` would create group No. 1. If you want to group stuff
with parentheses, but without grouping, use ``(?...)``.

For example matching the regular expression ``([Aa](.+)z)`` against
``alcatraz``, results in these groups:

 * group 1: ``alcatraz`` (from first to last parenthesis or whole pattern)
 * group 2: ``lcatra`` (beginning at second parenthesis)

The ``regexp_matches`` function will return all groups as a string array::

    cr> select regexp_matches('alcatraz', '(a(.+)z)') as matched;
    +------------------------+
    | matched                |
    +------------------------+
    | ["alcatraz", "lcatra"] |
    +------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select regexp_matches('alcatraz', 'traz') as matched;
    +----------+
    | matched  |
    +----------+
    | ["traz"] |
    +----------+
    SELECT 1 row in set (... sec)

Through array element access functionality, a group can be selected directly.
See :ref:`sql_dql_object_arrays_select` for details.

::

    cr> select regexp_matches('alcatraz', '(a(.+)z)')[2] as second_group;
    +--------------+
    | second_group |
    +--------------+
    | lcatra       |
    +--------------+
    SELECT 1 row in set (... sec)

.. _scalar-regexp-matches-flags:

Flags
.....

This function takes a number of flags as optional third parameter. These flags
are given as a string containing any of the characters listed below. Order does
not matter.

+-------+---------------------------------------------------------------------+
| Flag  | Description                                                         |
+=======+=====================================================================+
| ``i`` | enable case insensitive matching                                    |
+-------+---------------------------------------------------------------------+
| ``u`` | enable unicode case folding when used together with ``i``           |
+-------+---------------------------------------------------------------------+
| ``U`` | enable unicode support for character classes like ``\W``            |
+-------+---------------------------------------------------------------------+
| ``s`` | make ``.`` match line terminators, too                              |
+-------+---------------------------------------------------------------------+
| ``m`` | make ``^`` and ``$`` match on the beginning or end of a line        |
|       | too.                                                                |
+-------+---------------------------------------------------------------------+
| ``x`` | permit whitespace and line comments starting with ``#``             |
+-------+---------------------------------------------------------------------+
| ``d`` | only ``\n`` is considered a line-terminator when using ``^``, ``$`` |
|       | and ``.``                                                           |
+-------+---------------------------------------------------------------------+

Examples
........

::

    cr> select regexp_matches('foobar', '^(a(.+)z)$') as matched;
    +---------+
    | matched |
    +---------+
    | NULL    |
    +---------+
    SELECT 1 row in set (... sec)

::

    cr> select regexp_matches('99 bottles of beer on the wall', '\d{2}\s(\w+).*', 'ixU')
    ... as matched;
    +-------------+
    | matched     |
    +-------------+
    | ["bottles"] |
    +-------------+
    SELECT 1 row in set (... sec)

``regexp_replace(source, pattern, replacement [, flags])``
----------------------------------------------------------

``regexp_replace`` can be used to replace every (or only the first) occurence
of a subsequence matching ``pattern`` in the ``source`` string with the
``replacement`` string. If no subsequence in ``source`` matches the regular
expression ``pattern``, ``source`` is returned unchanged.

Returns: ``string``

``pattern`` is a java regular expression. For details on the regexp syntax, see
`Java Regular Expressions`_.

The ``replacement`` string may contain expressions like ``$N`` where ``N`` is a
digit between 0 and 9. It references the *N*\ th matched group of ``pattern``
and the matching subsequence of that group will be inserted in the returned
string. The expression ``$0`` will insert the whole matching ``source``.

Per default, only the first occurrence of a subsequence matching ``pattern``
will be replaced. If all occurrences shall be replaced use the ``g`` flag.

Flags
.....

``regexp_replace`` supports the same flags than ``regexp_matches``, see
:ref:`regexp_matches Flags <scalar-regexp-matches-flags>` and additionally the
``g`` flag:

+-------+---------------------------------------------------------------------+
| Flag  | Description                                                         |
+=======+=====================================================================+
| ``g`` | replace all occurrences of a subsequence matching ``pattern``,      |
|       | not only the first                                                  |
+-------+---------------------------------------------------------------------+

Examples
........

::

   cr> select name, regexp_replace(name, '(\w+)\s(\w+)+', '$1 - $2') as replaced from locations
   ... order by name limit 5;
    +---------------------+-----------------------+
    | name                | replaced              |
    +---------------------+-----------------------+
    |                     |                       |
    | Aldebaran           | Aldebaran             |
    | Algol               | Algol                 |
    | Allosimanius Syneca | Allosimanius - Syneca |
    | Alpha Centauri      | Alpha - Centauri      |
    +---------------------+-----------------------+
    SELECT 5 rows in set (... sec)

::

   cr> select regexp_replace('alcatraz', '(foo)(bar)+', '$1baz') as replaced;
    +----------+
    | replaced |
    +----------+
    | alcatraz |
    +----------+
    SELECT 1 row in set (... sec)

::

   cr> select name, regexp_replace(name, '([A-Z]\w+) .+', '$1', 'ig') as replaced from locations
   ... order by name limit 5;
    +---------------------+--------------+
    | name                | replaced     |
    +---------------------+--------------+
    |                     |              |
    | Aldebaran           | Aldebaran    |
    | Algol               | Algol        |
    | Allosimanius Syneca | Allosimanius |
    | Alpha Centauri      | Alpha        |
    +---------------------+--------------+
    SELECT 5 rows in set (... sec)

Array Functions
===============

``array_cat(first_array, second_array)``
----------------------------------------

The ``array_cat`` function concatenates two arrays into one array

Returns: ``array``

::

    cr> select array_cat([1,2,3],[3,4,5,6]);
    +------------------------------------+
    | array_cat([1, 2, 3], [3, 4, 5, 6]) |
    +------------------------------------+
    | [1, 2, 3, 3, 4, 5, 6]              |
    +------------------------------------+
    SELECT 1 row in set (... sec)

It can be used to append elements to array fields

::

    cr> create table array_cat_example (list array(integer));
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into array_cat_example (list) values ([1,2,3]);
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh array_cat_example

    cr> refresh table array_cat_example
    REFRESH OK, 1 row affected (... sec)

::

    cr> update array_cat_example set list = array_cat(list, [4, 5, 6]);
    UPDATE OK, 1 row affected (... sec)

.. Hidden: refresh array_cat_example

    cr> refresh table array_cat_example
    REFRESH OK, 1 row affected (... sec)

::

    cr> select * from array_cat_example;
    +--------------------+
    | list               |
    +--------------------+
    | [1, 2, 3, 4, 5, 6] |
    +--------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

   Appending to arrays with array_cat in updates is handy, but unfortunately
   not isolated. We use optimistic concurrency control to ensure that your
   update operation used the latest state of the row. But only 3 retry attempts
   are made by fetching the newest version again and if they all fail, the
   query fails.

You can also use the concat operator ``||`` with arrays

::

    cr> select [1,2,3] || [4,5,6] || [7,8,9];
    +-------------------------------------------------+
    | concat(concat([1, 2, 3], [4, 5, 6]), [7, 8, 9]) |
    +-------------------------------------------------+
    | [1, 2, 3, 4, 5, 6, 7, 8, 9]                     |
    +-------------------------------------------------+
    SELECT 1 row in set (... sec)

``array_unique(first_array, [ second_array])``
----------------------------------------------

The ``array_unique`` function merges two arrays into one array with unique
elements

Returns: ``array``

::

    cr> select array_unique([1, 2, 3], [3, 4, 4]);
    +------------------------------------+
    | array_unique([1, 2, 3], [3, 4, 4]) |
    +------------------------------------+
    | [1, 2, 3, 4]                       |
    +------------------------------------+
    SELECT 1 row in set (... sec)

If the arrays have different types all elements will be cast to the element
type of the first array with a defined type::

    cr> select array_unique([10, 20], [10.2, 20.3]);
    +--------------------------------------+
    | array_unique([10, 20], [10.2, 20.3]) |
    +--------------------------------------+
    | [10, 20]                             |
    +--------------------------------------+
    SELECT 1 row in set (... sec)

``array_difference(first_array, second_array)``
-----------------------------------------------

The ``array_difference`` function removes elements from the first array that
are contained in the second array.

Returns: ``array``

::

    cr> select array_difference([1,2,3,4,5,6,7,8,9,10],[2,3,6,9,15]);
    +---------------------------------------------------------------------+
    | array_difference([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [2, 3, 6, 9, 15]) |
    +---------------------------------------------------------------------+
    | [1, 4, 5, 7, 8, 10]                                                 |
    +---------------------------------------------------------------------+
    SELECT 1 row in set (... sec)

It can be used to remove elements from array fields.

::

    cr> create table array_difference_example (list array(integer));
    CREATE OK, 1 row affected (... sec)

::

    cr> insert into array_difference_example (list) values ([1,2,3,4,5,6,7,8,9,10]);
    INSERT OK, 1 row affected (... sec)

.. Hidden: refresh array_difference_example

    cr> refresh table array_difference_example
    REFRESH OK, 1 row affected (... sec)

::

    cr> update array_difference_example set list = array_difference(list, [6]);
    UPDATE OK, 1 row affected (... sec)

.. Hidden: refresh array_difference_example

    cr> refresh table array_difference_example
    REFRESH OK, 1 row affected (... sec)

::

    cr> select * from array_difference_example;
    +------------------------------+
    | list                         |
    +------------------------------+
    | [1, 2, 3, 4, 5, 7, 8, 9, 10] |
    +------------------------------+
    SELECT 1 row in set (... sec)


``array(subquery)``
-------------------

The ``array(subquery)`` expression is an array constructor function
which operates on the result of the ``subquery``.

Returns: ``array``

.. SEEALSO::

    :ref:`Array construction with subquery <sql_expressions_array_subquery>`


``array_upper(anyarray, dimension)``
------------------------------------
The ``array_upper`` function returns the number of elements in the requested
array dimmension (the upper bound of the dimension).

Returns: ``integer``

::

    cr> select array_upper([[1, 4], [3]], 1);
    +-------------------------------+
    | array_upper([[1, 4], [3]], 1) |
    +-------------------------------+
    | 2                             |
    +-------------------------------+
    SELECT 1 row in set (... sec)


``array_length(anyarray, dimension)``
-------------------------------------

The ``array_length`` function returns the number of elements in the requested
array dimmension.

Returns: ``integer``

::

    cr> select array_length([[1, 4], [3]], 1);
    +--------------------------------+
    | array_length([[1, 4], [3]], 1) |
    +--------------------------------+
    | 2                              |
    +--------------------------------+
    SELECT 1 row in set (... sec)


``array_lower(anyarray, dimension)``
------------------------------------
The ``array_lower`` function returns the lower bound of the requested array
dimension (which is ``1`` if the dimension is valid and has at least one
element).

Returns: ``integer``

::

    cr> select array_lower([[1, 4], [3]], 1);
    +-------------------------------+
    | array_lower([[1, 4], [3]], 1) |
    +-------------------------------+
    | 1                             |
    +-------------------------------+
    SELECT 1 row in set (... sec)


Conditional Functions and Expressions
=====================================

``CASE WHEN ... THEN ... END``
------------------------------

The ``case`` expression is a generic conditional expression similar to if/else
statements in other programming languages and can be used wherever an
expression is valid.

::

  CASE WHEN condition THEN result
       [WHEN ...]
       [ELSE result]
  END

Each *condition* expression must result in a boolean value. If the condition's
result is true, the value of the *result* expression that follows the condition
will be the final result of the ``case`` expression and the subsequent ``when``
branches will not be processed. If the condition's result is not true, any
subsequent ``when`` clauses are examined in the same manner. If no ``when``
condition yields true, the value of the ``case`` expression is the result of
the ``else`` clause. If the ``else`` clause is omitted and no condition is
true, the result is null.

.. Hidden: create table case_example

    cr> create table case_example (id long);
    CREATE OK, 1 row affected (... sec)
    cr> insert into case_example (id) values (0),(1),(2),(3);
    INSERT OK, 4 rows affected (... sec)
    cr> refresh table case_example
    REFRESH OK, 1 row affected (... sec)

Example:
::

    cr> select id,
    ...   case when id = 0 then 'zero'
    ...        when id % 2 = 0 then 'even'
    ...        else 'odd'
    ...   end as parity
    ... from case_example order by id;
    +----+--------+
    | id | parity |
    +----+--------+
    |  0 | zero   |
    |  1 | odd    |
    |  2 | even   |
    |  3 | odd    |
    +----+--------+
    SELECT 4 rows in set (... sec)

As a variant, a ``case`` expression can be written using the *simple* form:

::

  CASE expression
       WHEN value THEN result
       [WHEN ...]
       [ELSE result]
  END

Example:

::

    cr> select id,
    ...   case id when 0 then 'zero'
    ...           when 1 then 'one'
    ...           else 'other'
    ...   end as description
    ... from case_example order by id;
    +----+-------------+
    | id | description |
    +----+-------------+
    |  0 | zero        |
    |  1 | one         |
    |  2 | other       |
    |  3 | other       |
    +----+-------------+
    SELECT 4 rows in set (... sec)

.. NOTE::

   All *result* expressions must be convertible to a single data type.

.. Hidden: drop table case_example

    cr> drop table case_example;
    DROP OK, 1 row affected (... sec)

``if(condition, result [, default])``
-------------------------------------

The ``if`` function is a conditional function comparing to *if* statements of
most other programming languages. If the given *condition* expresion evaluates
to `true`, the *result* expression is evaluated and it's value is returned. If
the *condition* evaluates to `false`, the *result* expression is not evaluated
and the optional given *default* expression is evaluated instead and it's value
will be returned. If the *default* argument is omitted, NULL will be returned
instead.

.. Hidden: create table if_example

    cr> create table if_example (id long);
    CREATE OK, 1 row affected (... sec)
    cr> insert into if_example (id) values (0),(1),(2),(3);
    INSERT OK, 4 rows affected (... sec)
    cr> refresh table if_example
    REFRESH OK, 1 row affected (... sec)

::

   cr> select id, if(id = 0, 'zero', 'other') as description from if_example order by id;
    +----+-------------+
    | id | description |
    +----+-------------+
    |  0 | zero        |
    |  1 | other       |
    |  2 | other       |
    |  3 | other       |
    +----+-------------+
    SELECT 4 rows in set (... sec)

.. Hidden: drop table if_example

    cr> drop table if_example;
    DROP OK, 1 row affected (... sec)

``coalesce('first_arg', second_arg [, ... ])``
----------------------------------------------

The ``coalesce`` function takes one or more arguments of the same type and
returns the first non-null value of these. The result will be NULL only if all
the arguments evaluate to NULL.

Returns: same type as arguments

::

    cr> select coalesce(clustered_by, 'nothing')
    ...   from information_schema.tables
    ...   where table_name='nodes';
    +-----------------------------------+
    | coalesce(clustered_by, 'nothing') |
    +-----------------------------------+
    | nothing                           |
    +-----------------------------------+
    SELECT 1 row in set (... sec)

``greatest('first_arg', second_arg[ , ... ])``
----------------------------------------------

The ``greatest`` function takes one or more arguments of the same type and will
return the largest value of these. NULL values in the arguments list are
ignored. The result will be NULL only if all the arguments evaluate to NULL.

Returns: same type as arguments

::

    cr> select greatest(1, 2);
    +----------------+
    | greatest(1, 2) |
    +----------------+
    | 2              |
    +----------------+
    SELECT 1 row in set (... sec)

``least('first_arg', second_arg[ , ... ])``
-------------------------------------------

The ``least`` function takes one or more arguments of the same type and will
return the smallest value of these. NULL values in the arguments list are
ignored. The result will be NULL only if all the arguments evaluate to NULL.

Returns: same type as arguments

::

    cr> select least(1, 2);
    +-------------+
    | least(1, 2) |
    +-------------+
    | 1           |
    +-------------+
    SELECT 1 row in set (... sec)

``nullif('first_arg', second_arg)``
-----------------------------------

The ``nullif`` function compares two arguments of the same type and, if they
have the same value, returns NULL; otherwise returns the first argument.

Returns: same type as arguments

::

    cr> select nullif(table_schema, 'sys')
    ...   from information_schema.tables
    ...   where table_name='nodes';
    +-----------------------------+
    | nullif(table_schema, 'sys') |
    +-----------------------------+
    | NULL                        |
    +-----------------------------+
    SELECT 1 row in set (... sec)

System Information Functions
============================

``CURRENT_SCHEMA``
------------------

The ``CURRENT_SCHEMA`` system information function returns the name of the
current schema of the session. If no current schema is set, this function will
return the default schema, which is ``doc``.

Returns: ``string``

The default schema can be set when using the `JDBC
<https://crate.io/docs/reference/jdbc/#jdbc-url-format>`_ and `HTTP clients
<https://crate.io/docs/reference/protocols/http.html#default-schema>`_ such as
`CrateDB PDO`_.

.. NOTE::

    The ``CURRENT_SCHEMA`` function has a special SQL syntax, meaning that it
    must be called without trailing parenthesis (``()``). However, CrateDB also
    supports the optional parenthesis.

Synopsis::

    CURRENT_SCHEMA [ ( ) ]

Example::

    cr> SELECT CURRENT_SCHEMA;
    +----------------+
    | current_schema |
    +----------------+
    |            doc |
    +----------------+
    SELECT 1 row in set (... sec)

.. _current_schemas:

``CURRENT_SCHEMAS(boolean)``
----------------------------

The ``CURRENT_SCHEMAS()`` system information function returns the current stored
schemas inside the :ref:`search_path <conf-session-search-path>` session
state, optionally including implicit schemas (e.g. ``pg_catalog``). If no custom
:ref:`search_path <conf-session-search-path>` is set, this function will return
the default :ref:`search_path <conf-session-search-path>` schemas.


Returns: ``array(string)``

Synopsis::

    CURRENT_SCHEMAS ( boolean )

Example::

    cr> SELECT CURRENT_SCHEMAS(true);
    +-----------------------+
    | current_schemas(true) |
    +-----------------------+
    | ["pg_catalog", "doc"] |
    +-----------------------+
    SELECT 1 row in set (... sec)

.. _current_user:

``CURRENT_USER``
----------------

.. NOTE::

   ``CURRENT_USER`` is an
   :ref:`enterprise feature <enterprise_features>`.

The ``CURRENT_USER`` system information function returns the name of the
current connected user or ``crate`` if the user management module is disabled.

Returns: ``string``

Synopsis::

    CURRENT_USER

Example::

    cr> select current_user;
    +--------------+
    | current_user |
    +--------------+
    | crate        |
    +--------------+
    SELECT 1 row in set (... sec)

.. _user:

``USER``
--------

.. NOTE::

   ``USER`` is an
   :ref:`enterprise feature <enterprise_features>`.

Equivalent to `CURRENT_USER`_.

Returns: ``string``

Synopsis::

    USER

Example::

    cr> select user;
    +--------------+
    | current_user |
    +--------------+
    | crate        |
    +--------------+
    SELECT 1 row in set (... sec)

.. _session_user:

``SESSION_USER``
----------------

.. NOTE::

   ``SESSION_USER`` is an
   :ref:`enterprise feature <enterprise_features>`.

The ``SESSION_USER`` system information function returns the name of the
current connected user or ``crate`` if the user management module is disabled.

Returns: ``string``

Synopsis::

    SESSION_USER

Example::

    cr> select session_user;
    +--------------+
    | session_user |
    +--------------+
    | crate        |
    +--------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    CrateDB doesn't currently support the switching of execution context. This
    makes `SESSION_USER`_ functionally equivalent to `CURRENT_USER`_. We
    provide it as it's part of the SQL standard.

    Additionally, the `CURRENT_USER`_, `SESSION_USER`_ and `USER`_ functions
    have a a special SQL syntax, meaning that they must be called without
    trailing parenthesis (``()``).

``pg_backend_pid()``
--------------------

The ``pg_backend_pid()`` system information function is implemented for
enhanced compatibility with PostgreSQL. CrateDB will always return ``-1`` as
there isn't a single process attached to one query. This is different to
PostgreSQL, where this represents the process ID of the server process
attached to the current session.

Returns: ``integer``

Synopsis::

    pg_backend_pid()

Example::

    cr> select pg_backend_pid();
    +------------------+
    | pg_backend_pid() |
    +------------------+
    |               -1 |
    +------------------+
    SELECT 1 row in set (... sec)


``current_database()``
----------------------

The ``current_database`` function returns the name of the current database,
which in CrateDB will always be ``crate``::

    cr> select current_database();
    +--------------------+
    | current_database() |
    +--------------------+
    | crate              |
    +--------------------+
    SELECT 1 row in set (... sec)


Special Functions
=================

.. _ignore3vl:

``ignore3vl(boolean)``
----------------------

The ``ignore3vl`` function operates on a boolean argument and eliminates the
`3-valued logic`_ on the whole tree of operators beneath it. More specifically,
``FALSE`` is evaluated to ``FALSE``, ``TRUE`` to ``TRUE`` and ``NULL`` to
``FALSE``.

Returns: ``boolean``

.. hide:

    cr> CREATE TABLE IF NOT EXISTS doc.t(
    ...     int_array_col array(integer)
    ... );
    CREATE OK, 1 row affected (... sec)

    cr> INSERT INTO doc.t(int_array_col)
    ...   VALUES ([1,2,3, null]);
    INSERT OK, 1 row affected (... sec)

    cr> REFRESH table doc.t;
    REFRESH OK, 1 row affected (... sec)

.. NOTE::

    The main usage of the ``ignore3vl`` function is in the ``WHERE`` clause
    when a ``NOT`` operator is involved. Such filtering, with
    `3-valued logic`_, cannot be translated to an optimized query in the
    internal storage engine, and therefore can result into slow performance.
    E.g.::

      SELECT * FROM t
      WHERE NOT 5 = ANY(t.int_array_col);

    If we can ignore the `3-valued logic`_, we can write the query as::

      SELECT * FROM t
      WHERE NOT IGNORE3VL(5 = ANY(t.int_array_col));

    which will yield better performance (in execution time) than before.

    .. CAUTION::

      If there are NULL values in the `long_array_col`, in the case that
      `5 = ANY(t.long_array_col)` evaluates to ``NULL``, without the
      ``ignore3vl``, it would be evaluated as ``NOT NULL`` => ``NULL``,
      resulting to zero matched rows. With the ``IGNORE3VL`` in place it will
      be evaluated as ``NOT FALSE`` => ``TRUE`` resulting to all rows matching
      the filter. E.g::

        cr> SELECT * FROM t
        ... WHERE NOT 5 = ANY(t.int_array_col);
        +---------------+
        | int_array_col |
        +---------------+
        +---------------+
        SELECT 0 rows in set (... sec)

      ::

        cr> SELECT * FROM t
        ... WHERE NOT IGNORE3VL(5 = ANY(t.int_array_col));
        +-----------------+
        | int_array_col   |
        +-----------------+
        | [1, 2, 3, null] |
        +-----------------+
        SELECT 1 row in set (... sec)

.. hide:

   cr> DROP TABLE IF EXISTS doc.t;
   DROP OK, 1 row affected (... sec)


Synopsis::

    ignore3vl(boolean)

Example::

    cr> SELECT ignore3vl(true) as v1, ignore3vl(false) as v2, ignore3vl(null) as v3;
    +------+-------+-------+
    | v1   | v2    | v3    |
    +------+-------+-------+
    | TRUE | FALSE | FALSE |
    +------+-------+-------+
    SELECT 1 row in set (... sec)

.. rubric:: Footnotes

.. [#MySQL-Docs] http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format

.. _`formatter`: http://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html
.. _Java Regular Expressions: http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
.. _`MySQL date_format`: http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format
.. _`Haversine formula`: https://en.wikipedia.org/wiki/Haversine_formula
.. _`CrateDB PDO`: https://crate.io/docs/reference/pdo/usage.html#dsn
.. _`3-valued logic`: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
