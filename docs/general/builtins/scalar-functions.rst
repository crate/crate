.. highlight:: psql

.. _scalar-functions:
.. _builtins-scalar:

================
Scalar functions
================

Scalar functions are :ref:`functions <gloss-function>` that return
:ref:`scalars <gloss-scalar>`.

.. rubric:: Table of contents

.. contents::
   :local:


.. _scalar-string:

String functions
================


.. _scalar-concat:

``concat('first_arg', second_arg, [ parameter , ... ])``
--------------------------------------------------------

Concatenates a variable number of arguments into a single string. It ignores
``NULL`` values.

Returns: ``text``

::

    cr> select concat('foo', null, 'bar') AS col;
    +--------+
    | col    |
    +--------+
    | foobar |
    +--------+
    SELECT 1 row in set (... sec)

You can also use the ``||`` :ref:`operator <gloss-operator>`::

    cr> select 'foo' || 'bar' AS col;
    +--------+
    | col    |
    +--------+
    | foobar |
    +--------+
    SELECT 1 row in set (... sec)

.. TIP::

    The ``concat`` function can also be used for merging objects:
    :ref:`concat(object, object) <scalar-concat-object>`


.. _scalar-concat-ws:

``concat_ws('separator', second_arg, [ parameter , ... ])``
------------------------------------------------------------------------------

Concatenates a variable number of arguments into a single string using a
separator defined by the first argument. If first argument is ``NULL`` the
return value is ``NULL``. Remaining ``NULL`` arguments are ignored.

Returns: ``text``

::

    cr> select concat_ws(',','foo', null, 'bar') AS col;
    +---------+
    | col     |
    +---------+
    | foo,bar |
    +---------+
    SELECT 1 row in set (... sec)


.. _scalar-format:

``format('format_string', parameter, [ parameter , ... ])``
-----------------------------------------------------------

Formats a string similar to the C function ``printf``. For details about the
format string syntax, see `formatter`_

Returns: ``text``

::

    cr> select format('%s.%s', schema_name, table_name)  AS fqtable
    ... from sys.shards
    ... where table_name = 'locations'
    ... limit 1;
    +---------------+
    | fqtable       |
    +---------------+
    | doc.locations |
    +---------------+
    SELECT 1 row in set (... sec)

::

    cr> select format('%tY', date) AS year
    ... from locations
    ... group by format('%tY', date)
    ... order by 1;
    +------+
    | year |
    +------+
    | 1979 |
    | 2013 |
    +------+
    SELECT 2 rows in set (... sec)


.. _scalar-substr:

``substr('string', from, [ count ])``
-------------------------------------

Extracts a part of a string. ``from`` specifies where to start and ``count``
the length of the part.

Returns: ``text``

::

    cr> select substr('crate.io', 3, 2) AS substr;
    +--------+
    | substr |
    +--------+
    | at     |
    +--------+
    SELECT 1 row in set (... sec)


``substr('string' FROM 'pattern')``
-----------------------------------

Extract a part from a string that matches a POSIX regular expression pattern.

Returns:: ``text``.

If the pattern contains groups specified via parentheses it returns the first
matching group.
If the pattern doesn't match, the function returns ``NULL``.

::

    cr> SELECT
    ...   substring('2023-08-07', '[a-z]') as no_match,
    ...   substring('2023-08-07', '\d{4}-\d{2}-\d{2}') as full_date,
    ...   substring('2023-08-07', '\d{4}-(\d{2})-\d{2}') as month;
    +----------+------------+-------+
    | no_match | full_date  | month |
    +----------+------------+-------+
    | NULL     | 2023-08-07 |    08 |
    +----------+------------+-------+
    SELECT 1 row in set (... sec)


.. _scalar-substring:

``substring(...)``
---------------...

Alias for :ref:`scalar-substr`.


.. _scalar-char_length:

``char_length('string')``
-------------------------

Counts the number of characters in a string.

Returns: ``integer``

::

    cr> select char_length('crate.io') AS char_length;
    +-------------+
    | char_length |
    +-------------+
    |           8 |
    +-------------+
    SELECT 1 row in set (... sec)

Each character counts only once, regardless of its byte size.

::

    cr> select char_length('©rate.io') AS char_length;
    +-------------+
    | char_length |
    +-------------+
    |           8 |
    +-------------+
    SELECT 1 row in set (... sec)


.. _scalar-length:

``length(text)``
----------------

Returns the number of characters in a string.

The same as :ref:`char_length <scalar-char_length>`.


.. _scalar-bit_length:

``bit_length('string')``
------------------------

Counts the number of bits in a string.

Returns: ``integer``

.. NOTE::

    CrateDB uses UTF-8 encoding internally, which uses between 1 and 4 bytes
    per character.

::

    cr> select bit_length('crate.io') AS bit_length;
    +------------+
    | bit_length |
    +------------+
    |         64 |
    +------------+
    SELECT 1 row in set (... sec)

::

    cr> select bit_length('©rate.io') AS bit_length;
    +------------+
    | bit_length |
    +------------+
    |         72 |
    +------------+
    SELECT 1 row in set (... sec)


.. _scalar-octet_length:

``octet_length('string')``
--------------------------

Counts the number of bytes (octets) in a string.

Returns: ``integer``

::

    cr> select octet_length('crate.io') AS octet_length;
    +--------------+
    | octet_length |
    +--------------+
    |            8 |
    +--------------+
    SELECT 1 row in set (... sec)

::

    cr> select octet_length('©rate.io') AS octet_length;
    +--------------+
    | octet_length |
    +--------------+
    |            9 |
    +--------------+
    SELECT 1 row in set (... sec)


.. _scalar-ascii:

``ascii(string)``
-----------------

Returns the ASCII code of the first character. For UTF-8, returns the Unicode
code point of the characters.

Returns: ``int``

::

    cr> SELECT ascii('a') AS a, ascii('🎈') AS b;
    +----+--------+
    |  a |      b |
    +----+--------+
    | 97 | 127880 |
    +----+--------+
    SELECT 1 row in set (... sec)


.. _scalar-chr:

``chr(int)``
------------

Returns the character with the given code. For UTF-8 the argument is treated as
a Unicode code point.

Returns: ``string``

::

    cr> SELECT chr(65) AS a;
    +---+
    | a |
    +---+
    | A |
    +---+
    SELECT 1 row in set (... sec)


.. _scalar-lower:

``lower('string')``
-------------------

Converts all characters to lowercase. ``lower`` does not perform
locale-sensitive or context-sensitive mappings.

Returns: ``text``

::

    cr> select lower('TransformMe') AS lower;
    +-------------+
    | lower       |
    +-------------+
    | transformme |
    +-------------+
    SELECT 1 row in set (... sec)


.. _scalar-upper:

``upper('string')``
-------------------

Converts all characters to uppercase. ``upper`` does not perform
locale-sensitive or context-sensitive mappings.

Returns: ``text``

::

    cr> select upper('TransformMe') as upper;
    +-------------+
    | upper       |
    +-------------+
    | TRANSFORMME |
    +-------------+
    SELECT 1 row in set (... sec)


.. _scalar-initcap:

``initcap('string')``
---------------------

Converts the first letter of each word to upper case and the rest to lower case
(*capitalize letters*).

Returns: ``text``

::

    cr> select initcap('heLlo WORLD') AS initcap;
    +-------------+
    | initcap     |
    +-------------+
    | Hello World |
    +-------------+
     SELECT 1 row in set (... sec)


.. _scalar-sha1:

``sha1('string')``
------------------

Returns: ``text``

Computes the SHA1 checksum of the given string.

::

    cr> select sha1('foo') AS sha1;
    +------------------------------------------+
    | sha1                                     |
    +------------------------------------------+
    | 0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33 |
    +------------------------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-md5:

``md5('string')``
-----------------

Returns: ``text``

Computes the MD5 checksum of the given string.

See :ref:`sha1 <scalar-sha1>` for an example.


.. _scalar-replace:

``replace(text, from, to)``
---------------------------

Replaces all occurrences of ``from`` in ``text`` with ``to``.

::

    cr> select replace('Hello World', 'World', 'Stranger') AS hello;
    +----------------+
    | hello          |
    +----------------+
    | Hello Stranger |
    +----------------+
    SELECT 1 row in set (... sec)


.. _scalar-translate:

``translate(string, from, to)``
-------------------------------

Performs several single-character, one-to-one translation in one operation. It
translates ``string`` by replacing the characters in the ``from`` set,
one-to-one positionally, with their counterparts in the ``to`` set. If ``from``
is longer than ``to``, the function removes the occurrences of the extra
characters in ``from``. If there are repeated characters in ``from``, only the
first mapping is considered.

Synopsis::

    translate(string, from, to)

Examples::

   cr> select translate('Crate', 'Ct', 'Dk') as translation;
    +-------------+
    | translation |
    +-------------+
    | Drake       |
    +-------------+
    SELECT 1 row in set (... sec)

::

   cr> select translate('Crate', 'rCe', 'c') as translation;
    +-------------+
    | translation |
    +-------------+
    | cat         |
    +-------------+
    SELECT 1 row in set (... sec)


.. _scalar-trim:

``trim({LEADING | TRAILING | BOTH} 'str_arg_1' FROM 'str_arg_2')``
------------------------------------------------------------------

Removes the longest string containing characters from ``str_arg_1`` (``' '`` by
default) from the start, end, or both ends (``BOTH`` is the default) of
``str_arg_2``.

If any of the two strings is ``NULL``, the result is ``NULL``.

Synopsis::

    trim([ [ {LEADING | TRAILING | BOTH} ] [ str_arg_1 ] FROM ] str_arg_2)

Examples::

    cr> select trim(BOTH 'ab' from 'abcba') AS trim;
    +------+
    | trim |
    +------+
    | c    |
    +------+
    SELECT 1 row in set (... sec)

::

    cr> select trim('ab' from 'abcba') AS trim;
    +------+
    | trim |
    +------+
    | c    |
    +------+
    SELECT 1 row in set (... sec)

::

    cr> select trim('   abcba   ') AS trim;
    +-------+
    | trim  |
    +-------+
    | abcba |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-ltrim:

``ltrim(text, [ trimmingText ])``
---------------------------------

Removes set of characters which are matching ``trimmingText`` (``' '`` by
default) to the left of ``text``.

If any of the arguments is ``NULL``, the result is ``NULL``.

::

    cr> select ltrim('xxxzzzabcba', 'xz') AS ltrim;
    +-------+
    | ltrim |
    +-------+
    | abcba |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-rtrim:

``rtrim(text, [ trimmingText ])``
---------------------------------

Removes set of characters which are matching ``trimmingText`` (``' '`` by
default) to the right of ``text``.

If any of the arguments is ``NULL``, the result is ``NULL``.

::

    cr> select rtrim('abcbaxxxzzz', 'xz') AS rtrim;
    +-------+
    | rtrim |
    +-------+
    | abcba |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-btrim:

``btrim(text, [ trimmingText ])``
---------------------------------

A combination of :ref:`ltrim <scalar-ltrim>` and :ref:`rtrim <scalar-rtrim>`,
removing the longest string matching ``trimmingText`` from both the start and
end of ``text``.

If any of the arguments is ``NULL``, the result is ``NULL``.

::

    cr> select btrim('XXHelloXX', 'XX') AS btrim;
    +-------+
    | btrim |
    +-------+
    | Hello |
    +-------+
    SELECT 1 row in set (... sec)



.. _scalar-quote_ident:

``quote_ident(text)``
---------------------

Returns: ``text``

Quotes a provided string argument. Quotes are added only if necessary. For
example, if the string contains non-identifier characters, keywords, or would be
case-folded. Embedded quotes are properly doubled.

The quoted string can be used as an identifier in an SQL statement.

::

    cr> select pg_catalog.quote_ident('Column name') AS quoted;
    +---------------+
    | quoted        |
    +---------------+
    | "Column name" |
    +---------------+
    SELECT 1 row in set (... sec)


.. _scalar-left:

``left('string', len)``
-----------------------

Returns the first ``len`` characters of ``string`` when ``len`` > 0, otherwise
all but last ``len`` characters.

Synopsis::

    left(string, len)

Examples::

    cr> select left('crate.io', 5) AS col;
    +-------+
    | col   |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)

::

    cr> select left('crate.io', -3) AS col;
    +-------+
    | col   |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-right:

``right('string', len)``
------------------------

Returns the last ``len`` characters in ``string`` when ``len`` > 0, otherwise
all but first ``len`` characters.

Synopsis::

    right(string, len)

Examples::

    cr> select right('crate.io', 2) AS col;
    +-----+
    | col |
    +-----+
    | io  |
    +-----+
    SELECT 1 row in set (... sec)

::

    cr> select right('crate.io', -6) AS col;
    +-----+
    | col |
    +-----+
    | io  |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-lpad:

``lpad('string1', len[, 'string2'])``
-------------------------------------

Fill up ``string1`` to length ``len`` by prepending the characters ``string2``
(a space by default). If ``string1`` is already longer than ``len`` then it is
truncated (on the right).

Synopsis::

    lpad(string1, len[, string2])

Example::

    cr> select lpad(' I like CrateDB!!', 41, 'yes! ') AS col;
    +-------------------------------------------+
    | col                                       |
    +-------------------------------------------+
    | yes! yes! yes! yes! yes! I like CrateDB!! |
    +-------------------------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-rpad:

``rpad('string1', len[, 'string2'])``
-------------------------------------

Fill up ``string1`` to length ``len`` by appending the characters ``string2``
(a space by default). If string1 is already longer than ``len`` then it is
truncated.

Synopsis::

    rpad(string1, len[, string2])

Example::

    cr> select rpad('Do you like Crate?', 38, ' yes!') AS col;
    +----------------------------------------+
    | col                                    |
    +----------------------------------------+
    | Do you like Crate? yes! yes! yes! yes! |
    +----------------------------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    In both cases, the scalar functions ``lpad`` and ``rpad`` do now accept a
    length greater than 50000.


.. _scalar-encode:

``encode(bytea, format)``
-------------------------

Encode takes a binary string (``hex`` format) and returns a text encoding using
the specified format. Supported formats are: ``base64``, ``hex``, and
``escape``. The ``escape`` format replaces unprintable characters with octal
byte notation like ``\nnn``. For the reverse function, see :ref:`decode()
<scalar-decode>`.

Synopsis::

    encode(string1, format)

Example::

    cr> select encode(E'123\b\t56', 'base64') AS col;
    +--------------+
    | col          |
    +--------------+
    | MTIzCAk1Ng== |
    +--------------+
    SELECT 1 row in set (... sec)


.. _scalar-decode:

``decode(text, format)``
-------------------------

Decodes a text encoded string using the specified format and returns a binary
string (``hex`` format). Supported formats are: ``base64``, ``hex``, and
``escape``. For the reverse function, see :ref:`encode() <scalar-encode>`.

Synopsis::

    decode(text1, format)

Example::

    cr> select decode('T\214', 'escape') AS col;
    +--------+
    | col    |
    +--------+
    | \x548c |
    +--------+
    SELECT 1 row in set (... sec)


.. _scalar-repeat:

``repeat(text, integer)``
-------------------------

Repeats a string the specified number of times.

If the number of repetitions is equal or less than zero then the function
returns an empty string.

Returns: ``text``

::

    cr> select repeat('ab', 3) AS repeat;
    +--------+
    | repeat |
    +--------+
    | ababab |
    +--------+
    SELECT 1 row in set (... sec)


.. _scalar-split_part:

``split_part(text, text, integer)``
-----------------------------------

Splits a string into parts using a delimiter and returns the part at the given
index. The first part is addressed by index ``1``.

Special Cases:

* Returns the empty string if the index is greater than the number of parts.

* If any of the arguments is ``NULL``, the result is ``NULL``.

* If the delimiter is the empty string, the input string is considered as
  consisting of exactly one part.

Returns: ``text``

Synopsis::

    split_part(string, delimiter, index)

Example::

    cr> select split_part('ab--cdef--gh', '--', 2) AS part;
    +------+
    | part |
    +------+
    | cdef |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-parse_uri:

``parse_uri(text)``
-----------------------------------

Returns: ``object``

Parses the given URI string and returns an object containing the various 
components of the URI. The returned object has the following properties::

    "uri" OBJECT AS (
        "scheme" TEXT,
        "userinfo" TEXT,
        "hostname" TEXT,
        "port" INT,
        "path" TEXT,
        "query" TEXT,
        "fragment" TEXT
    )

.. csv-table::
   :header: "URI Component", "Description"
   :widths: 25, 75
   :align: left

   ``scheme`` , "The scheme of the URI (e.g. ``http``, ``crate``, etc.)"
   ``userinfo`` , "The decoded user-information component of this URI."
   ``hostname`` , "The hostname or IP address specified in the URI."
   ``port`` , "The port number specified in the URI"
   ``path`` , "The decoded path specified in the URI."
   ``query`` , "The decoded query string specified in the URI"
   ``fragment`` , "The query string specified in the URI"

.. NOTE::

    For URI properties not specified in the input string, ``null`` is returned.

Synopsis::

    parse_uri(text)

Example::

    cr> SELECT parse_uri('crate://my_user@cluster.crate.io:5432/doc?sslmode=verify-full') as uri;                                                                               
    +------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | uri                                                                                                                                                        |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | {"fragment": null, "hostname": "cluster.crate.io", "path": "/doc", "port": 5432, "query": "sslmode=verify-full", "scheme": "crate", "userinfo": "my_user"} |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------+
    SELECT 1 row in set (... sec)

If you just want to select a specific URI component, you can use the bracket 
notation on the returned object::

    cr> SELECT parse_uri('crate://my_user@cluster.crate.io:5432')['hostname'] as uri_hostname;                                                                                  
    +------------------+
    | uri_hostname     |
    +------------------+
    | cluster.crate.io |
    +------------------+
    SELECT 1 row in set (... sec)


.. _scalar-parse_url:

``parse_url(text)``
-----------------------------------

Returns: ``object``

Parses the given URL string and returns an object containing the various 
components of the URL. The returned object has the following properties::

    "url" OBJECT AS (
        "scheme" TEXT,
        "userinfo" TEXT,
        "hostname" TEXT,
        "port" INT,
        "path" TEXT,
        "query" TEXT,
        "parameters" OBJECT AS (
            "key1" ARRAY(TEXT),
            "key2" ARRAY(TEXT)   
        ),
        "fragment" TEXT
    )

.. csv-table::
   :header: "URL Component", "Description"
   :widths: 25, 75
   :align: left

   ``scheme`` , "The scheme of the URL (e.g. ``https``, ``crate``, etc.)"
   ``userinfo`` , "The decoded user-information component of this URL."
   ``hostname`` , "The hostname or IP address specified in the URL."
   ``port`` , "The port number specified in the URL. If no port number is specified, the default port for the given scheme will be used."
   ``path`` , "The decoded path specified in the URL."
   ``query`` , "The decoded query string specified in the URL."
   ``parameters`` , "For each query parameter included in the URL, the ``parameter`` property holds an object property that stores an array of decoded text values for that specific query parameter."
   ``fragment`` , "The decoded fragment specified in the URL"

.. NOTE::

    For URL properties not specified in the input string, ``null`` is returned.

Synopsis::

    parse_url(text)

Example::

    cr> SELECT parse_url('https://my_user@cluster.crate.io:8000/doc?sslmode=verify-full') as url;
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | url                                                                                                                                                                                                    |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | {"fragment": null, "hostname": "cluster.crate.io", "parameters": {"sslmode": ["verify-full"]}, "path": "/doc", "port": 8000, "query": "sslmode=verify-full", "scheme": "https", "userinfo": "my_user"} |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    SELECT 1 row in set (... sec)

If you just want to select a specific URL component, you can use the bracket 
notation on the returned object::

    cr> SELECT parse_url('https://my_user@cluster.crate.io:5432')['hostname'] as url_hostname;
    +------------------+
    | url_hostname     |
    +------------------+
    | cluster.crate.io |
    +------------------+
    SELECT 1 row in set (... sec)

Parameter values are always treated as ``text``. There is no conversion of 
comma-separated parameter values into arrays::

    cr> SELECT parse_url('http://crate.io?p1=1,2,3&p1=a&p2[]=1,2,3')['parameters'] as params;
    +-------------------------------------------+
    | params                                    |
    +-------------------------------------------+
    | {"p1": ["1,2,3", "a"], "p2[]": ["1,2,3"]} |
    +-------------------------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-date-time:

Date and time functions
=======================


.. _scalar-date_trunc:

``date_trunc('interval', ['timezone',] timestamp)``
---------------------------------------------------

Returns: ``timestamp with time zone``

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

The following example shows how to use the ``date_trunc`` function to generate
a day based histogram in the ``Europe/Moscow`` timezone::

    cr> select
    ... date_trunc('day', 'Europe/Moscow', date) as day,
    ... count(*) as num_locations
    ... from locations
    ... group by 1
    ... order by 1;
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
    ... group by 1
    ... order by 1;
    +---------------+---------------+
    | day           | num_locations |
    +---------------+---------------+
    | 308534400000  | 4             |
    | 1367366400000 | 1             |
    | 1373932800000 | 8             |
    +---------------+---------------+
    SELECT 3 rows in set (... sec)

.. _date-bin:

``date_bin(interval, timestamp, origin)``
-----------------------------------------

``date_bin`` "bins" the input timestamp to the specified interval, aligned with
a specified origin.

``interval`` is an expression of type ``interval``.
``Timestamp`` and ``origin`` are expressions of type
``timestamp with time zone`` or ``timestamp without time zone``.
The return type matches the timestamp and origin types and will be either
``timestamp with time zone`` or ``timestamp without time zone``.

The return value marks the beginning of the bin into which the input timestamp
is placed.

If you use an interval with a single unit like ``1 second`` or ``1 minute``,
this function returns the same result as :ref:`date_trunc <scalar-date_trunc>`.

Intervals with months and/or year units are not allowed.

If the interval is ``1 week``, ``date_bin`` only returns the same result as
``date_trunc`` if the origin is a Monday.

If at least one argument is ``NULL``, the return value is ``NULL``. The
interval cannot be zero. Negative intervals are allowed and are treated the
same as positive intervals. Intervals having month or year units are not
supported due to varying length of those units.

A timestamp can be binned to an interval of arbitrary length
aligned with a custom origin.

Examples:

::

    cr> SELECT date_bin('2 hours'::INTERVAL, ts,
    ... '2021-01-01T05:00:00Z'::TIMESTAMP) as bin,
    ... date_format('%y-%m-%d %H:%i',
    ... date_bin('2 hours'::INTERVAL, ts, '2021-01-01T05:00:00Z'::TIMESTAMP))
    ... formatted_bin
    ... FROM unnest(ARRAY[
    ... '2021-01-01T08:30:10Z',
    ... '2021-01-01T08:38:10Z',
    ... '2021-01-01T18:18:10Z',
    ... '2021-01-01T18:18:10Z'
    ... ]::TIMESTAMP[]) as tbl (ts);
    +---------------+----------------+
    |           bin | formatted_bin  |
    +---------------+----------------+
    | 1609484400000 | 21-01-01 07:00 |
    | 1609484400000 | 21-01-01 07:00 |
    | 1609520400000 | 21-01-01 17:00 |
    | 1609520400000 | 21-01-01 17:00 |
    +---------------+----------------+
    SELECT 4 rows in set (... sec)

.. TIP::

    0 can be used as a shortcut for Unix zero as the origin::

        cr> select date_bin('2 hours' :: INTERVAL,
        ... '2021-01-01T08:30:10Z' :: timestamp without time ZONE, 0) as bin;
        +---------------+
        |           bin |
        +---------------+
        | 1609488000000 |
        +---------------+
        SELECT 1 row in set (... sec)

    Please note, that implicit cast treats numbers as is, i.e. as a timestamp
    in that zone and if timestamp is in non-UTC zone you might want to set
    numeric origin to the same zone::

        cr> select date_bin('4 hours' :: INTERVAL,
        ... '2020-01-01T09:00:00+0200'::timestamp with time zone,
        ... TIMEZONE('+02:00', 0)) as bin;
        +---------------+
        |           bin |
        +---------------+
        | 1577858400000 |
        +---------------+
        SELECT 1 row in set (... sec)

.. _scalar-extract:

``extract(field from source)``
------------------------------

``extract`` is a special :ref:`expression <gloss-expression>` that translates
to a function which retrieves subcolumns such as day, hour or minute from a
timestamp or an interval.

The return type depends on the used ``field``.

Example with timestamp::

    cr> select extract(day from '2014-08-23') AS day;
    +-----+
    | day |
    +-----+
    |  23 |
    +-----+
    SELECT 1 row in set (... sec)

Example with interval::

    cr> select extract(hour from INTERVAL '5 days 12 hours 45 minutes') AS hour;
    +------+
    | hour |
    +------+
    |   12 |
    +------+
    SELECT 1 row in set (... sec)

Synopsis::

    EXTRACT( field FROM source )

``field``
  An identifier or string literal which identifies the part of the timestamp or
  interval that should be extracted.

``source``
  An expression that resolves to an interval, or a timestamp (with or without
  timezone), or is castable to a timestamp.

.. NOTE::

    When extracting from an :ref:`INTERVAL <type-interval>` there is
    normalization of units, up to days e.g.::

       cr> SELECT extract(day from INTERVAL '14 years 1250 days 49 hours') AS days;
       +------+
       | days |
       +------+
       | 1252 |
       +------+
       SELECT 1 row in set (... sec)

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
  | the day of the month for timestamps, days for intervals

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
  | *Return type:* ``double precision``
  | The number of seconds since Jan 1, 1970.
  | Can be negative if earlier than Jan 1, 1970.

.. _scalar-current_time:

``CURRENT_TIME``
----------------

The ``CURRENT_TIME`` :ref:`expression <gloss-expression>` returns the time in
microseconds since midnight UTC at the time the SQL statement was
handled. Clock time is looked up at most once within the scope of a single
query, to ensure that multiple occurrences of ``CURRENT_TIME`` :ref:`evaluate
<gloss-evaluation>` to the same value.

Synopsis::

    CURRENT_TIME [ ( precision ) ]

``precision``
  Must be a positive integer between 0 and 6. The default value is 6. It
  determines the number of fractional seconds to output. A value of 0 means the
  time will have second precision, no fractional seconds (microseconds) are
  given.

.. NOTE::

    No guarantee is provided about the accuracy of the underlying clock,
    results may be limited to millisecond precision, depending on the system.


.. _scalar-current_timestamp:

``CURRENT_TIMESTAMP``
---------------------

The ``CURRENT_TIMESTAMP`` expression returns the timestamp in milliseconds
since midnight UTC at the time the SQL statement was handled. Therefore, the
same timestamp value is returned for every invocation of a single statement.

Synopsis::

    CURRENT_TIMESTAMP [ ( precision ) ]

``precision``
  Must be a positive integer between ``0`` and ``3``. The default value is
  ``3``. This value determines the number of fractional seconds to output. A
  value of ``0`` means the timestamp will have second precision, no fractional
  seconds (milliseconds) are given.

.. TIP::

    To get an offset value of ``CURRENT_TIMESTAMP`` (e.g., this same time one
    day ago), you can add or subtract an :ref:`interval <type-interval>`,
    like so::

        CURRENT_TIMESTAMP - '1 day'::interval

.. NOTE::

    If the ``CURRENT_TIMESTAMP`` function is used in
    :ref:`ddl-generated-columns` it behaves slightly different in ``UPDATE``
    operations. In such a case the actual timestamp of each row update is
    returned.


.. _scalar-curdate:

``CURDATE()``
----------------

The ``CURDATE()`` scalar function is an alias of the :ref:`scalar-current_date`
expression.

Synopsis::

    CURDATE()


.. _scalar-current_date:

``CURRENT_DATE``
----------------

The ``CURRENT_DATE`` expression returns the date in UTC timezone at the time
the SQL statement was handled.

Clock time is looked up at most once within the scope of a single query, to
ensure that multiple occurrences of ``CURRENT_DATE`` evaluate to the same
value.

Synopsis::

    CURRENT_DATE


.. _scalar-now:

``now()``
---------

Returns the current date and time in UTC.

This is the same as ``current_timestamp``

Returns: ``timestamp with time zone``

Synopsis::

    now()


.. _scalar-date_format:

``date_format([format_string, [timezone,]] timestamp)``
-------------------------------------------------------

The ``date_format`` function formats a timestamp as string according to the
(optional) format string.

Returns: ``text``

Synopsis::

    DATE_FORMAT( [ format_string, [ timezone, ] ] timestamp )

The only mandatory argument is the ``timestamp`` value to format. It can be any
:ref:`expression <gloss-expression>` that is safely convertible to timestamp
data type with or without timezone.

The syntax for the ``format_string`` is 100% compatible to the syntax of the
`MySQL date_format`_ function. For reference, the format is listed in detail
below:

.. csv-table::
   :header: "Format Specifier", "Description"

   ``%a``, "Abbreviated weekday name (Sun..Sat)"
   ``%b``, "Abbreviated month name (Jan..Dec)"
   ``%c``, "Month in year, numeric (0..12)"
   ``%D``, "Day of month as ordinal number (1st, 2nd, ... 24th)"
   ``%d``, "Day of month, padded to 2 digits (00..31)"
   ``%e``, "Day of month (0..31)"
   ``%f``, "Microseconds, padded to 6 digits (000000..999999)"
   ``%H``, "Hour in 24-hour clock, padded to 2 digits (00..23)"
   ``%h``, "Hour in 12-hour clock, padded to 2 digits (01..12)"
   ``%I``, "Hour in 12-hour clock, padded to 2 digits (01..12)"
   ``%i``, "Minutes, numeric (00..59)"
   ``%j``, "Day of year, padded to 3 digits (001..366)"
   ``%k``, "Hour in 24-hour clock (0..23)"
   ``%l``, "Hour in 12-hour clock (1..12)"
   ``%M``, "Month name (January..December)"
   ``%m``, "Month in year, numeric, padded to 2 digits (00..12)"
   ``%p``, "AM or PM"
   ``%r``, "Time, 12-hour (``hh:mm:ss`` followed by AM or PM)"
   ``%S``, "Seconds, padded to 2 digits (00..59)"
   ``%s``, "Seconds, padded to 2 digits (00..59)"
   ``%T``, "Time, 24-hour (``hh:mm:ss``)"
   ``%U``, "Week number, Sunday as first day of the week, first week of the
   year (01) is the one starting in this year, week 00 starts in last year
   (00..53)"
   ``%u``, "Week number, Monday as first day of the week, first week of the
   year (01) is the one with at least 4 days in this year (00..53)"
   ``%V``, "Week number, Sunday as first day of the week, first week of the
   year (01) is the one starting in this year, uses the week number of the last
   year, if the week started in last year (01..53)"
   ``%v``, "Week number, Monday as first day of the week, first week of the
   year (01) is the one with at least 4 days in this year, uses the week number
   of the last year, if the week started in last year (01..53)"
   ``%W``, "Weekday name (Sunday..Saturday)"
   ``%w``, "Day of the week (0=Sunday..6=Saturday)"
   ``%X``, "Week year, Sunday as first day of the week, numeric, four digits;
   used with %V"
   ``%x``, "Week year, Monday as first day of the week, numeric, four digits;
   used with %v"
   ``%Y``, "Year, numeric, four digits"
   ``%y``, "Year, numeric, two digits"
   ``%%``, "A literal '%' character"
   ``%x``, "x, for any 'x' not listed above"

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


.. _scalar-timezone:

``timezone(timezone, timestamp)``
---------------------------------

The timezone scalar function converts values of ``timestamp`` without time zone
to/from timestamp with time zone.

Synopsis::

    TIMEZONE(timezone, timestamp)

It has two variants depending on the type of ``timestamp``:

.. csv-table::
   :header: "Type of timestamp", "Return Type", "Description"

   "timestamp without time zone OR bigint", "timestamp with time zone", "Treat
   given timestamp without time zone as located in the specified timezone"
   "timestamp with time zone", "timestamp without time zone", "Convert given
   timestamp with time zone to the new timezone with no time zone designation"

::

    cr> select
    ...     257504400000 as no_tz,
    ...     date_format(
    ...         '%Y-%m-%d %h:%i', 257504400000
    ...     ) as no_tz_str,
    ...     timezone(
    ...         'Europe/Madrid', 257504400000
    ...     ) as in_madrid,
    ...     date_format(
    ...         '%Y-%m-%d %h:%i',
    ...         timezone(
    ...             'Europe/Madrid', 257504400000
    ...         )
    ...     ) as in_madrid_str;
    +--------------+------------------+--------------+------------------+
    |        no_tz | no_tz_str        |    in_madrid | in_madrid_str    |
    +--------------+------------------+--------------+------------------+
    | 257504400000 | 1978-02-28 09:00 | 257500800000 | 1978-02-28 08:00 |
    +--------------+------------------+--------------+------------------+
    SELECT 1 row in set (... sec)

::

    cr> select
    ...     timezone(
    ...         'Europe/Madrid',
    ...         '1978-02-28T10:00:00+01:00'::timestamp with time zone
    ...     ) as epoque,
    ...     date_format(
    ...          '%Y-%m-%d %h:%i',
    ...          timezone(
    ...              'Europe/Madrid',
    ...              '1978-02-28T10:00:00+01:00'::timestamp with time zone
    ...          )
    ...     ) as epoque_str;
    +--------------+------------------+
    |       epoque | epoque_str       |
    +--------------+------------------+
    | 257508000000 | 1978-02-28 10:00 |
    +--------------+------------------+
    SELECT 1 row in set (... sec)

::

    cr> select
    ...     timezone(
    ...         'Europe/Madrid',
    ...         '1978-02-28T10:00:00+01:00'::timestamp without time zone
    ...     ) as epoque,
    ...     date_format(
    ...         '%Y-%m-%d %h:%i',
    ...         timezone(
    ...             'Europe/Madrid',
    ...             '1978-02-28T10:00:00+01:00'::timestamp without time zone
    ...         )
    ...     ) as epoque_str;
    +--------------+------------------+
    |       epoque | epoque_str       |
    +--------------+------------------+
    | 257504400000 | 1978-02-28 09:00 |
    +--------------+------------------+
    SELECT 1 row in set (... sec)


.. _scalar-to_char:

``to_char(expression, format_string)``
--------------------------------------

The ``to_char`` function converts a ``timestamp`` or ``interval`` value to a
string, based on a given format string.

Returns: ``text``

Synopsis::

    TO_CHAR( expression, format_string )

Here, ``expression`` can be any value with the type of ``timestamp`` (with or
without a timezone) or ``interval``.

The syntax for the ``format_string`` differs based the type of the
:ref:`expression <gloss-expression>`. For ``timestamp`` expressions, the
``format_string`` is a template string containing any of the following symbols:

+-----------------------+-----------------------------------------------------+
| Pattern               | Description                                         |
+=======================+=====================================================+
| ``HH`` / ``HH12``     | Hour of day (01-12)                                 |
+-----------------------+-----------------------------------------------------+
| ``HH24``              | Hour of day (00-23)                                 |
+-----------------------+-----------------------------------------------------+
| ``MI``                | Minute (00-59)                                      |
+-----------------------+-----------------------------------------------------+
| ``SS``                | Second (00-59)                                      |
+-----------------------+-----------------------------------------------------+
| ``MS``                | Millisecond (000-999)                               |
+-----------------------+-----------------------------------------------------+
| ``US``                | Microsecond (000000-999999)                         |
+-----------------------+-----------------------------------------------------+
| ``FF1``               | Tenth of second (0-9)                               |
+-----------------------+-----------------------------------------------------+
| ``FF2``               | Hundredth of second (00-99)                         |
+-----------------------+-----------------------------------------------------+
| ``FF3``               | Millisecond (000-999)                               |
+-----------------------+-----------------------------------------------------+
| ``FF4``               | Tenth of millisecond (0000-9999)                    |
+-----------------------+-----------------------------------------------------+
| ``FF5``               | Hundredth of millisecond (00000-99999)              |
+-----------------------+-----------------------------------------------------+
| ``FF6``               | Microsecond (000000-999999)                         |
+-----------------------+-----------------------------------------------------+
| ``SSSS`` / ``SSSSS``  | Seconds past midnight (0-86399)                     |
+-----------------------+-----------------------------------------------------+
| ``AM`` / ``am`` /     | Meridiem indicator                                  |
| ``PM`` / ``pm``       |                                                     |
+-----------------------+-----------------------------------------------------+
| ``A.M.`` / ``a.m.`` / | Meridiem indicator (with periods)                   |
| ``P.M.`` / ``p.m.``   |                                                     |
+-----------------------+-----------------------------------------------------+
| ``Y,YYY``             | 4 digit year with comma                             |
+-----------------------+-----------------------------------------------------+
| ``YYYY``              | 4 digit year                                        |
+-----------------------+-----------------------------------------------------+
| ``YYY``               | Last 3 digits of year                               |
+-----------------------+-----------------------------------------------------+
| ``YY``                | Last 2 digits of year                               |
+-----------------------+-----------------------------------------------------+
| ``Y``                 | Last digit of year                                  |
+-----------------------+-----------------------------------------------------+
| ``IYYY``              | 4 digit ISO-8601 week-numbering year                |
+-----------------------+-----------------------------------------------------+
| ``IYY``               | Last 3 digits of ISO-8601 week-numbering year       |
+-----------------------+-----------------------------------------------------+
| ``IY``                | Last 2 digits of ISO-8601 week-numbering year       |
+-----------------------+-----------------------------------------------------+
| ``I``                 | Last digit of ISO-8601 week-numbering year          |
+-----------------------+-----------------------------------------------------+
| ``BC`` / ``bc`` /     | Era indicator                                       |
| ``AD`` / ``ad``       |                                                     |
+-----------------------+-----------------------------------------------------+
| ``B.C.`` / ``b.c.`` / | Era indicator with periods                          |
| ``A.D.`` / ``a.d.``   |                                                     |
+-----------------------+-----------------------------------------------------+
| ``MONTH`` / ``Month`` | Full month name (uppercase, capitalized, lowercase) |
| / ``month``           | padded to 9 characters                              |
+-----------------------+-----------------------------------------------------+
| ``MON`` / ``Mon`` /   | Short month name (uppercase, capitalized,           |
| ``mon``               | lowercase) padded to 9 characters                   |
+-----------------------+-----------------------------------------------------+
| ``MM``                | Month number (01-12)                                |
+-----------------------+-----------------------------------------------------+
| ``DAY`` / ``Day`` /   | Full day name (uppercase, capitalized, lowercase)   |
| ``day``               | padded to 9 characters                              |
+-----------------------+-----------------------------------------------------+
| ``DY`` / ``Dy`` /     | Short, 3 character day name                         |
| ``dy``                | (uppercase, capitalized, lowercase)                 |
+-----------------------+-----------------------------------------------------+
| ``DDD``               | Day of year (001-366)                               |
+-----------------------+-----------------------------------------------------+
| ``IDDD``              | Day of ISO-8601 week-numbering year, where the      |
|                       | first Monday of the first ISO week is day 1         |
|                       | (001-371)                                           |
+-----------------------+-----------------------------------------------------+
| ``DD``                | Day of month (01-31)                                |
+-----------------------+-----------------------------------------------------+
| ``D``                 | Day of the week, from Sunday (1) to Saturday (7)    |
+-----------------------+-----------------------------------------------------+
| ``ID``                | ISO-8601 day of the week, from Monday (1) to Sunday |
|                       | (7)                                                 |
+-----------------------+-----------------------------------------------------+
| ``W``                 | Week of month (1-5)                                 |
+-----------------------+-----------------------------------------------------+
| ``WW``                | Week number of year (1-53)                          |
+-----------------------+-----------------------------------------------------+
| ``IW``                | Week number of ISO-8601 week-numbering year (01-53) |
+-----------------------+-----------------------------------------------------+
| ``CC``                | Century                                             |
+-----------------------+-----------------------------------------------------+
| ``J``                 | Julian Day                                          |
+-----------------------+-----------------------------------------------------+
| ``Q``                 | Quarter                                             |
+-----------------------+-----------------------------------------------------+
| ``RM`` / ``rm``       | Month in Roman numerals (uppercase, lowercase)      |
+-----------------------+-----------------------------------------------------+
| ``TZ`` / ``tz``       | Time-zone abbreviation (uppercase, lowercase)       |
+-----------------------+-----------------------------------------------------+
| ``TZH``               | Time-zone hours                                     |
+-----------------------+-----------------------------------------------------+
| ``TZM``               | Time-zone minutes                                   |
+-----------------------+-----------------------------------------------------+
| ``OF``                | Time-zone offset from UTC                           |
+-----------------------+-----------------------------------------------------+

Example::

    cr> select
    ...     to_char(
    ...         timestamp '1970-01-01T17:31:12',
    ...         'Day, Month DD - HH12:MI AM YYYY AD'
    ...     ) as ts;
    +-----------------------------------------+
    | ts                                      |
    +-----------------------------------------+
    | Thursday, January 01 - 05:31 PM 1970 AD |
    +-----------------------------------------+
    SELECT 1 row in set (... sec)

For ``interval`` expressions, the formatting string accepts the same tokens as
``timestamp`` expressions. The function then uses the timestamp of the
specified interval added to the timestamp of ``0000/01/01 00:00:00``::

    cr> select
    ...     to_char(
    ...         interval '1 year 3 weeks 200 minutes',
    ...         'YYYY MM DD HH12:MI:SS'
    ...     ) as interval;
    +---------------------+
    | interval            |
    +---------------------+
    | 0001 01 22 03:20:00 |
    +---------------------+
    SELECT 1 row in set (... sec)

.. _scalar-pg-age:

``age([timestamp,] timestamp)``
---------------------------------------------------

Returns: :ref:`interval <type-interval>` between 2 timestamps. Second argument
is subtracted from the first one. If at least one argument is ``NULL``, the
return value is ``NULL``. If only one timestamp is given, the return value is
interval between current_date (at midnight) and the given timestamp.

Example::

    cr> select pg_catalog.age('2021-10-21'::timestamp, '2021-10-20'::timestamp)
    ... as age;
    +----------------+
    | age            |
    +----------------+
    | 1 day 00:00:00 |
    +----------------+
    SELECT 1 row in set (... sec)

    cr> select pg_catalog.age(date_trunc('day', CURRENT_DATE)) as age;
    +----------+
    | age      |
    +----------+
    | 00:00:00 |
    +----------+
    SELECT 1 row in set (... sec)

.. _scalar-geo:

Geo functions
=============


.. _scalar-distance:

``distance(geo_point1, geo_point2)``
------------------------------------

Returns: ``double precision``

The ``distance`` function can be used to calculate the distance between two
points on earth. It uses the `Haversine formula`_ which gives great-circle
distances between 2 points on a sphere based on their latitude and longitude.

The return value is the distance in meters.

Below is an example of the distance function where both points are specified
using WKT. See :ref:`data-types-geo` for more information on the implicit
type casting of geo points::

    cr> select distance('POINT (10 20)', 'POINT (11 21)') AS col;
    +-------------------+
    |               col |
    +-------------------+
    | 152354.3209044634 |
    +-------------------+
    SELECT 1 row in set (... sec)

This scalar function can always be used in both the ``WHERE`` and ``ORDER BY``
clauses. With the limitation that one of the arguments must be a literal and
the other argument must be a column reference.

.. NOTE::

    The algorithm of the calculation which is used when the distance function
    is used as part of the result column list has a different precision than
    what is stored inside the index which is utilized if the distance function
    is part of a WHERE clause.

    For example, if ``select distance(...)`` returns 0.0, an equality check
    with ``where distance(...) = 0`` might not yield anything at all due to the
    precision difference.


.. _scalar-within:

``within(shape1, shape2)``
--------------------------

Returns: ``boolean``

The ``within`` function returns true if ``shape1`` is within ``shape2``. If
that is not the case false is returned.

``shape1`` can either be a ``geo_shape`` or a ``geo_point``. ``shape2`` must be
a ``geo_shape``.

Below is an example of the ``within`` function which makes use of the implicit
type casting from strings in WKT representation to geo point and geo shapes::

    cr> select within(
    ...   'POINT (10 10)',
    ...   'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'
    ... ) AS is_within;
    +-----------+
    | is_within |
    +-----------+
    | TRUE      |
    +-----------+
    SELECT 1 row in set (... sec)

This function can always be used within the ``WHERE`` clause.


.. _scalar-intersects:

``intersects(geo_shape, geo_shape)``
------------------------------------

Returns: ``boolean``

The ``intersects`` function returns true if both argument shapes share some
points or area, they *overlap*. This also includes two shapes where one lies
:ref:`within <scalar-within>` the other.

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

Due to a limitation on the :ref:`data-types-geo-shape` datatype this function
cannot be used in the :ref:`ORDER BY <sql-select-order-by>` clause.


.. _scalar-latitude-longitude:

``latitude(geo_point)`` and ``longitude(geo_point)``
----------------------------------------------------

Returns: ``double precision``

The ``latitude`` and ``longitude`` function return the coordinates of latitude
or longitude of a point, or ``NULL`` if not available. The input must be a
column of type ``geo_point``, a valid WKT string or a ``double precision``
array. See :ref:`data-types-geo` for more information on the implicit type
casting of geo points.

Example::

    cr> select
    ...     mountain,
    ...     height,
    ...     longitude(coordinates) as "lon",
    ...     latitude(coordinates) as "lat"
    ... from sys.summits
    ... order by height desc limit 1;
    +------------+--------+---------+---------+
    | mountain   | height |     lon |     lat |
    +------------+--------+---------+---------+
    | Mont Blanc |   4808 | 6.86444 | 45.8325 |
    +------------+--------+---------+---------+
    SELECT 1 row in set (... sec)

Below is an example of the latitude/longitude functions which make use of the
implicit type casting from strings to geo point::

    cr> select
    ...    latitude('POINT (10 20)') AS lat,
    ...    longitude([10.0, 20.0]) AS long;
    +------+------+
    |  lat | long |
    +------+------+
    | 20.0 | 10.0 |
    +------+------+
    SELECT 1 row in set (... sec)


.. _scalar-geohash:

``geohash(geo_point)``
----------------------

Returns: ``text``

Returns a `GeoHash <https://en.wikipedia.org/wiki/Geohash>`_ representation
based on full precision (12 characters) of the input point, or ``NULL`` if not
available. The input has to be a column of type ``geo_point``, a valid WKT
string or a ``double precision`` array. See :ref:`data-types-geo` for more
information of the implicit type casting of geo points.

Example::

    cr> select
    ...     mountain,
    ...     height,
    ...     geohash(coordinates) as "geohash"
    ... from sys.summits
    ... order by height desc limit 1;
    +------------+--------+--------------+
    | mountain   | height | geohash      |
    +------------+--------+--------------+
    | Mont Blanc |   4808 | u0huspw99j1r |
    +------------+--------+--------------+
    SELECT 1 row in set (... sec)



.. _scalar-area:

``area(geo_shape)``
----------------------

Returns: ``double precision``

The ``area`` function calculates the  area of the input shape in
square-degrees. The calculation will use geospatial awareness (AKA `geodetic`_)
instead of `Euclidean geometry`_. The input has to be a column of type
:ref:`data-types-geo-shape`, a valid `WKT`_ string or `GeoJSON`_.
See :ref:`data-types-geo-shape` for more information.

Below you can find an example.

Example::

    cr> select
    ...     round(area('POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))')) as "area";
    +------+
    | area |
    +------+
    |   25 |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-math:

Mathematical functions
======================

All mathematical functions can be used within ``WHERE`` and ``ORDER BY``
clauses.


.. _scalar-abs:

``abs(number)``
---------------

Returns the absolute value of the given number in the datatype of the given
number::

    cr> select abs(214748.0998) AS a, abs(0) AS b, abs(-214748) AS c;
    +-------------+---+--------+
    |           a | b |      c |
    +-------------+---+--------+
    | 214748.0998 | 0 | 214748 |
    +-------------+---+--------+
    SELECT 1 row in set (... sec)


.. _scalar-ceil:

``ceil(number)``
----------------

Returns the smallest integer or long value that is not less than the argument.

Returns: ``bigint`` or ``integer``

Return value will be of type ``integer`` if the input value is an integer or
float. If the input value is of type ``bigint`` or ``double precision`` the
return value will be of type ``bigint``::

    cr> select ceil(29.9) AS col;
    +-----+
    | col |
    +-----+
    |  30 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-ceiling:

``ceiling(number)``
-------------------

This is an alias for :ref:`ceil <scalar-ceil>`.


.. _scalar-degrees:

``degrees(double precision)``
-----------------------------

Convert the given ``radians`` value to ``degrees``.

Returns: ``double precision``

::

    cr> select degrees(0.5) AS degrees;
    +-------------------+
    |           degrees |
    +-------------------+
    | 28.64788975654116 |
    +-------------------+
    SELECT 1 row in set (... sec)


.. _scalar-exp:

``exp(number)``
---------------

Returns Euler's number ``e`` raised to the power of the given numeric value.
The output will be cast to the given input type and thus may loose precision.

Returns: Same as input type.

::

    > select exp(1.0) AS exp;
    +-------------------+
    |               exp |
    +-------------------+
    | 2.718281828459045 |
    +-------------------+
    SELECT 1 row in set (... sec)

.. test skipped because java.lang.Math.exp() can return with different
   precision on different CPUs (e.g.: Apple M1)

.. _scalar-floor:

``floor(number)``
-----------------

Returns the largest integer or long value that is not greater than the
argument.

Returns: ``bigint`` or ``integer``

Return value will be an integer if the input value is an integer or a float. If
the input value is of type ``bigint`` or ``double precision`` the return value
will be of type ``bigint``.

See below for an example::

    cr> select floor(29.9) AS floor;
    +-------+
    | floor |
    +-------+
    |    29 |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-ln:

``ln(number)``
--------------

Returns the natural logarithm of given ``number``.

Returns: ``double precision``

See below for an example::

    cr> SELECT ln(1) AS ln;
    +-----+
    |  ln |
    +-----+
    | 0.0 |
    +-----+
    SELECT 1 row in set (... sec)

.. NOTE::

    An error is returned for arguments which lead to undefined or illegal
    results. E.g. ln(0) results in ``minus infinity``, and therefore, an error
    is returned.


.. _scalar-log:

``log(x : number, b : number)``
-------------------------------

Returns the logarithm of given ``x`` to base ``b``.

Returns: ``double precision``

See below for an example, which essentially is the same as above::

    cr> SELECT log(100, 10) AS log;
    +-----+
    | log |
    +-----+
    | 2.0 |
    +-----+
    SELECT 1 row in set (... sec)

The second argument (``b``) is optional. If not present, base 10 is used::

    cr> SELECT log(100) AS log;
    +-----+
    | log |
    +-----+
    | 2.0 |
    +-----+
    SELECT 1 row in set (... sec)

.. NOTE::

    An error is returned for arguments which lead to undefined or illegal
    results. E.g. log(0) results in ``minus infinity``, and therefore, an error
    is returned.

    The same is true for arguments which lead to a ``division by zero``, as,
    e.g., log(10, 1) does.


.. _scalar-modulus:

``modulus(y, x)``
-----------------

Returns the remainder of ``y/x``.

Returns: Same as argument types.

::

    cr> select modulus(5, 4) AS mod;
    +-----+
    | mod |
    +-----+
    |   1 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-mod:

``mod(y, x)``
-----------------

This is an alias for :ref:`modulus <scalar-modulus>`.


.. _scalar-power:

``power(a: number, b: number)``
-------------------------------

Returns the given argument ``a`` raised to the power of argument ``b``.

Returns: ``double precision``

The return type of the power function is always ``double precision``, even when
both the inputs are integral types, in order to be consistent across positive
and negative exponents (which will yield decimal types).

See below for an example::

    cr> SELECT power(2,3) AS pow;
    +-----+
    | pow |
    +-----+
    | 8.0 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-radians:

``radians(double precision)``
-----------------------------

Convert the given ``degrees`` value to ``radians``.

Returns: ``double precision``

::

    cr> select radians(45.0) AS radians;
    +--------------------+
    |            radians |
    +--------------------+
    | 0.7853981633974483 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-random:

``random()``
------------

The ``random`` function returns a random value in the range 0.0 <= X < 1.0.

Returns: ``double precision``

.. NOTE::

    Every call to ``random`` will yield a new random number.


.. _scalar-gen_random_text_uuid:

``gen_random_text_uuid()``
--------------------------

Returns a random time based UUID as ``text``. The returned ID is similar to
flake IDs and well suited for use as primary key value.

Note that the ID is opaque (i.e., not to be considered meaningful in any way)
and the implementation is free to change.


.. _scalar-round:

``round(number)``
-----------------

If the input is of type ``double precision`` or ``bigint`` the result is the
closest ``bigint`` to the argument, with ties rounding up.

If the input is of type ``real`` or ``integer`` the result is the closest
integer to the argument, with ties rounding up.

Returns: ``bigint`` or ``integer``

See below for an example::

    cr> select round(42.2) AS round;
    +-------+
    | round |
    +-------+
    |    42 |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-trunc:

``trunc(number[, precision])``
------------------------------

Returns ``number`` truncated to the specified ``precision`` (decimal places).

When ``precision`` is not specified, the result's type is an ``integer``, or
``bigint``. When it is specified, the result's type is ``double precision``.
Notice that ``trunc(number)`` and ``trunc(number, 0)`` return different result
types.

See below for examples::

    cr> select trunc(29.999999, 3) AS trunc;
    +--------+
    |  trunc |
    +--------+
    | 29.999 |
    +--------+
    SELECT 1 row in set (... sec)

    cr> select trunc(29.999999) AS trunc;
    +-------+
    | trunc |
    +-------+
    |    29 |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-sqrt:

``sqrt(number)``
----------------

Returns the square root of the argument.

Returns: ``double precision``

See below for an example::

    cr> select sqrt(25.0) AS sqrt;
    +------+
    | sqrt |
    +------+
    |  5.0 |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-sin:

``sin(number)``
---------------

Returns the sine of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT sin(1) AS sin;
    +--------------------+
    |                sin |
    +--------------------+
    | 0.8414709848078965 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-asin:

``asin(number)``
----------------

Returns the arcsine of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT asin(1) AS asin;
    +--------------------+
    |               asin |
    +--------------------+
    | 1.5707963267948966 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-cos:

``cos(number)``
---------------

Returns the cosine of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT cos(1) AS cos;
    +--------------------+
    |                cos |
    +--------------------+
    | 0.5403023058681398 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-acos:

``acos(number)``
----------------

Returns the arccosine of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT acos(-1) AS acos;
    +-------------------+
    |              acos |
    +-------------------+
    | 3.141592653589793 |
    +-------------------+
    SELECT 1 row in set (... sec)


.. _scalar-tan:

``tan(number)``
---------------

Returns the tangent of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT tan(1) AS tan;
    +--------------------+
    |                tan |
    +--------------------+
    | 1.5574077246549023 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-cot:

``cot(number)``
---------------

Returns the cotangent of the argument that represents the angle expressed in
radians. The range of the argument is all real numbers. The cotangent of zero
is undefined and returns ``Infinity``.

Returns: ``double precision``

See below for an example::

    cr> select cot(1) AS cot;
    +--------------------+
    |                cot |
    +--------------------+
    | 0.6420926159343306 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-atan:

``atan(number)``
----------------

Returns the arctangent of the argument.

Returns: ``double precision``

See below for an example::

    cr> SELECT atan(1) AS atan;
    +--------------------+
    |               atan |
    +--------------------+
    | 0.7853981633974483 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-atan2:

``atan2(y: number, x: number)``
-------------------------------

Returns the arctangent of ``y/x``.

Returns: ``double precision``

::

    cr> SELECT atan2(2, 1) AS atan2;
    +--------------------+
    |              atan2 |
    +--------------------+
    | 1.1071487177940904 |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-pi:

``pi()``
--------

Returns the π constant.

Returns: ``double precision``

::

    cr> SELECT pi() AS pi;
    +-------------------+
    |                pi |
    +-------------------+
    | 3.141592653589793 |
    +-------------------+
    SELECT 1 row in set (... sec)


.. _scalar-regexp:

Regular expression functions
============================

The :ref:`regular expression <gloss-regular-expression>` functions in CrateDB
use `Java Regular Expressions`_.

See the API documentation for more details.

.. NOTE::

    Be aware that, in contrast to the functions, the :ref:`regular expression
    operator <sql_dql_regexp>` uses `Lucene Regular Expressions`_.


.. _scalar-regexp_replace:

``regexp_replace(source, pattern, replacement [, flags])``
----------------------------------------------------------

``regexp_replace`` can be used to replace every (or only the first) occurrence
of a subsequence matching ``pattern`` in the ``source`` string with the
``replacement`` string. If no subsequence in ``source`` matches the regular
expression ``pattern``, ``source`` is returned unchanged.

Returns: ``text``

``pattern`` is a Java regular expression. For details on the regexp syntax, see
`Java Regular Expressions`_.

The ``replacement`` string may contain expressions like ``$N`` where ``N`` is a
digit between 0 and 9. It references the nth matched group of ``pattern``
and the matching subsequence of that group will be inserted in the returned
string. The expression ``$0`` will insert the whole matching ``source``.

By default, only the first occurrence of a subsequence matching ``pattern``
will be replaced. If all occurrences shall be replaced use the ``g`` flag.


.. _scalar-regexp_replace-flags:

Flags
.....

``regexp_replace`` supports a number of flags as optional parameters. These
flags are given as a string containing any of the characters listed below.
Order does not matter.

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
| ``g`` | replace all occurrences of a subsequence matching ``pattern``,      |
|       | not only the first                                                  |
+-------+---------------------------------------------------------------------+


.. _scalar-regexp_replace-examples:

Examples
........

::

   cr> select
   ...     name,
   ...     regexp_replace(
   ...         name, '(\w+)\s(\w+)+', '$1 - $2'
   ...      ) as replaced
   ... from locations
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

   cr> select
   ...     regexp_replace(
   ...         'alcatraz', '(foo)(bar)+', '$1baz'
   ...     ) as replaced;
    +----------+
    | replaced |
    +----------+
    | alcatraz |
    +----------+
    SELECT 1 row in set (... sec)

::

   cr> select
   ...     name,
   ...     regexp_replace(
   ...         name, '([A-Z]\w+) .+', '$1', 'ig'
   ...     ) as replaced
   ... from locations
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


.. _scalar-arrays:

Array functions
===============

.. _scalar-array_append:

``array_append(anyarray, value)``
----------------------------------------

The ``array_append`` function adds the value at the end of the array

Returns: ``array``

::

    cr> select
    ...     array_append([1,2,3], 4) AS array_append;
    +--------------+
    | array_append |
    +--------------+
    | [1, 2, 3, 4] |
    +--------------+
    SELECT 1 row in set (... sec)


.. _scalar-array_cat:

``array_cat(first_array, second_array)``
----------------------------------------

The ``array_cat`` function concatenates two arrays into one array

Returns: ``array``

::

    cr> select
    ...     array_cat([1,2,3],[3,4,5,6]) AS array_cat;
    +-----------------------+
    | array_cat             |
    +-----------------------+
    | [1, 2, 3, 3, 4, 5, 6] |
    +-----------------------+
    SELECT 1 row in set (... sec)


You can also use the concat :ref:`operator <gloss-operator>` ``||`` with
arrays::

    cr> select
    ...     [1,2,3] || [4,5,6] || [7,8,9] AS arr;
    +-----------------------------+
    | arr                         |
    +-----------------------------+
    | [1, 2, 3, 4, 5, 6, 7, 8, 9] |
    +-----------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-array_unique:

``array_unique(first_array, [ second_array])``
----------------------------------------------

The ``array_unique`` function merges two arrays into one array with unique
elements

Returns: ``array``

::

    cr> select
    ...     array_unique(
    ...         [1, 2, 3],
    ...         [3, 4, 4]
    ...     ) AS arr;
    +--------------+
    | arr          |
    +--------------+
    | [1, 2, 3, 4] |
    +--------------+
    SELECT 1 row in set (... sec)

If the arrays have different types all elements will be cast to a common type
based on the type precedence.

::

    cr> select
    ...      array_unique(
    ...          [10, 20],
    ...          [10.0, 20.3]
    ...      ) AS arr;
    +--------------------+
    | arr                |
    +--------------------+
    | [10.0, 20.0, 20.3] |
    +--------------------+
    SELECT 1 row in set (... sec)


.. _scalar-array_difference:

``array_difference(first_array, second_array)``
-----------------------------------------------

The ``array_difference`` function removes elements from the first array that
are contained in the second array.

Returns: ``array``

::

    cr> select
    ...     array_difference(
    ...         [1,2,3,4,5,6,7,8,9,10],
    ...         [2,3,6,9,15]
    ...     ) AS arr;
    +---------------------+
    | arr                 |
    +---------------------+
    | [1, 4, 5, 7, 8, 10] |
    +---------------------+
    SELECT 1 row in set (... sec)


.. _scalar-array:

``array(subquery)``
-------------------

The ``array(subquery)`` :ref:`expression <gloss-expression>` is an array
constructor function which operates on the result of the ``subquery``.

Returns: ``array``

.. SEEALSO::

    :ref:`Array construction with subquery <sql_expressions_array_subquery>`


.. _scalar-array_upper:

``array_upper(anyarray, dimension)``
------------------------------------

The ``array_upper`` function returns the number of elements in the requested
array dimension (the upper bound of the dimension). CrateDB allows mixing
arrays with different sizes on the same dimension. Returns ``NULL`` if array
argument is ``NULL`` or if dimension <= 0 or if dimension is ``NULL``.

Returns: ``integer``

::

    cr> select array_upper([[1, 4], [3]], 1) AS size;
    +------+
    | size |
    +------+
    |    2 |
    +------+
    SELECT 1 row in set (... sec)

An empty array has no dimension and returns ``NULL`` instead of ``0``.

::

    cr> select array_upper(ARRAY[]::int[], 1) AS size;
    +------+
    | size |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-array_length:

``array_length(anyarray, dimension)``
-------------------------------------

An alias for :ref:`scalar-array_upper`.

::

    cr> select array_length([[1, 4], [3]], 1) AS len;
    +-----+
    | len |
    +-----+
    |   2 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-array_lower:

``array_lower(anyarray, dimension)``
------------------------------------

The ``array_lower`` function returns the lower bound of the requested array
dimension (which is ``1`` if the dimension is valid and has at least one
element). Returns ``NULL`` if array argument is ``NULL`` or if dimension <= 0
or if dimension is ``NULL``.

Returns: ``integer``

::

    cr> select array_lower([[1, 4], [3]], 1) AS size;
    +------+
    | size |
    +------+
    |    1 |
    +------+
    SELECT 1 row in set (... sec)

If there is at least one empty array or ``NULL`` on the requested dimension
return value is ``NULL``. Example:

::

    cr> select array_lower([[1, 4], [3], []], 2) AS size;
    +------+
    | size |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-array_set:

``array_set(array, index, value)``
----------------------------------

The ``array_set`` function returns the array with the element at ``index`` set
to ``value``.

Gaps are filled with ``null``.

Returns: ``array``

::

    cr> select array_set(['_', 'b'], 1, 'a') AS arr;
    +------------+
    | arr        |
    +------------+
    | ["a", "b"] |
    +------------+
    SELECT 1 row in set (... sec)


``array_set(source_array, indexes_array, values_array)``
--------------------------------------------------------

Second overload for ``array_set`` that updates many indices with many values at
once. Depending on the indexes provided, ``array_set`` updates or appends the
values and also fills any gaps with ``nulls``.

Returns: ``array``

::

    cr> select array_set(['_', 'b'], [1, 4], ['a', 'd']) AS arr;
    +-----------------------+
    | arr                   |
    +-----------------------+
    | ["a", "b", null, "d"] |
    +-----------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    Updating indexes less than or equal to 0 is not supported.


.. _scalar-array_slice:

``array_slice(anyarray, from, to)``
-----------------------------------

The ``array_slice`` function returns a slice of the given array using the given
lower and upper bound.

Returns: ``array``

.. SEEALSO::

    :ref:`Accessing arrays<sql_dql_arrays>`

::

    cr> select array_slice(['a', 'b', 'c', 'd'], 2, 3) AS arr;
    +------------+
    | arr        |
    +------------+
    | ["b", "c"] |
    +------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    The first index value is ``1``. The maximum array index is ``2147483647``.
    Both the ``from`` and ``to`` index values are inclusive.
    Using an index greater than the array size results in an empty array.

.. _scalar-array_to_string:

``array_to_string(anyarray, separator, [ null_string ])``
---------------------------------------------------------

The ``array_to_string`` function concatenates elements of the given array into
a single string using the ``separator``.

Returns: ``text``

::

    cr> select
    ...     array_to_string(
    ...         ['Arthur', 'Ford', 'Trillian'], ','
    ...     ) AS str;
    +----------------------+
    | str                  |
    +----------------------+
    | Arthur,Ford,Trillian |
    +----------------------+
    SELECT 1 row in set (... sec)

If the ``separator`` argument is ``NULL``, the result is ``NULL``::

    cr> select
    ...     array_to_string(
    ...         ['Arthur', 'Ford', 'Trillian'], NULL
    ...     ) AS str;
    +------+
    |  str |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)

If ``null_string`` is provided and is not ``NULL``, then ``NULL`` elements of
the array are replaced by that string, otherwise they are omitted::

    cr> select
    ...     array_to_string(
    ...         ['Arthur', NULL, 'Trillian'], ',', 'Ford'
    ...     ) AS str;
    +----------------------+
    | str                  |
    +----------------------+
    | Arthur,Ford,Trillian |
    +----------------------+
    SELECT 1 row in set (... sec)

::

    cr> select
    ...     array_to_string(
    ...         ['Arthur', NULL, 'Trillian'], ','
    ...     ) AS str;
    +-----------------+
    | str             |
    +-----------------+
    | Arthur,Trillian |
    +-----------------+
    SELECT 1 row in set (... sec)

::

    cr> select
    ...     array_to_string(
    ...         ['Arthur', NULL, 'Trillian'], ',', NULL
    ...     ) AS str;
    +-----------------+
    | str             |
    +-----------------+
    | Arthur,Trillian |
    +-----------------+
    SELECT 1 row in set (... sec)


.. _scalar-string_to_array:

``string_to_array(string, separator, [ null_string ])``
-------------------------------------------------------

The ``string_to_array`` splits a string into an array of ``text`` elements
using a supplied separator and an optional null-string to set matching
substring elements to NULL.

Returns: ``array(text)``

::

    cr> select string_to_array('Arthur,Ford,Trillian', ',') AS arr;
    +--------------------------------+
    | arr                            |
    +--------------------------------+
    | ["Arthur", "Ford", "Trillian"] |
    +--------------------------------+
    SELECT 1 row in set (... sec)

::

    cr> select string_to_array('Arthur,Ford,Trillian', ',', 'Ford') AS arr;
    +------------------------------+
    | arr                          |
    +------------------------------+
    | ["Arthur", null, "Trillian"] |
    +------------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-string_to_array-separator:

``separator``
.............

If the ``separator`` argument is NULL, each character of the input string
becomes a separate element in the resulting array.

::

    cr> select string_to_array('Ford', NULL) AS arr;
    +----------------------+
    | arr                  |
    +----------------------+
    | ["F", "o", "r", "d"] |
    +----------------------+
    SELECT 1 row in set (... sec)

If the separator is an empty string, then the entire input string is returned
as a one-element array.

::

    cr> select string_to_array('Arthur,Ford', '') AS arr;
    +-----------------+
    | arr             |
    +-----------------+
    | ["Arthur,Ford"] |
    +-----------------+
    SELECT 1 row in set (... sec)


.. _scalar-string_to_array-null_string:

``null_string``
...............

If the ``null_string`` argument is omitted or NULL, none of the substrings of
the input will be replaced by NULL.


.. _scalar-array_min:

``array_min(array)``
--------------------

The ``array_min`` function returns the smallest element in ``array``. If
``array`` is ``NULL`` or an empty array, the function returns ``NULL``. This
function supports arrays of any of the :ref:`primitive types
<data-types-primitive>`.

::

    cr> SELECT array_min([3, 2, 1]) AS min;
    +-----+
    | min |
    +-----+
    |   1 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-array_position:

``array_position(anycompatiblearray, anycompatible [, integer ] ) → integer``
-----------------------------------------------------------------------------

The ``array_position`` function returns the position of the first
occurrence of the second argument in the ``array``, or ``NULL`` if it's not
present. If the third argument is given, the search begins at that position.
The third argument is ignored if it's null. If not within the ``array`` range,
``NULL`` is returned. It is also possible to search for ``NULL`` values.

::

    cr> SELECT array_position([1,3,7,4], 7) as position;
    +----------+
    | position |
    +----------+
    |        3 |
    +----------+
    SELECT 1 row in set (... sec)

Begin the search from given position (optional).

::

    cr> SELECT array_position([1,3,7,4], 7, 2) as position;
    +----------+
    | position |
    +----------+
    |        3 |
    +----------+
    SELECT 1 row in set (... sec)

.. TIP::
    When searching for the existence of an ``array`` element, using the
    :ref:`ANY <sql_any_array_comparison>` operator inside the ``WHERE``
    clause is much more efficient as it can utilize the index whereas
    ``array_position`` won't even when used inside the ``WHERE`` clause.


.. _scalar-array_max:

``array_max(array)``
--------------------

The ``array_max`` function returns the largest element in ``array``. If
``array`` is ``NULL`` or an empty array, the function returns ``NULL``. This
function supports arrays of any of the :ref:`primitive types
<data-types-primitive>`.

::

    cr> SELECT array_max([1,2,3]) AS max;
    +-----+
    | max |
    +-----+
    |   3 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-array_sum:

``array_sum(array)``
--------------------

Returns the sum of array elements that are not ``NULL``. If ``array`` is
``NULL`` or an empty array, the function returns ``NULL``. This function
supports arrays of any :ref:`numeric types <type-numeric>`.

For ``real`` and ``double precison`` arguments, the return type is equal to the
argument type. For ``char``, ``smallint``, ``integer``, and ``bigint``
arguments, the return type changes to ``bigint``.

If any ``bigint`` value exceeds range limits (-2^64 to 2^64-1), an
``ArithmeticException`` will be raised.

::

    cr> SELECT array_sum([1,2,3]) AS sum;
    +-----+
    | sum |
    +-----+
    |   6 |
    +-----+
    SELECT 1 row in set (... sec)

The sum on the bigint array will result in an overflow in the following query:

::

    cr> SELECT
    ...     array_sum(
    ...         [9223372036854775807, 9223372036854775807]
    ...     ) as sum;
    ArithmeticException[long overflow]

To address the overflow of the sum of the given array elements, we cast the
array to the numeric data type:

::

    cr>  SELECT
    ...     array_sum(
    ...         [9223372036854775807, 9223372036854775807]::numeric[]
    ...     ) as sum;
    +----------------------+
    |                  sum |
    +----------------------+
    | 18446744073709551614 |
    +----------------------+
    SELECT 1 row in set (... sec)


.. _scalar-array_avg:

``array_avg(array)``
--------------------

Returns the average of all values in ``array`` that are not ``NULL`` If
``array`` is ``NULL`` or an empty array, the function returns ``NULL``. This
function supports arrays of any :ref:`numeric types <type-numeric>`.

For ``real`` and ``double precison`` arguments, the return type is equal to the
argument type. For ``char``, ``smallint``, ``integer``, and ``bigint``
arguments, the return type is ``numeric``.

::

    cr> SELECT array_avg([1,2,3]) AS avg;
    +-----+
    | avg |
    +-----+
    |   2 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-array_unnest:

``array_unnest(nested_array)``
------------------------------

Takes a nested array and returns a flattened array. Only flattens one level at a
time.

Returns ``NULL`` if the argument is ``NULL``. ``NULL`` array elements are
skipped and ``NULL`` leaf elements within arrays are preserved.

::

    cr> SELECT array_unnest([[1, 2], [3, 4, 5]]) AS result;
    +-----------------+
    | result          |
    +-----------------+
    | [1, 2, 3, 4, 5] |
    +-----------------+
    SELECT 1 row in set (... sec)


    cr> SELECT array_unnest([[1, null, 2], null, [3, 4, 5]]) AS result;
    +-----------------------+
    | result                |
    +-----------------------+
    | [1, null, 2, 3, 4, 5] |
    +-----------------------+
    SELECT 1 row in set (... sec)

.. SEEALSO::

    :ref:`UNNEST table function <unnest>`


.. _scalar-objects:

Object functions
================

.. _scalar-object_keys:

``object_keys(object)``
-----------------------

The ``object_keys`` function returns the set of first level keys of an ``object``.

Returns: ``array(text)``

::

    cr> SELECT
    ...     object_keys({a = 1, b = {c = 2}}) AS object_keys;
    +-------------+
    | object_keys |
    +-------------+
    | ["a", "b"]  |
    +-------------+
    SELECT 1 row in set (... sec)


.. _scalar-concat-object:

``concat(object, object)``
--------------------------

The ``concat(object, object)`` function combines two objects into a new object 
containing the union of their first level properties, taking the second 
object's values for duplicate properties.  If one of the objects is ``NULL``, 
the function returns the non-``NULL`` object. If both objects are ``NULL``, 
the function returns ``NULL``.

Returns: ``object``

::

    cr> SELECT
    ...     concat({a = 1}, {a = 2, b = {c = 2}}) AS object_concat;
    +-------------------------+
    | object_concat           |
    +-------------------------+
    | {"a": 2, "b": {"c": 2}} |
    +-------------------------+
    SELECT 1 row in set (... sec)


You can also use the concat :ref:`operator <gloss-operator>` ``||`` with
objects::

    cr> SELECT
    ...     {a = 1} || {b = 2} || {c = 3} AS object_concat;
    +--------------------------+
    | object_concat            |
    +--------------------------+
    | {"a": 1, "b": 2, "c": 3} |
    +--------------------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    ``concat(object, object)`` does not operate recursively: only the 
    top-level object structure is merged::
        
        cr> SELECT
        ...     concat({a = {b = 4}}, {a = {c = 2}}) as object_concat;                                                                                                                                                                                                                            
        +-----------------+
        | object_concat   |
        +-----------------+
        | {"a": {"c": 2}} |
        +-----------------+
        SELECT 1 row in set (... sec)


.. _scalar-null-or-empty:


``null_or_empty(object)``
-------------------------

The ``null_or_empty(object)`` function returns a boolean indicating if an object
is ``NULL`` or empty (``{}``).

This can serve as a faster alternative to ``IS NULL`` if matching on empty
objects is acceptable. It makes better use of indices.

::

    cr> SELECT null_or_empty({}) x, null_or_empty(NULL) y, null_or_empty({x=10}) z;
    +------+------+-------+
    | x    | y    | z     |
    +------+------+-------+
    | TRUE | TRUE | FALSE |
    +------+------+-------+
    SELECT 1 row in set (... sec)


.. _scalar-conditional-fn-exp:

Conditional functions and expressions
=====================================


.. _scalar-case-when-then-end:

``CASE WHEN ... THEN ... END``
------------------------------

The ``case`` :ref:`expression <gloss-expression>` is a generic conditional
expression similar to if/else statements in other programming languages and can
be used wherever an expression is valid.

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

    cr> create table case_example (id bigint);
    CREATE OK, 1 row affected (... sec)
    cr> insert into case_example (id) values (0),(1),(2),(3);
    INSERT OK, 4 rows affected (... sec)
    cr> refresh table case_example
    REFRESH OK, 1 row affected (... sec)

Example::

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

As a variant, a ``case`` expression can be written using the *simple* form::

    CASE expression
         WHEN value THEN result
         [WHEN ...]
         [ELSE result]
    END

Example::

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


.. _scalar-if:

``if(condition, result [, default])``
-------------------------------------

The ``if`` function is a conditional function comparing to *if* statements of
most other programming languages. If the given *condition* :ref:`expression
<gloss-expression>` :ref:`evaluates <gloss-evaluation>` to ``true``, the
*result* expression is evaluated and its value is returned. If the *condition*
evaluates to ``false``, the *result* expression is not evaluated and the
optional given *default* expression is evaluated instead and its value will be
returned. If the *default* argument is omitted, ``NULL`` will be returned
instead.

.. Hidden: create table if_example

    cr> create table if_example (id bigint);
    CREATE OK, 1 row affected (... sec)
    cr> insert into if_example (id) values (0),(1),(2),(3);
    INSERT OK, 4 rows affected (... sec)
    cr> refresh table if_example
    REFRESH OK, 1 row affected (... sec)

::

    cr> select
    ...     id,
    ...     if(id = 0, 'zero', 'other') as description
    ... from if_example
    ... order by id;
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


.. _scalar-coalesce:

``coalesce('first_arg', second_arg [, ... ])``
----------------------------------------------

The ``coalesce`` function takes one or more arguments of the same type and
returns the first non-null value of these. The result will be NULL only if all
the arguments :ref:`evaluate <gloss-evaluation>` to NULL.

Returns: same type as arguments

::

    cr> select coalesce(clustered_by, 'nothing') AS clustered_by
    ...   from information_schema.tables
    ...   where table_name='nodes';
    +--------------+
    | clustered_by |
    +--------------+
    | nothing      |
    +--------------+
    SELECT 1 row in set (... sec)

.. NOTE::

    If the data types of the arguments are not of the same type, ``coalesce``
    will try to cast them to a common type, and if it fails to do so, an error
    is thrown.


.. _scalar-greatest:

``greatest('first_arg', second_arg[ , ... ])``
----------------------------------------------

The ``greatest`` function takes one or more arguments of the same type and will
return the largest value of these. NULL values in the arguments list are
ignored. The result will be NULL only if all the arguments :ref:`evaluate
<gloss-evaluation>` to NULL.

Returns: same type as arguments

::

    cr> select greatest(1, 2) AS greatest;
    +----------+
    | greatest |
    +----------+
    |        2 |
    +----------+
    SELECT 1 row in set (... sec)

.. NOTE::

    If the data types of the arguments are not of the same type, ``greatest``
    will try to cast them to a common type, and if it fails to do so, an error
    is thrown.


.. _scalar-least:

``least('first_arg', second_arg[ , ... ])``
-------------------------------------------

The ``least`` function takes one or more arguments of the same type and will
return the smallest value of these. NULL values in the arguments list are
ignored. The result will be NULL only if all the arguments :ref:`evaluate
<gloss-evaluation>` to NULL.

Returns: same type as arguments

::

    cr> select least(1, 2) AS least;
    +-------+
    | least |
    +-------+
    |     1 |
    +-------+
    SELECT 1 row in set (... sec)

.. NOTE::

    If the data types of the arguments are not of the same type, ``least`` will
    try to cast them to a common type, and if it fails to do so, an error is
    thrown.


.. _scalar-nullif:

``nullif('first_arg', second_arg)``
-----------------------------------

The ``nullif`` function compares two arguments of the same type and, if they
have the same value, returns NULL; otherwise returns the first argument.

Returns: same type as arguments

::

    cr> select nullif(table_schema, 'sys') AS nullif
    ...   from information_schema.tables
    ...   where table_name='nodes';
    +--------+
    | nullif |
    +--------+
    |   NULL |
    +--------+
    SELECT 1 row in set (... sec)

.. NOTE::

    If the data types of the arguments are not of the same type, ``nullif`` will
    try to cast them to a common type, and if it fails to do so, an error is
    thrown.

.. _scalar-sysinfo:

System information functions
============================


.. _scalar-current_schema:

``CURRENT_SCHEMA``
------------------

The ``CURRENT_SCHEMA`` system information function returns the name of the
current schema of the session. If no current schema is set, this function will
return the default schema, which is ``doc``.

Returns: ``text``

The default schema can be set when using the `JDBC client
<https://crate.io/docs/jdbc/en/latest/connect.html>`_ and :ref:`HTTP clients
<http-default-schema>` such as `CrateDB PDO`_.

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


.. _scalar-current_schemas:

``CURRENT_SCHEMAS(boolean)``
----------------------------

The ``CURRENT_SCHEMAS()`` system information function returns the current
stored schemas inside the :ref:`search_path <conf-session-search-path>` session
state, optionally including implicit schemas (e.g. ``pg_catalog``). If no
custom :ref:`search_path <conf-session-search-path>` is set, this function will
return the default :ref:`search_path <conf-session-search-path>` schemas.

Returns: ``array(text)``

Synopsis::

    CURRENT_SCHEMAS ( boolean )

Example::

    cr> SELECT CURRENT_SCHEMAS(true) AS schemas;
    +-----------------------+
    | schemas               |
    +-----------------------+
    | ["pg_catalog", "doc"] |
    +-----------------------+
    SELECT 1 row in set (... sec)


.. _scalar-current_user:

``CURRENT_USER``
----------------

The ``CURRENT_USER`` system information function returns the name of the
current connected user or ``crate`` if the user management module is disabled.

Returns: ``text``

Synopsis::

    CURRENT_USER

Example::

    cr> select current_user AS name;
    +-------+
    | name  |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-user:

``USER``
--------

Equivalent to `CURRENT_USER`_.

Returns: ``text``

Synopsis::

    USER

Example::

    cr> select user AS name;
    +-------+
    | name  |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-session_user:

``SESSION_USER``
----------------

The ``SESSION_USER`` system information function returns the name of the
current connected user or ``crate`` if the user management module is disabled.

Returns: ``text``

Synopsis::

    SESSION_USER

Example::

    cr> select session_user AS name;
    +-------+
    | name  |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)

.. NOTE::

    CrateDB doesn't currently support the switching of execution context. This
    makes `SESSION_USER`_ functionally equivalent to `CURRENT_USER`_. We
    provide it as it's part of the SQL standard.

    Additionally, the `CURRENT_USER`_, `SESSION_USER`_ and `USER`_ functions
    have a special SQL syntax, meaning that they must be called without
    trailing parenthesis (``()``).

.. _scalar-has-database-priv:

``has_database_privilege([user,] database, privilege text)``
------------------------------------------------------------

Returns ``boolean`` or ``NULL`` if at least one argument is ``NULL``.

First argument is ``TEXT`` user name or ``INTEGER`` user OID. If user is not
specified current user is used as an argument.

Second argument is ``TEXT`` database name or ``INTEGER`` database OID.

.. NOTE::

    Only `crate` is valid for database name and only `0` is valid for database
    OID.

Third argument is privilege(s) to check. Multiple privileges
can be provided as a comma separated list, in which case the result will be
``true`` if any of the listed privileges is held. Allowed privilege types are
``CONNECT``, ``CREATE`` and ``TEMP`` or ``TEMPORARY``. Privilege string is case
insensitive and extra whitespace is allowed between privilege names. Duplicate
entries in privilege string are allowed.

:CONNECT:
  is ``true`` for all defined users in the database

:CREATE:
  is ``true`` if the user has any ``DDL`` privilege on ``CLUSTER`` or on any
  ``SCHEMA``

:TEMP:
  is ``false`` for all users

Example::

    cr> select has_database_privilege('crate', ' Connect ,  CREATe ')
    ... as has_priv;
    +----------+
    | has_priv |
    +----------+
    | TRUE     |
    +----------+
    SELECT 1 row in set (... sec)


.. _scalar-has-schema-priv:

``has_schema_privilege([user,] schema, privilege text)``
--------------------------------------------------------

Returns ``boolean`` or ``NULL`` if at least one argument is ``NULL``.

First argument is ``TEXT`` user name or ``INTEGER`` user OID. If user is not
specified current user is used as an argument.

Second argument is ``TEXT`` schema name or ``INTEGER`` schema OID.

Third argument is privilege(s) to check. Multiple privileges
can be provided as a comma separated list, in which case the result will be
``true`` if any of the listed privileges is held. Allowed privilege types are
``CREATE`` and ``USAGE`` which corresponds to CrateDB's ``DDL`` and ``DQL``.
Privilege string is case insensitive and extra whitespace is allowed between
privilege names. Duplicate entries in privilege string are allowed.

Example::

    cr> select has_schema_privilege('pg_catalog', ' Create , UsaGe , CREATe ')
    ... as has_priv;
    +----------+
    | has_priv |
    +----------+
    | TRUE     |
    +----------+
    SELECT 1 row in set (... sec)

.. _scalar-pg_backend_pid:

``pg_backend_pid()``
--------------------

The ``pg_backend_pid()`` system information function is implemented for
enhanced compatibility with PostgreSQL. CrateDB will always return ``-1`` as
there isn't a single process attached to one query. This is different to
PostgreSQL, where this represents the process ID of the server process attached
to the current session.

Returns: ``integer``

Synopsis::

    pg_backend_pid()

Example::

    cr> select pg_backend_pid() AS pid;
    +-----+
    | pid |
    +-----+
    |  -1 |
    +-----+
    SELECT 1 row in set (... sec)


.. _scalar-pg_postmaster_start_time:

``pg_postmaster_start_time()``
------------------------------

Returns the server start time as ``timestamp with time zone``.


.. _scalar-current_database:

``current_database()``
----------------------

The ``current_database`` function returns the name of the current database,
which in CrateDB will always be ``crate``::

    cr> select current_database() AS db;
    +-------+
    | db    |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-current_setting:

``current_setting(text [,boolean])``
------------------------------------

The ``current_setting`` function returns the current value of a :ref:`session
setting <conf-session>`.

Returns: ``text``

Synopsis::

    current_setting(setting_name [, missing_ok])

If no setting exists for ``setting_name``, current_setting throws an error,
unless ``missing_ok`` argument is provided and is true.

Examples::

    cr> select current_setting('search_path') AS search_path;
    +-------------+
    | search_path |
    +-------------+
    | doc         |
    +-------------+
    SELECT 1 row in set (... sec)

::

    cr> select current_setting('foo');
    SQLParseException[Unrecognised Setting: foo]

::

    cr> select current_setting('foo', true) AS foo;
    +------+
    |  foo |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-pg_get_expr:

``pg_get_expr()``
-----------------

The function ``pg_get_expr`` is implemented to improve compatibility with
clients that use the PostgreSQL wire protocol. The function always returns
``null``.

Synopsis::

   pg_get_expr(expr text, relation_oid int [, pretty boolean])

Example::

    cr> select pg_get_expr('literal', 1) AS col;
    +------+
    |  col |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)

.. _scalar-pg_get_partkeydef:

``pg_get_partkeydef()``
-----------------------

The function ``pg_get_partkeydef`` is implemented to improve compatibility with
clients that use the PostgreSQL wire protocol. Partitioning in CrateDB is
different from PostgreSQL, therefore this function always returns ``null``.

Synopsis::

   pg_get_partkeydef(relation_oid int)

Example::

    cr> select pg_get_partkeydef(1) AS col;
    +------+
    |  col |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)

.. _scalar-pg_get_serial_sequence:

``pg_get_serial_sequence()``
----------------------------

The function ``pg_get_serial_sequence`` is implemented to improve compatibility
with clients that use the PostgreSQL wire protocol. The function always returns
``null``. Existence of tables or columns is not validated.

Synopsis::

   pg_get_serial_sequence(table_name text, column_name text)

Example::

    cr> select pg_get_serial_sequence('t1', 'c1') AS col;
    +------+
    |  col |
    +------+
    | NULL |
    +------+
    SELECT 1 row in set (... sec)

.. _scalar-pg_encoding_to_char:

``pg_encoding_to_char()``
-------------------------

The function ``pg_encoding_to_char`` converts an PostgreSQL encoding's internal
identifier to a human-readable name.

Returns: ``text``

Synopsis::

   pg_encoding_to_char(encoding int)

Example::

    cr> select pg_encoding_to_char(6) AS encoding;
    +----------+
    | encoding |
    +----------+
    | UTF8     |
    +----------+
    SELECT 1 row in set (... sec)


.. _scalar-pg_get_userbyid:

``pg_get_userbyid()``
---------------------

The function ``pg_get_userbyid`` is implemented to improve compatibility with
clients that use the PostgreSQL wire protocol. The function always returns the
default CrateDB user for non-null arguments, otherwise, ``null`` is returned.

Returns: ``text``

Synopsis::

   pg_get_userbyid(id integer)

Example::

    cr> select pg_get_userbyid(1) AS name;
    +-------+
    | name  |
    +-------+
    | crate |
    +-------+
    SELECT 1 row in set (... sec)


.. _scalar-pg_typeof:

``pg_typeof()``
---------------

The function ``pg_typeof`` returns the text representation of the value's data
type passed to it.

Returns: ``text``

Synopsis::

   pg_typeof(expression)

Example:

::

    cr> select pg_typeof([1, 2, 3]) as typeof;
    +---------------+
    | typeof        |
    +---------------+
    | integer_array |
    +---------------+
    SELECT 1 row in set (... sec)


.. _scalar-pg_function_is_visible:

``pg_function_is_visible()``
----------------------------

The function ``pg_function_is_visible`` returns true for OIDs that refer to a
system or a user defined function.

Returns: ``boolean``

Synopsis::

   pg_function_is_visible(OID)

Example:

::

    cr> select pg_function_is_visible(-919555782) as pg_function_is_visible;
    +------------------------+
    | pg_function_is_visible |
    +------------------------+
    | TRUE                   |
    +------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-pg_get_function_result:

``pg_get_function_result()``
----------------------------

The function ``pg_get_function_result`` returns the text representation of the
return value's data type of the function referred by the OID.

Returns: ``text``

Synopsis::

   pg_get_function_result(OID)

Example:

::

    cr> select pg_get_function_result(-919555782) as _pg_get_function_result;
    +-------------------------+
    | _pg_get_function_result |
    +-------------------------+
    | time with time zone     |
    +-------------------------+
    SELECT 1 row in set (... sec)


.. _scalar-version:

``version()``
-------------

Returns the CrateDB version information.

Returns: ``text``

Synopsis::

  version()

Example:

::

    cr> select version() AS version;
    +---------...-+
    | version     |
    +---------...-+
    | CrateDB ... |
    +---------...-+
    SELECT 1 row in set (... sec)


.. _scalar-col_description:

``col_description(integer, integer)``
-------------------------------------

This function exists mainly for compatibility with PostgreSQL. In PostgreSQL,
the function returns the comment for a table column. CrateDB doesn't support
user defined comments for table columns, so it always returns ``null``.


Returns: ``text``

Example:

::

    cr> SELECT pg_catalog.col_description(1, 1) AS comment;
    +---------+
    | comment |
    +---------+
    |    NULL |
    +---------+
    SELECT 1 row in set (... sec)


.. _scalar-obj_description:

``obj_description(integer, text)``
----------------------------------

This function exists mainly for compatibility with PostgreSQL. In PostgreSQL,
the function returns the comment for a database object. CrateDB doesn't support
user defined comments for database objects, so it always returns ``null``.


Returns: ``text``

Example:

::

    cr> SELECT pg_catalog.obj_description(1, 'pg_type') AS comment;
    +---------+
    | comment |
    +---------+
    |    NULL |
    +---------+
    SELECT 1 row in set (... sec)


.. _scalar-format_type:

``format_type(integer, integer)``
---------------------------------

Returns the type name of a type. The first argument is the ``OID`` of the type.
The second argument is the type modifier. This function exits for PostgreSQL
compatibility and the type modifier is always ignored.

Returns: ``text``

Example:

::

    cr> SELECT pg_catalog.format_type(25, null) AS name;
    +------+
    | name |
    +------+
    | text |
    +------+
    SELECT 1 row in set (... sec)


If the given ``OID`` is not know, ``???`` is returned::


    cr> SELECT pg_catalog.format_type(3, null) AS name;
    +------+
    | name |
    +------+
    |  ??? |
    +------+
    SELECT 1 row in set (... sec)


.. _scalar-special:

Special functions
=================


.. _scalar_knn_match:


``knn_match(float_vector, float_vector, int)``
----------------------------------------------

The ``knn_match`` function uses a k-nearest
neighbour (kNN) search algorithm to find vectors that are similar
to a query vector.

The first argument is the column to search.
The second argument is the query vector.
The third argument is the number of nearest neighbours to search in the index.
Searching a larger number of nearest neighbours is more expensive. There is one
index per shard, and on each shard the function will match at most `k` records.
To limit the total query result, add a :ref:`LIMIT clause <sql-select-limit>` to
the query.

``knn_match(search_vector, target, k)``

This function must be used within a ``WHERE`` clause targeting a table to use it
as a predicate that searches the whole dataset of a table.
Using it *outside* of a ``WHERE`` clause, or in a ``WHERE`` clause targeting a
virtual table instead of a physical table, results in an error.

Similar to the :ref:`MATCH predicate <predicates_match>`, this function affects
the :ref:`_score <sql_administration_system_column_score>` value.

An example::


    cr> CREATE TABLE IF NOT EXISTS doc.vectors (
    ...    xs float_vector(2)
    ...  );
    CREATE OK, 1 row affected (... sec)

    cr> INSERT INTO doc.vectors (xs)
    ...   VALUES
    ...   ([3.14, 8.17]),
    ...   ([14.3, 19.4]);
    INSERT OK, 2 rows affected (... sec)

.. HIDE:

    cr> REFRESH TABLE doc.vectors;
    REFRESH OK, 1 row affected (... sec)

::

    cr> SELECT xs, _score FROM doc.vectors
    ... WHERE knn_match(xs, [3.14, 8], 2)
    ... ORDER BY _score DESC;
    +--------------+--------------+
    | xs           |       _score |
    +--------------+--------------+
    | [3.14, 8.17] | 0.9719117    |
    | [14.3, 19.4] | 0.0039138086 |
    +--------------+--------------+
    SELECT 2 rows in set (... sec)


.. _scalar-ignore3vl:

``ignore3vl(boolean)``
----------------------

The ``ignore3vl`` function operates on a boolean argument and eliminates the
`3-valued logic`_ on the whole tree of :ref:`operators <gloss-operator>`
beneath it. More specifically, ``FALSE`` is :ref:`evaluated <gloss-evaluation>`
to ``FALSE``, ``TRUE`` to ``TRUE`` and ``NULL`` to ``FALSE``.

Returns: ``boolean``

.. HIDE:

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
    when a ``NOT`` operator is involved. Such filtering, with `3-valued
    logic`_, cannot be translated to an optimized query in the internal storage
    engine, and therefore can degrade performance. E.g.::

        SELECT * FROM t
        WHERE NOT 5 = ANY(t.int_array_col);

    If we can ignore the `3-valued logic`_, we can write the query as::

        SELECT * FROM t
        WHERE NOT IGNORE3VL(5 = ANY(t.int_array_col));

    which will yield better performance (in execution time) than before.

.. CAUTION::

    If there are ``NULL`` values in the ``long_array_col``, in the case that
    ``5 = ANY(t.long_array_col)`` evaluates to ``NULL``, without the
    ``ignore3vl``, it would be evaluated as ``NOT NULL`` => ``NULL``, resulting
    to zero matched rows. With the ``IGNORE3VL`` in place it will be evaluated
    as ``NOT FALSE`` => ``TRUE`` resulting to all rows matching the
    filter. E.g::

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

.. HIDE:

    cr> DROP TABLE IF EXISTS doc.t;
    DROP OK, 1 row affected (... sec)

Synopsis::

    ignore3vl(boolean)

Example::

    cr> SELECT
    ...     ignore3vl(true) as v1,
    ...     ignore3vl(false) as v2,
    ...     ignore3vl(null) as v3;
    +------+-------+-------+
    | v1   | v2    | v3    |
    +------+-------+-------+
    | TRUE | FALSE | FALSE |
    +------+-------+-------+
    SELECT 1 row in set (... sec)


.. _3-valued logic: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
.. _available time zones: https://www.joda.org/joda-time/timezones.html
.. _CrateDB PDO: https://crate.io/docs/pdo/en/latest/connect.html
.. _Euclidean geometry: https://en.wikipedia.org/wiki/Euclidean_geometry
.. _formatter: https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html
.. _geodetic: https://en.wikipedia.org/wiki/Geodesy
.. _GeoJSON: https://geojson.org/
.. _Haversine formula: https://en.wikipedia.org/wiki/Haversine_formula
.. _Java DateTimeFormatter: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
.. _Java DecimalFormat: https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html
.. _Java Regular Expressions: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
.. _Joda-Time: https://www.joda.org/joda-time/
.. _Lucene Regular Expressions: https://lucene.apache.org/core/4_9_0/core/org/apache/lucene/util/automaton/RegExp.html
.. _MySQL date_format: https://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format
.. _WKT: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
