.. _sql_administration_udf:

======================
User-Defined Functions
======================

.. rubric:: Table of Contents

.. contents::
   :local:

``CREATE OR REPLACE``
=====================

CrateDB supports user-defined functions. See :ref:`ref-create-function` for a
full syntax description.

These functions can be created like so::

    cr> CREATE FUNCTION my_subtract_function(integer, integer)
    ...  RETURNS integer
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function my_subtract_function(a, b) { return a - b; }';
    CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('my_subtract_function(1::integer, 1::integer)')

::

    cr> SELECT doc.my_subtract_function(3, 1);
    +--------------------------------+
    | doc.my_subtract_function(3, 1) |
    +--------------------------------+
    |                              2 |
    +--------------------------------+
    SELECT 1 row in set (... sec)

``OR REPLACE`` can be used to replace an existing function::

    cr> CREATE OR REPLACE FUNCTION log10(long)
    ...  RETURNS double
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function log10(a) { return Math.log(a)/Math.log(10); }';
    CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('log10(1::long)')

::

    cr> SELECT doc.log10(10);
    +---------------+
    | doc.log10(10) |
    +---------------+
    |           1.0 |
    +---------------+
    SELECT 1 row in set (... sec)

Arguments can be named in the function definition.

For example, if you wanted two ``geo_point`` arguments named ``start_point``
and ``end_point``, you would do it like this::

    cr> CREATE OR REPLACE FUNCTION calculate_distance(start_point geo_point, end_point geo_point)
    ...  RETURNS float
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function calculate_distance(start_point, end_point){
    ...        return Math.sqrt( Math.pow(end_point[0] - start_point[0], 2), Math.pow(end_point[1] - start_point[1], 2));
    ...      }';
    CREATE OK, 1 row affected  (... sec)


.. NOTE::

    Argument names are used for query documentation purposes only. You cannot
    reference arguments by name in the function body.

Optionally, you can specify a schema for the function. If you omit the schema,
the current session schema is used.

You can explicitly assign a schema like this::

    cr> CREATE OR REPLACE FUNCTION my_schema.log10(long)
    ...  RETURNS double
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function log10(a) { return Math.log(a)/Math.log(10); }';
    CREATE OK, 1 row affected  (... sec)


.. WARNING::

   :ref:`snapshot-restore` can't be used to backup functions, because snapshots
   contain table data only.

Supported Types
===============

The argument types, and the return type of the function can be any of the
CrateDB supported :ref:`data-types`. Data types of values passed into a
function must strictly correspond to its argument data types.

.. NOTE::

    The value returned by the function will be casted to the return type
    provided in the definition if required. An exception will be thrown if the
    cast is not successful.

Overloading
===========

Within a specific schema, you can overload functions by defining two functions
with the same name that have a different set of arguments::

    cr> CREATE FUNCTION my_schema.my_multiply(integer, integer)
    ...  RETURNS integer
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function my_multiply(a, b) { return a * b; }';
    CREATE OK, 1 row affected  (... sec)

This would overload our ``my_multiply`` function with different argument
types::

    cr> CREATE FUNCTION my_schema.my_multiply(long, long)
    ...  RETURNS long
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function my_multiply(a, b) { return a * b; }';
    CREATE OK, 1 row affected  (... sec)

This would overload our ``my_multiply`` function with more arguments::

    cr> CREATE FUNCTION my_schema.my_multiply(long, long, long)
    ...  RETURNS long
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function my_multiply(a, b, c) { return a * b * c; }';
    CREATE OK, 1 row affected  (... sec)

.. CAUTION::

    It is considered bad practice to create functions that have the same name
    as the CrateDB built-in functions!

.. NOTE::

    If you call a function without a schema name, CrateDB will look it up in
    the built-in functions first and only then in the user-defined functions
    with the schema of the current session (see
    :ref:`search_path <conf-session-search-path>`).

    **Therefore a built-in function with the same name as a user-defined
    function will hide the latter, even if it contains a different set of
    arguments!** However, such functions can still be called if the schema name
    is explicitly provided.

Determinism
===========

.. CAUTION::

    User-defined functions need to be deterministic, meaning that they must
    always return the same result value when called with the same argument
    values, because CrateDB might cache the returned values and reuse the value
    if the function is called multiple times with the same arguments.

``DROP FUNCTION``
=================

Functions can be dropped like this::

     cr> DROP FUNCTION doc.log10(long);
     DROP OK, 1 row affected  (... sec)

Adding ``IF EXISTS`` prevents from raising an error if the function doesn't
exist::

     cr> DROP FUNCTION IF EXISTS doc.log10(integer);
     DROP OK, 1 row affected  (... sec)

Optionally, argument names can be specified within the drop statement::

     cr> DROP FUNCTION IF EXISTS doc.calculate_distance(start_point geo_point, end_point geo_point);
     DROP OK, 1 row affected  (... sec)

Optionally, you can provide a schema::

     cr> DROP FUNCTION my_schema.log10(long);
     DROP OK, 1 row affected  (... sec)

Supported Languages
===================

CrateDB currently only supports the UDF language ``javascript``.

.. _udf_lang_js:

JavaScript
----------

The UDF language ``javascript`` supports the `ECMAScript 5.1`_ standard.

.. NOTE::

   The JavaScript language is an :ref:`enterprise feature
   <enterprise_features>`.

CrateDB uses the Java built-in JavaScript engine Nashorn_ to interpret and
execute functions written in JavaScript. The engine is initialized using the
``--no-java`` option which basically restricts all access to Java APIs from
within the JavaScript context. CrateDB's engine also does not allow
non-standard syntax extensions (``--no-syntax-extensions``).

**This, however, does not mean that JavaScript is securely sandboxed.**

Also, even though Nashorn runs ECMA-complient JavaScript, objects that are
normally accessible with a web browser (e.g. ``window``, ``console`` and so on)
are are not available.

.. CAUTION::

   The :ref:`udf_lang_js` language is an experimental feature and is disabled
   by default. You can enable the :ref:`conf-node-lang-js` via the configuration
   file.

Supported Types
...............

JavaScript functions can handle all CrateDB data types. However, for some
return types the function output must correspond to the certain format.

If a function requires ``geo_point`` as a return type, then the JavaScript
function must return a ``double array`` of size 2, ``WKT`` string or
``GeoJson`` object.

Here is an example of a JavaScript function returning a ``double array``::

    cr> CREATE FUNCTION rotate_point(point geo_point, angle float)
    ...  RETURNS geo_point
    ...  LANGUAGE JAVASCRIPT
    ...  AS 'function rotate_point(point, angle) {
    ...        var cos = Math.cos(angle);
    ...        var sin = Math.sin(angle);
    ...        var x = cos * point[0] - sin * point[1];
    ...        var y = sin * point[0] + cos * point[1];
    ...        return [x, y];
    ...      }';
    CREATE OK, 1 row affected  (... sec)

Below is an example of a JavaScript function returning a ``WKT`` string, which
will be cast to ``geo_point``::

     cr> CREATE FUNCTION symmetric_point(point geo_point)
     ...  RETURNS geo_point
     ...  LANGUAGE JAVASCRIPT
     ...  AS 'function symmetric_point (point, angle) {
     ...        var x = - point[0],
     ...            y = - point[1];
     ...        return "POINT (\" + x + \", \" + y +\")";
     ...      }';
     CREATE OK, 1 row affected  (... sec)

Similarly, if the function specifies the ``geo_shape`` return data type, then
the JavaScript function should return a ``GeoJson`` object or``WKT`` string::

     cr> CREATE FUNCTION line(start_point array(double), end_point array(double))
     ...  RETURNS object
     ...  LANGUAGE JAVASCRIPT
     ...  AS 'function line(start_point, end_point) {
     ...        return { "type": "LineString", "coordinates" : [start_point, end_point] };
     ...      }';
     CREATE OK, 1 row affected  (... sec)

.. NOTE::

   If the return value of the JavaScript function is ``undefined``, it is
   converted to ``NULL``.

Working with ``NUMBERS``
------------------------

The JavaScript engine Nashorn_ interprets numbers as ``java.lang.Double``,
``java.lang.Long``, or ``java.lang.Integer``, depending on the computation
performed. In most cases, this is not an issue, since the return type of the
JavaScript function will be cast to the return type specified in the ``CREATE
FUNCTION`` statement, although cast might result in a loss of precision.

However, when you try to cast ``DOUBLE`` to ``TIMESTAMP``, it will be
interpreted as UTC seconds and will result in a wrong value::

     cr> CREATE FUNCTION utc(long, long, long)
     ...  RETURNS TIMESTAMP
     ...  LANGUAGE JAVASCRIPT
     ...  AS 'function utc(year, month, day) {
     ...        return Date.UTC(year, month, day, 0, 0, 0);
     ...      }';
     CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('utc(1::long, 1::long, 1::long)')

::

    cr> SELECT date_format(utc(2016,04,6)) as epoque;
    +------------------------------+
    | epoque                       |
    +------------------------------+
    | 48314-07-22T00:00:00.000000Z |
    +------------------------------+
    SELECT 1 row in set (... sec)

.. hide:

    cr> DROP FUNCTION utc(long, long, long);
    DROP OK, 1 row affected  (... sec)

To avoid this behavior, the numeric value should be divided by 1000 before it
is returned::

     cr> CREATE FUNCTION utc(long, long, long)
     ...  RETURNS TIMESTAMP
     ...  LANGUAGE JAVASCRIPT
     ...  AS 'function utc(year, month, day) {
     ...        return Date.UTC(year, month, day, 0, 0, 0)/1000;
     ...      }';
     CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('utc(1::long, 1::long, 1::long)')

::

    cr> SELECT date_format(utc(2016,04,6)) as epoque;
    +-----------------------------+
    | epoque                      |
    +-----------------------------+
    | 2016-05-06T00:00:00.000000Z |
    +-----------------------------+
    SELECT 1 row in set (... sec)

.. hide:

    cr> DROP FUNCTION my_subtract_function(integer, integer);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION my_schema.my_multiply(integer, integer);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION my_schema.my_multiply(long, long, long);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION my_schema.my_multiply(long, long);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION rotate_point(point geo_point, angle float);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION symmetric_point(point geo_point);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION line(start_point array(double), end_point array(double));
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION utc(long, long, long);
    DROP OK, 1 row affected  (... sec)

.. _Nashorn: http://www.oracle.com/technetwork/articles/java/jf14-nashorn-2126515.html
.. _ECMAScript 5.1: https://www.ecma-international.org/ecma-262/5.1/
