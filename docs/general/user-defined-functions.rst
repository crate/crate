.. _user-defined-functions:

======================
User-defined functions
======================

.. rubric:: Table of contents

.. contents::
   :local:


.. _udf-create-replace:

``CREATE OR REPLACE``
=====================

CrateDB supports user-defined :ref:`functions <gloss-function>`. See
:ref:`ref-create-function` for a full syntax description.

``CREATE FUNCTION`` defines a new function::

    cr> CREATE FUNCTION my_subtract_function(integer, integer)
    ... RETURNS integer
    ... LANGUAGE JAVASCRIPT
    ... AS 'function my_subtract_function(a, b) { return a - b; }';
    CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('my_subtract_function(1::integer, 1::integer)')

::

    cr> SELECT doc.my_subtract_function(3, 1) AS col;
    +-----+
    | col |
    +-----+
    |   2 |
    +-----+
    SELECT 1 row in set (... sec)

``CREATE OR REPLACE FUNCTION`` will either create a new function or replace
an existing function definition::

    cr> CREATE OR REPLACE FUNCTION log10(bigint)
    ... RETURNS double precision
    ... LANGUAGE JAVASCRIPT
    ... AS 'function log10(a) {return Math.log(a)/Math.log(10); }';
    CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('log10(1::bigint)')

::

    cr> SELECT doc.log10(10) AS col;
    +-----+
    | col |
    +-----+
    | 1.0 |
    +-----+
    SELECT 1 row in set (... sec)

It is possible to use named function arguments in the function signature. For
example, the ``calculate_distance`` function signature has two ``geo_point``
arguments named ``start`` and ``end``::

    cr> CREATE OR REPLACE FUNCTION calculate_distance("start" geo_point, "end" geo_point)
    ... RETURNS real
    ... LANGUAGE JAVASCRIPT
    ... AS 'function calculate_distance(start, end) {
    ...       return Math.sqrt(
    ...            Math.pow(end[0] - start[0], 2),
    ...            Math.pow(end[1] - start[1], 2));
    ...    }';
    CREATE OK, 1 row affected  (... sec)


.. NOTE::

    Argument names are used for query documentation purposes only. You cannot
    reference arguments by name in the function body.

Optionally, a schema-qualified function name can be defined. If you omit the
schema, the current session schema is used::

    cr> CREATE OR REPLACE FUNCTION my_schema.log10(bigint)
    ... RETURNS double precision
    ... LANGUAGE JAVASCRIPT
    ... AS 'function log10(a) { return Math.log(a)/Math.log(10); }';
    CREATE OK, 1 row affected  (... sec)

.. NOTE::

   In order to improve the PostgreSQL server compatibility CrateDB allows the
   creation of user defined functions against the :ref:`postgres-pg_catalog`
   schema. However, the creation of user defined functions against the
   read-only :ref:`system-information` and :ref:`information_schema` schemas is
   prohibited.


.. _udf-supported-types:

Supported types
===============

Function arguments and return values can be any of the supported :ref:`data
types <data-types>`. The values passed into a function must strictly
correspond to the specified argument data types.

.. NOTE::

    The value returned by the function will be casted to the return type
    provided in the definition if required. An exception will be thrown if the
    cast is not successful.


.. _udf-overloading:

Overloading
===========

Within a specific schema, you can overload functions by defining functions
with the same name but a different set of arguments::

    cr> CREATE FUNCTION my_schema.my_multiply(integer, integer)
    ... RETURNS integer
    ... LANGUAGE JAVASCRIPT
    ... AS 'function my_multiply(a, b) { return a * b; }';
    CREATE OK, 1 row affected  (... sec)

This would overload the ``my_multiply`` function with different argument
types::

    cr> CREATE FUNCTION my_schema.my_multiply(bigint, bigint)
    ... RETURNS bigint
    ... LANGUAGE JAVASCRIPT
    ... AS 'function my_multiply(a, b) { return a * b; }';
    CREATE OK, 1 row affected  (... sec)

This would overload the ``my_multiply`` function with more arguments::

    cr> CREATE FUNCTION my_schema.my_multiply(bigint, bigint, bigint)
    ... RETURNS bigint
    ... LANGUAGE JAVASCRIPT
    ... AS 'function my_multiply(a, b, c) { return a * b * c; }';
    CREATE OK, 1 row affected  (... sec)

.. CAUTION::

    It is considered bad practice to create functions that have the same name
    as the CrateDB built-in functions!

.. NOTE::

    If you call a function without a schema name, CrateDB will look it up in
    the built-in functions first and only then in the user-defined functions
    available in the :ref:`search_path <conf-session-search-path>`.

    **Therefore a built-in function with the same name as a user-defined
    function will hide the latter, even if it contains a different set of
    arguments!** However, such functions can still be called if the schema name
    is explicitly provided.

.. _udf-determinism:

Determinism
===========

.. CAUTION::

    User-defined functions need to be deterministic, meaning that they must
    always return the same result value when called with the same argument
    values, because CrateDB might cache the returned values and reuse the value
    if the function is called multiple times with the same arguments.


.. _udf-drop-function:

``DROP FUNCTION``
=================

Functions can be dropped like this::

     cr> DROP FUNCTION doc.log10(bigint);
     DROP OK, 1 row affected  (... sec)

Adding ``IF EXISTS`` prevents from raising an error if the function doesn't
exist::

     cr> DROP FUNCTION IF EXISTS doc.log10(integer);
     DROP OK, 1 row affected  (... sec)

Optionally, argument names can be specified within the drop statement::

     cr> DROP FUNCTION IF EXISTS doc.calculate_distance(start_point geo_point, end_point geo_point);
     DROP OK, 1 row affected  (... sec)

Optionally, you can provide a schema::

     cr> DROP FUNCTION my_schema.log10(bigint);
     DROP OK, 1 row affected  (... sec)


.. _udf-supported-languages:

Supported languages
===================

Currently, CrateDB only supports JavaScript for user-defined functions.


.. _udf-js:

JavaScript
----------

The user defined function JavaScript is compatible with the `ECMAScript 2019`_
specification.

CrateDB uses the `GraalVM JavaScript`_ engine as a JavaScript (ECMAScript)
language execution runtime. The `GraalVM JavaScript`_ engine is a Java
application that works on the stock Java Virtual Machines (VMs). The
interoperability between Java code (host language) and JavaScript user-defined
functions (guest language) is guaranteed by the `GraalVM Polyglot API`_.

Please note: CrateDB does not use the GraalVM JIT compiler as optimizing
compiler. However, the `stock host Java VM JIT compilers`_ can JIT-compile,
optimize, and execute the GraalVM JavaScript codebase to a certain extent.

The execution context for guest JavaScript is created with restricted
privileges to allow for the safe execution of less trusted guest language
code. The guest language application context for each user-defined function
is created with default access modifiers, so any access to managed resources
is denied. The only exception is the host language interoperability
configuration which explicitly allows access to Java lists and arrays. Please
refer to `GraalVM Security Guide`_ for more detailed information.

Also, even though user-defined functions implemented with ECMA-compliant
JavaScript, objects that are normally accessible with a web browser
(e.g. ``window``, ``console``, and so on) are not available.


.. _udf-js-supported-types:

JavaScript supported types
..........................

JavaScript functions can handle all CrateDB data types. However, for some
return types the function output must correspond to the certain format.

If a function requires ``geo_point`` as a return type, then the JavaScript
function must return a ``double precision`` array of size 2, ``WKT`` string or
``GeoJson`` object.

Here is an example of a JavaScript function returning a ``double array``::

    cr> CREATE FUNCTION rotate_point(point geo_point, angle real)
    ... RETURNS geo_point
    ... LANGUAGE JAVASCRIPT
    ... AS 'function rotate_point(point, angle) {
    ...       var cos = Math.cos(angle);
    ...       var sin = Math.sin(angle);
    ...       var x = cos * point[0] - sin * point[1];
    ...       var y = sin * point[0] + cos * point[1];
    ...       return [x, y];
    ...    }';
    CREATE OK, 1 row affected  (... sec)

Below is an example of a JavaScript function returning a ``WKT`` string, which
will be cast to ``geo_point``::

     cr> CREATE FUNCTION symmetric_point(point geo_point)
     ... RETURNS geo_point
     ... LANGUAGE JAVASCRIPT
     ... AS 'function symmetric_point (point, angle) {
     ...       var x = - point[0],
     ...           y = - point[1];
     ...       return "POINT (\" + x + \", \" + y +\")";
     ...    }';
     CREATE OK, 1 row affected  (... sec)

Similarly, if the function specifies the ``geo_shape`` return data type, then
the JavaScript function should return a ``GeoJson`` object or ``WKT`` string::

     cr> CREATE FUNCTION line("start" array(double precision), "end" array(double precision))
     ... RETURNS object
     ... LANGUAGE JAVASCRIPT
     ... AS 'function line(start, end) {
     ...        return { "type": "LineString", "coordinates" : [start_point, end_point] };
     ...    }';
     CREATE OK, 1 row affected  (... sec)

.. NOTE::

   If the return value of the JavaScript function is ``undefined``, it is
   converted to ``NULL``.


.. _udf-js-numbers:

Working with ``NUMBERS``
........................

The JavaScript engine interprets numbers as ``java.lang.Double``,
``java.lang.Long``, or ``java.lang.Integer``, depending on the computation
performed. In most cases, this is not an issue, since the return type of the
JavaScript function will be cast to the return type specified in the ``CREATE
FUNCTION`` statement, although cast might result in a loss of precision.

However, when you try to cast ``DOUBLE PRECISION`` to
``TIMESTAMP WITH TIME ZONE``, it will be interpreted as UTC seconds and will
result in a wrong value::

     cr> CREATE FUNCTION utc(bigint, bigint, bigint)
     ... RETURNS TIMESTAMP WITH TIME ZONE
     ... LANGUAGE JAVASCRIPT
     ... AS 'function utc(year, month, day) {
     ...       return Date.UTC(year, month, day, 0, 0, 0);
     ...    }';
     CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('utc(1::bigint, 1::bigint, 1::bigint)')

::

    cr> SELECT date_format(utc(2016,04,6)) as epoque;
    +------------------------------+
    | epoque                       |
    +------------------------------+
    | 48314-07-22T00:00:00.000000Z |
    +------------------------------+
    SELECT 1 row in set (... sec)

.. hide:

    cr> DROP FUNCTION utc(bigint, bigint, bigint);
    DROP OK, 1 row affected  (... sec)

To avoid this behavior, the numeric value should be divided by 1000 before it
is returned::

     cr> CREATE FUNCTION utc(bigint, bigint, bigint)
     ... RETURNS TIMESTAMP WITH TIME ZONE
     ... LANGUAGE JAVASCRIPT
     ... AS 'function utc(year, month, day) {
     ...       return Date.UTC(year, month, day, 0, 0, 0)/1000;
     ...    }';
     CREATE OK, 1 row affected  (... sec)

.. hide:

    cr> _wait_for_function('utc(1::bigint, 1::bigint, 1::bigint)')

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

    cr> DROP FUNCTION my_schema.my_multiply(bigint, bigint, bigint);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION my_schema.my_multiply(bigint, bigint);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION rotate_point(point geo_point, angle real);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION symmetric_point(point geo_point);
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION line(start_point array(double precision), end_point array(double precision));
    DROP OK, 1 row affected  (... sec)

    cr> DROP FUNCTION utc(bigint, bigint, bigint);
    DROP OK, 1 row affected  (... sec)


.. _udf-js-array-methods:

Working with ``Array`` methods
..............................

The JavaScript ``Array`` object has a number of prototype methods you can
use, such as `join`_, `map`_, `sort`_, `slice`_, `reduce`_, and so on.

Normally, you can call these methods directly from an ``Array`` object, like
so:

.. code-block:: js

    function array_join(a, b) {
        return a.join(b);
    }

However, when writing JavaScript for use with CrateDB, you must explicitly use
the prototype method:

.. code-block:: js

    function array_join(a, b) {
        return Array.prototype.join.call(a, b);
    }

You must do it like this because arguments are not passed as ``Array`` objects,
and so do not have the associated prototype methods available. Arguments are instead
passed as array-like objects.


.. _ECMAScript 2019: https://262.ecma-international.org/10.0/index.html
.. _GraalVM JavaScript: https://www.graalvm.org/reference-manual/js/
.. _GraalVM Polyglot API: https://www.graalvm.org/reference-manual/embed-languages/
.. _GraalVM Security Guide: https://www.graalvm.org/security-guide/
.. _join: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/join
.. _map: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/map
.. _reduce: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/reduce
.. _slice: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/slice
.. _sort: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/sort
.. _stock host Java VM JIT compilers: https://www.graalvm.org/reference-manual/js/RunOnJDK/
