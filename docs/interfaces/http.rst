.. highlight:: sh

.. _sql_http_endpoint:

=============
HTTP endpoint
=============

.. rubric:: Table of contents

.. contents::
   :local:

Introduction
============

CrateDB provides a HTTP Endpoint that can be used to submit SQL queries. The
endpoint is accessible under ``<servername:port>/_sql``.

SQL statements are sent to the ``_sql`` endpoint in ``json`` format, whereby
the statement is sent as value associated to the key ``stmt``.

.. SEEALSO::

    :ref:`dml`

A simple ``SELECT`` statement can be submitted like this::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' \
    ... -d '{"stmt":"select name, position from locations order by id limit 2"}'
    {
      "cols": [
        "name",
        "position"
      ],
      "rows": [
        [
          "North West Ripple",
          1
        ],
        [
          "Outer Eastern Rim",
          2
        ]
      ],
      "rowcount": 2,
      "duration": ...
    }

.. NOTE::

    We're using a simple command line invokation of ``curl`` here so you can
    see how to run this by hand in the terminal. For the rest of the examples
    in this document, we use `here documents`_ (i.e. ``EOF``) for multiline
    readability.

.. _parameter_substitution:

Parameter substitution
======================

In addition to the ``stmt`` key the request body may also contain an ``args``
key which can be used for SQL parameter substitution.

The SQL statement has to be changed to use placeholders where the values should
be inserted. Placeholders can either be numbered (in the form of ``$1``,
``$2``, etc.) or unnumbered using a question mark ``?``.

The placeholders will then be substituted with values from an array that is
expected under the ``args`` key::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' -d@- <<- EOF
    ... {
    ...   "stmt":
    ...     "select date,position from locations
    ...     where date <= \$1 and position < \$2 order by position",
    ...   "args": ["1979-10-12", 3]
    ... }
    ... EOF
    {
      "cols": [
        "date",
        "position"
      ],
      "rows": [
        [
          308534400000,
          1
        ],
        [
          308534400000,
          2
        ]
      ],
      "rowcount": 2,
      "duration": ...
    }

.. NOTE::

    In this example the placeholders start with an backslash due to shell
    escaping.

.. WARNING::

    Parameter substitution must not be used within subscript notation.

    For example, ``column[?]`` is not allowed.

The same query using question marks as placeholders looks like this::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' -d@- <<- EOF
    ... {
    ...   "stmt":
    ...     "select date,position from locations
    ...     where date <= ? and position < ? order by position",
    ...   "args": ["1979-10-12", 3]
    ... }
    ... EOF
    {
      "cols": [
        "date",
        "position"
      ],
      "rows": [
        [
          308534400000,
          1
        ],
        [
          308534400000,
          2
        ]
      ],
      "rowcount": 2,
      "duration": ...
    }

.. NOTE::

    With some queries the row count is not ascertainable. In this cases
    rowcount is ``-1``.

.. _http_default_schema:

Default schema
==============

It is possible to set a default schema while querying the CrateDB cluster via
``_sql`` end point. In such case the HTTP request should contain the
``Default-Schema`` header with the specified schema name::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' \
    ... -H 'Default-Schema: doc' -d@- <<- EOF
    ... {
    ...   "stmt":"select name, position from locations order by id limit 2"
    ... }
    ... EOF
    {
      "cols": [
        "name",
        "position"
      ],
      "rows": [
        [
          "North West Ripple",
          1
        ],
        [
          "Outer Eastern Rim",
          2
        ]
      ],
      "rowcount": 2,
      "duration": ...
    }

If the schema name is not specified in the header, the default ``doc`` schema
will be used instead.

Column types
============

CrateDB can respond a list ``col_types`` with the data type ID of every
responded column. This way one can know what exact data type a column is
holding.

In order to get the list of column data types, a ``types`` query parameter must
be passed to the request::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql?types' -d@- <<- EOF
    ... {
    ...   "stmt":
    ...     "select date, position from locations
    ...      where date <= \$1 and position < \$2 order by position",
    ...   "args": ["1979-10-12", 3]
    ... }
    ... EOF
    {
      "cols": [
        "date",
        "position"
      ],
      "col_types": [
        11,
        9
      ],
      "rows": [
        [
          308534400000,
          1
        ],
        [
          308534400000,
          2
        ]
      ],
      "rowcount": 2,
      "duration": ...
    }

The ``Array`` collection data type is displayed as a list where the first value
is the collection type and the second is the inner type. The inner type could
also be a collection.

Example of JSON representation of a column list of (String, Integer[])::

  "column_types": [ 4, [ 100, 9 ] ]

IDs of all currently available data types:

.. list-table::
   :widths: 8 30 62
   :header-rows: 1

   * - ID
     - Data Type
     - Format
   * - 0
     - Null
     - null
   * - 1
     - Not Supported
     -
   * - 2
     - :ref:`char <data-type-special>`
     - single byte
   * - 3
     - :ref:`boolean <data-type-boolean>`
     - `true` or `false`
   * - 4
     - :ref:`text <data-type-text>`
     - all unicode characters allowed
   * - 5
     - :ref:`ip <ip-type>`
     - '0:0:0:0:0:ffff:c0a8:64', '192.169.0.55'
   * - 6
     - :ref:`double precision <data-type-numeric>`
     - 15 decimal digits precision
   * - 7
     - real
     - 6 decimal digits precision
   * - 8
     - smallint
     - range -32768 to 32767
   * - 9
     - integer
     - range -2^31 to 2^31-1
   * - 10
     - bigint
     - range -2^63 to 2^63-1
   * - 11
     - :ref:`timestamp <timestamp_data_type>`
     - ``bigint`` e.g. 1591808274761
   * - 12
     - :ref:`object(dynamic|strict|ignored) <object_data_type>`
     - '{"key": "value"}', { key = 'value'}
   * - 13
     - :ref:`geo_point <geo_point_data_type>`
     - [lon_value::``double``, lat_value::``double``] e.g. [28.979999972507358,-57.33000000938773]
   * - 14
     - :ref:`geo_shape <geo_shape_data_type>`
     - object[] e.g. [{"coordinates":[[[100.0,0.0],[101.0,0.0],[101.0,1.0]]],"type":"Polygon"}]
   * - 15
     - Unchecked Object
     -
   * - 19
     - :ref:`regproc <oid_regproc>`
     -
   * - 20
     - :ref:`time with time zone <time-data-type>`
     - [``bigint``, ``integer``] e.g. [70652987666, 0]
   * - 21
     - :ref:`oidvector <oidvector_type>`
     - An array of numbers
   * - 100
     - :ref:`array <data-type-array>`
     - [``integer``, ``integer``] e.g. [100, 9] for a ``array(integer)``

.. _bulk_operations:

Bulk operations
===============

The REST endpoint allows to issue bulk operations which are executed as single
calls on the back-end site. It can be compared to `prepared statement`_.

A bulk operation can be expressed simply as an SQL statement.

Supported bulk SQL statements are:

 - Insert
 - Update
 - Delete

Instead of the ``args`` (:ref:`parameter_substitution`) key, use the key
``bulk_args``. This allows to specify a list of lists, containing all the
records which shall be processed. The inner lists need to match the specified
columns.

The bulk response contains a ``results`` array, with a rowcount for each bulk
operation. Those results are in the same order as the issued operations of the
bulk operation.

The following example describes how to issue an insert bulk operation and
insert three records at once::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' -d@- <<- EOF
    ... {
    ...   "stmt": "INSERT INTO locations (id, name, kind, description)
    ...           VALUES (?, ?, ?, ?)",
    ...   "bulk_args": [
    ...     [1337, "Earth", "Planet", "An awesome place to spend some time on."],
    ...     [1338, "Sun", "Star", "An extraordinarily hot place."],
    ...     [1339, "Titan", "Moon", "Titan, where it rains fossil fuels."]
    ...   ]
    ... }
    ... EOF
    {
      "cols": [],
      "duration": ...,
      "results": [
        {
          "rowcount": 1
        },
        {
          "rowcount": 1
        },
        {
          "rowcount": 1
        }
      ]
    }

Error handling
==============

Queries that are invalid or cannot be satisfied will result in an error
response. The response will contain an error code, an error message and in some
cases additional arguments that are specific to the error code.

Client libraries should use the error code to translate the error into an
appropriate exception::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' -d@- <<- EOF
    ... {
    ...   "stmt":"select name, position from foo.locations"
    ... }
    ... EOF
    {
      "error": {
        "message": "SchemaUnknownException[Schema 'foo' unknown]",
        "code": 4045
      }
    }

To get more insight into what exactly went wrong an additional ``error_trace``
GET parameter can be specified to return the stack trace::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql?error_trace=true' -d@- <<- EOF
    ... {
    ...   "stmt":"select name, position from foo.locations"
    ... }
    ... EOF
    {
      "error": {
        "message": "SchemaUnknownException[Schema 'foo' unknown]",
        "code": 4045
      },
      "error_trace": "..."
    }

.. NOTE::

    This parameter is intended for CrateDB developers or for users requesting
    support for CrateDB. Client libraries shouldn't make use of this option and
    not include the stacktrace.

Currently the defined error codes are:

====== =====================================================================
Code   Error
====== =====================================================================
4000   The statement contains an invalid syntax or unsupported SQL statement
------ ---------------------------------------------------------------------
4001   The statement contains an invalid analyzer definition.
------ ---------------------------------------------------------------------
4002   The name of the relation is invalid.
------ ---------------------------------------------------------------------
4003   Field type validation failed
------ ---------------------------------------------------------------------
4004   Possible feature not supported (yet)
------ ---------------------------------------------------------------------
4005   Alter table using a table alias is not supported.
------ ---------------------------------------------------------------------
4006   The used column alias is ambiguous.
------ ---------------------------------------------------------------------
4007   The operation is not supported on this relation, as it is not
       accessible.
------ ---------------------------------------------------------------------
4008   The name of the column is invalid.
------ ---------------------------------------------------------------------
4009   CrateDB License is expired.
------ ---------------------------------------------------------------------
4010   User is not authorized to perform the SQL statement.
------ ---------------------------------------------------------------------
4011   Missing privilege for user.
------ ---------------------------------------------------------------------
4031   Only read operations are allowed on this node.
------ ---------------------------------------------------------------------
4041   Unknown relation.
------ ---------------------------------------------------------------------
4042   Unknown analyzer.
------ ---------------------------------------------------------------------
4043   Unknown column.
------ ---------------------------------------------------------------------
4044   Unknown type.
------ ---------------------------------------------------------------------
4045   Unknown schema.
------ ---------------------------------------------------------------------
4046   Unknown Partition.
------ ---------------------------------------------------------------------
4047   Unknown Repository.
------ ---------------------------------------------------------------------
4048   Unknown Snapshot.
------ ---------------------------------------------------------------------
4049   Unknown user-defined function.
------ ---------------------------------------------------------------------
40410  Unknown user.
------ ---------------------------------------------------------------------
4091   A document with the same primary key exists already.
------ ---------------------------------------------------------------------
4092   A VersionConflict. Might be thrown if an attempt was made to update
       the same document concurrently.
------ ---------------------------------------------------------------------
4093   A relation with the same name exists already.
------ ---------------------------------------------------------------------
4094   The used table alias contains tables with different schema.
------ ---------------------------------------------------------------------
4095   A repository with the same name exists already.
------ ---------------------------------------------------------------------
4096   A snapshot with the same name already exists in the repository.
------ ---------------------------------------------------------------------
4097   A partition for the same values already exists in this table.
------ ---------------------------------------------------------------------
4098   A user-defined function with the same signature already exists.
------ ---------------------------------------------------------------------
4099   A user with the same name already exists.
------ ---------------------------------------------------------------------
5000   Unhandled server error.
------ ---------------------------------------------------------------------
5001   The execution of one or more tasks failed.
------ ---------------------------------------------------------------------
5002   One or more shards are not available.
------ ---------------------------------------------------------------------
5003   The query failed on one or more shards
------ ---------------------------------------------------------------------
5004   Creating a snapshot failed
------ ---------------------------------------------------------------------
5030   The query was killed by a ``kill`` statement
====== =====================================================================

Bulk errors
-----------

If a bulk operation fails, the resulting rowcount will be ``-2`` and the
resulting object may contain an ``error_message`` depending on the resulting
error::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X POST '127.0.0.1:4200/_sql' -d@- <<- EOF
    ... {
    ...   "stmt": "INSERT into locations (name, id) values (?,?)",
    ...   "bulk_args": [
    ...     ["Mars", 1341],
    ...     ["Sun", 1341]
    ...   ]
    ... }
    ... EOF
    {
      "cols": [],
      "duration": ...,
      "results": [
        {
          "rowcount": 1
        },
        {
          "rowcount": -2
        }
      ]
    }

.. NOTE::

   Every bulk operation will be executed, independent if one of the operation
   fails.

.. _prepared statement: https://en.wikipedia.org/wiki/Prepared_statement
.. _here documents: https://www.tldp.org/LDP/abs/html/here-docs.html
