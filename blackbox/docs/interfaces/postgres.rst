.. _postgres_wire_protocol:

========================
PostgreSQL Wire Protocol
========================

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

CrateDB contains support for the `PostgreSQL wire protocol v3`_.

If a node is started with postgres support enabled it will bind to port
``5432`` by default. To use a custom port, set the corresponding
:ref:`conf_ports` in the :ref:`Configuration <config>`.

However, even though connecting PostgreSQL tools and client libraries is
supported, the actual SQL statements have to be supported by CrateDB's SQL
dialect. A notable difference is that CrateDB doesn't support transactions,
which is why clients should generally enable ``autocommit``.

.. NOTE::

    In order to use ``setFetchSize`` in JDBC it is possible to set autocommit
    to false.

    The client will utilize the fetchSize on SELECT statements and only load up
    to fetchSize rows into memory.

    See `PostgreSQL JDBC Query docs
    <https://jdbc.postgresql.org/documentation/head/query.html>` for more
    information.

    Write operations will still behave as if autocommit was enabled and commit
    or rollback calls are ignored.

Server Compatibility and Implementation Status
==============================================

CrateDB emulates PostgreSQL server version ``9.5``.

Start-Up
--------

SSL Support
...........

SSL support is only available in the `Enterprise Edition`_. If enterprise is
disabled, all ``SSLRequests`` are answered with ``N``, indicating to the client
that SSL is not available.

.. SEEALSO::

  :ref:`admin_ssl`

Authentication
..............

If the `Enterprise Edition`_ is enabled, authentication methods can be
configured using :ref:`admin_hba`.

If the enterprise functionality is disabled, all "start up" requests are
answered with an ``AuthenticationOK`` response.

ParameterStatus
...............

After the authentication has succeeded the server has the possibility to send
multiple ``ParameterStatus`` messages to the client.
These are used to communicate information like ``server_version`` (emulates
PostgreSQL 9.5) or ``server_encoding``.

``CrateDB`` also sends a message containing the ``crate_version`` parameter.
This contains the current ``CrateDB`` version number.

This information is useful for clients to detect that they're connecting to
``CrateDB`` instead of a PostgreSQL instance.

Database selection
..................

Since CrateDB uses schemas instead of databases, the ``database`` parameter
sets the default schema name for future queries. If no schema is specified, the
schema ``doc`` will be used as default. Additionally, the only supported charset
is ``UTF8``.

Query Modes
-----------

Simple Query
............

The `Simple Query`_ protocol mode is fully implemented.

Extended Query
..............

The `Extended Query`_ protocol mode is implemented with the following limitations:

- The ``ParameterDescription`` message works for the most common use cases
  except for DDL statements.

- To optimize the execution of bulk operations the execution of statements is
  delayed until the ``Sync`` message is received

Copy Operations
---------------

CrateDB does not support the ``COPY`` sub-protocol.

Function Call
-------------

The function call sub-protocol is not supported since it's a legacy feature.

Canceling Requests
------------------

Operations can be cancelled using the ``KILL`` statement, hence the
``CancelRequest`` message  is unsupported. Consequently, the server won't send
a ``BackendKeyData`` message during connection initialization.

``pg_type``
-----------

Some clients require the ``pg_catalog.pg_type`` in order to be able to stream
arrays or other non-primitive types.

For compatibility reasons there is a trimmed down ``pg_type`` table available in
CrateDB::

    cr> select * from pg_catalog.pg_type order by oid;
    +------+-------------+----------+---------+-------------+---------+
    |  oid | typbasetype | typdelim | typelem | typname     | typtype |
    +------+-------------+----------+---------+-------------+---------+
    |   16 |           0 | ,        |       0 | bool        | b       |
    |   18 |           0 | ,        |       0 | char        | b       |
    |   20 |           0 | ,        |       0 | int8        | b       |
    |   21 |           0 | ,        |       0 | int2        | b       |
    |   23 |           0 | ,        |       0 | int4        | b       |
    |  114 |           0 | ,        |       0 | json        | b       |
    |  199 |           0 | ,        |     114 | _json       | b       |
    |  700 |           0 | ,        |       0 | float4      | b       |
    |  701 |           0 | ,        |       0 | float8      | b       |
    | 1000 |           0 | ,        |      16 | _bool       | b       |
    | 1002 |           0 | ,        |      18 | _char       | b       |
    | 1005 |           0 | ,        |      21 | _int2       | b       |
    | 1007 |           0 | ,        |      23 | _int4       | b       |
    | 1015 |           0 | ,        |    1043 | _varchar    | b       |
    | 1016 |           0 | ,        |      20 | _int8       | b       |
    | 1021 |           0 | ,        |     700 | _float4     | b       |
    | 1022 |           0 | ,        |     701 | _float8     | b       |
    | 1043 |           0 | ,        |       0 | varchar     | b       |
    | 1184 |           0 | ,        |       0 | timestampz  | b       |
    | 1185 |           0 | ,        |    1184 | _timestampz | b       |
    +------+-------------+----------+---------+-------------+---------+
    SELECT 20 rows in set (... sec)

Show Transaction Isolation
--------------------------

For compatibility with JDBC the ``SHOW TRANSACTION ISOLATION LEVEL`` statement
is implemented::

    cr> show transaction isolation level;
    +-----------------------+
    | transaction_isolation |
    +-----------------------+
    | read uncommitted      |
    +-----------------------+
    SHOW 1 row in set (... sec)

Begin Statement
---------------

For compatibility with the lib/pq driver, the full PostgreSQL syntax of the
``BEGIN`` statement is implemented, for example::

    cr> BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED,
    ...                   READ ONLY,
    ...                   NOT DEFERRABLE;
    BEGIN OK, 0 rows affected  (... sec)

The ``BEGIN`` statement and any of its parameters are ignored.

Client Compatibility
====================

JDBC
----

`pgjdbc`_ JDBC drivers version ``9.4.1209`` and above are compatible.

Limitations
...........

- *reflection* methods like ``conn.getMetaData().getTables(...)`` won't work
  since the required tables are unavailable in CrateDB.

  As a workaround it's possible to use ``SHOW TABLES`` or query the
  ``information_schema`` tables manually using ``SELECT`` statements.

- ``OBJECT`` and ``GEO_SHAPE`` columns can be streamed as ``JSON`` but require
  `pgjdbc`_ version ``9.4.1210`` or newer.

- Multidimensional arrays will be streamed as ``JSON`` encoded string to avoid
  a protocol limitation where all sub-arrays are required to have the same
  length.

- The behavior of ``PreparedStatement.executeBatch`` in error cases depends on
  in which stage an error occurs: A ``BatchUpdateException`` is thrown if no
  processing has been done yet, whereas single operations failing after the
  processing started are indicated by an ``EXECUTE_FAILED`` (-3) return value.

- Transaction limitations as described above.

- Having ``escape processing`` enabled could prevent the usage of :ref:`Object
  Literals <data-type-object-literals>` in case an object key's starting
  character clashes with a JDBC escape keyword (see also `JDBC escape syntax
  <https://docs.oracle.com/javadb/10.10.1.2/ref/rrefjdbc1020262.html>`_).
  Currently, disabling ``escape processing`` will remedy this, but prevent the
  `Extended Query`_ API from working due to a `bug
  <https://github.com/pgjdbc/pgjdbc/issues/653>`_ at `pgjdbc`_.

Connection Failover and Load Balancing
......................................

Connection failover and load balancing is supported as described here:
`PostgreSQL JDBC connection failover`_.

.. NOTE::

   It is not recommended to use the **targetServerType** parameter since
   CrateDB has no concept of master-replica nodes.

Implementation Differences
==========================

The PostgreSQL Wire Protocol makes it easy to use many PostgreSQL compatible
tools and libraries directly with CrateDB. However, many of these tools assume
that they are talking to PostgreSQL specifically, and thus rely on SQL
extensions and idioms that are unique to PostgreSQL. Because of this, some
tools or libraries may not work with other SQL databases such as CrateDB.

CrateDB's SQL query engine enables real-time search & aggregations for online
analytic processing (OLAP) and business intelligence (BI) with the benefit of
the ability to scale horizontally. The use-cases of CrateDB are different than
those of PostgreSQL, as CrateDB's specialized storage schema and query
execution engine address different requirements (see `High Level
Architecture`_).

The listed features below cover the main differences in implementation and
dialect between CrateDB and PostgreSQL. A detailed comparison between CrateDB's
SQL dialect and standard SQL is defined in
:ref:`crate_standard_sql`.

``COPY``
--------

CrateDB does not support the distinct sub-protocol that is used to serve
``COPY`` operations and provides another implementation for transferring bulk
data using the :ref:`copy_from` and :ref:`copy_to` statements.

Objects
-------

The definition of structured values by using ``JSON`` types, *composite types*
or ``HSTORE`` are not supported. CrateDB alternatively allows the definition of
nested documents (of type :ref:`object_data_type`) that store fieldscontaining
any CrateDB supported data type, including nested object types.

Type Casts
----------

CrateDB accepts the :ref:`type_conversion` syntax for conversion of one data
type to another (see `Value Expressions`_).

Arrays
------

Declaration of Arrays
.....................

The definition of an array by writing its values as a literal constant with the
syntax of  ``'{ val1 delim val2 delim ... }'`` is not supported.

While multidimensional arrays in PostgreSQL must have matching extends for each
dimension, CrateDB allows different length nested arrays as this example
shows::

    cr> select [[1,2,3],[1,2]] from sys.cluster;
    +---------------------+
    | [[1, 2, 3], [1, 2]] |
    +---------------------+
    | [[1, 2, 3], [1, 2]] |
    +---------------------+
    SELECT 1 row in set (... sec)

Accessing Arrays
................

Fetching arbitrary rectangular slices of an array using
``lower-bound:upper-bound`` expression (see `Arrays`_) in the array subscript
is not supported.

Text Search Functions and Operators
-----------------------------------

The functions and operators provided by PostgreSQL for full-text search (see
`PostgreSQL Fulltext Search`_) are not compatible with those provided by
CrateDB. For more information about the built-in full-text search in CrateDB
refer to :ref:`sql_dql_fulltext_search`.

If you are missing features, functions or dialect improvements and have a great
use case for it, let us know on `Github`_. We're always improving and extending
CrateDB, and we love to hear feedback.

Expression Evaluation
---------------------

Unlike PostgreSQL, expressions are not evaluated if the query results in 0 rows
either because of the table is empty or by a not matching where clause.

.. _Arrays: https://www.postgresql.org/docs/current/static/arrays.html
.. _Enterprise Edition: https://crate.io/enterprise-edition/
.. _Simple Query: https://www.postgresql.org/docs/current/static/protocol-flow.html#id-1.10.5.7.4
.. _Extended Query: https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
.. _Github: https://github.com/crate/crate
.. _High Level Architecture: https://crate.io/overview/high-level-architecture
.. _pgjdbc: https://github.com/pgjdbc/pgjdbc
.. _PostgreSQL Fulltext Search: https://www.postgresql.org/docs/current/static/functions-textsearch.html
.. _PostgreSQL JDBC connection failover: https://jdbc.postgresql.org/documentation/head/connect.html#connection-failover
.. _PostgreSQL wire protocol v3: https://www.postgresql.org/docs/current/static/protocol.html
.. _Value Expressions: https://www.postgresql.org/docs/current/static/sql-expressions.html
