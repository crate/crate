.. _postgres_wire_protocol:

========================
PostgreSQL wire protocol
========================

.. rubric:: Table of contents

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

Server compatibility and implementation status
==============================================

CrateDB emulates PostgreSQL server version ``10.5``.

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

Simple query
............

The `Simple Query`_ protocol mode is fully implemented.

Extended query
..............

The `Extended Query`_ protocol mode is implemented with the following limitations:

- The ``ParameterDescription`` message works for the most common use cases
  except for DDL statements.

- To optimize the execution of bulk operations the execution of statements is
  delayed until the ``Sync`` message is received

Copy operations
---------------

CrateDB does not support the ``COPY`` sub-protocol.

Function call
-------------

The function call sub-protocol is not supported since it's a legacy feature.

Canceling requests
------------------

Operations can be cancelled using the ``KILL`` statement, hence the
``CancelRequest`` message  is unsupported. Consequently, the server won't send
a ``BackendKeyData`` message during connection initialization.

.. _postgres_pg_catalog:

``pg_catalog``
--------------

For improved compatibility, the ``pg_catalog`` schema is implemented containing
following tables:

 - `pg_type`_
 - `pg_database <pgsql_pg_database_>`__
 - `pg_class <pgsql_pg_class_>`__
 - `pg_proc <pgsql_pg_proc_>`__
 - `pg_namespace <pgsql_pg_namespace_>`__
 - `pg_attribute <pgsql_pg_attribute_>`__
 - `pg_attrdef <pgsql_pg_attrdef_>`__
 - `pg_index <pgsql_pg_index_>`__
 - `pg_constraint <pgsql_pg_constraint_>`__
 - `pg_settings <pgsql_pg_settings_>`__
 - `pg_description`_
 - `pg_range`_
 - `pg_enum`_


.. _postgres_pg_type:

``pg_type``
...........

Some clients require the ``pg_catalog.pg_type`` in order to be able to stream
arrays or other non-primitive types.

For compatibility reasons, there is a trimmed down `pg_type <pgsql_pg_type_>`__
table available in CrateDB::

    cr> SELECT oid, typname, typarray, typelem, typlen, typtype, typcategory
    ... FROM pg_catalog.pg_type
    ... ORDER BY oid;
    +------+------------------------------+----------+---------+--------+---------+-------------+
    |  oid | typname                      | typarray | typelem | typlen | typtype | typcategory |
    +------+------------------------------+----------+---------+--------+---------+-------------+
    |   16 | bool                         |     1000 |       0 |      1 | b       | N           |
    |   18 | char                         |     1002 |       0 |      1 | b       | S           |
    |   19 | name                         |       -1 |       0 |     64 | b       | S           |
    |   20 | int8                         |     1016 |       0 |      8 | b       | N           |
    |   21 | int2                         |     1005 |       0 |      2 | b       | N           |
    |   23 | int4                         |     1007 |       0 |      4 | b       | N           |
    |  114 | json                         |      199 |       0 |     -1 | b       | U           |
    |  199 | _json                        |        0 |     114 |     -1 | b       | A           |
    |  600 | point                        |     1017 |       0 |     16 | b       | G           |
    |  700 | float4                       |     1021 |       0 |      4 | b       | N           |
    |  701 | float8                       |     1022 |       0 |      8 | b       | N           |
    | 1000 | _bool                        |        0 |      16 |     -1 | b       | A           |
    | 1002 | _char                        |        0 |      18 |     -1 | b       | A           |
    | 1005 | _int2                        |        0 |      21 |     -1 | b       | A           |
    | 1007 | _int4                        |        0 |      23 |     -1 | b       | A           |
    | 1015 | _varchar                     |        0 |    1043 |     -1 | b       | A           |
    | 1016 | _int8                        |        0 |      20 |     -1 | b       | A           |
    | 1017 | _point                       |        0 |     600 |     -1 | b       | A           |
    | 1021 | _float4                      |        0 |     700 |     -1 | b       | A           |
    | 1022 | _float8                      |        0 |     701 |     -1 | b       | A           |
    | 1043 | varchar                      |     1015 |       0 |     -1 | b       | S           |
    | 1114 | timestamp without time zone  |     1115 |       0 |      8 | b       | D           |
    | 1115 | _timestamp without time zone |        0 |    1114 |     -1 | b       | A           |
    | 1184 | timestamptz                  |     1185 |       0 |      8 | b       | D           |
    | 1185 | _timestamptz                 |        0 |    1184 |     -1 | b       | A           |
    | 1186 | interval                     |     1187 |       0 |     16 | b       | T           |
    | 1187 | _interval                    |        0 |    1186 |     -1 | b       | A           |
    | 1266 | time with time zone          |     1270 |       0 |     12 | b       | D           |
    | 1270 | _time with time zone         |        0 |    1266 |     -1 | b       | A           |
    | 2249 | record                       |     2287 |       0 |     -1 | p       | P           |
    | 2276 | any                          |        0 |       0 |      4 | p       | P           |
    | 2277 | anyarray                     |        0 |    2276 |     -1 | p       | P           |
    | 2287 | _record                      |        0 |    2249 |     -1 | p       | A           |
    +------+------------------------------+----------+---------+--------+---------+-------------+
    SELECT 33 rows in set (... sec)

.. NOTE::

   This is just a snapshot of the table.
   Check table :ref:`information_schema.columns <information_schema_columns>`
   to get information for all supported columns.

Show transaction isolation
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

BEGIN/COMMIT statements
-----------------------

For compatibility with clients that use the Postgres wire protocol, such as the
Golang lib/pq and pgx drivers, the full PostgreSQL syntax of the
:ref:`BEGIN <ref-begin>` and :ref:`COMMIT <ref-commit>` statements is
implemented, for example::

    cr> BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED,
    ...                   READ ONLY,
    ...                   NOT DEFERRABLE;
    BEGIN OK, 0 rows affected  (... sec)

    cr> COMMIT
    COMMIT OK, 0 rows affected  (... sec)

Since CrateDB does not support transactions, both the ``COMMIT`` and ``BEGIN``
statement and any of its parameters are ignored.

Client compatibility
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

Connection failover and load balancing
......................................

Connection failover and load balancing is supported as described here:
`PostgreSQL JDBC connection failover`_.

.. NOTE::

   It is not recommended to use the **targetServerType** parameter since
   CrateDB has no concept of master-replica nodes.

Implementation differences
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

Type casts
----------

CrateDB accepts the :ref:`type_conversion` syntax for conversion of one data
type to another (see `Value Expressions`_).

Arrays
------

Declaration of arrays
.....................

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

Accessing arrays
................

Fetching arbitrary rectangular slices of an array using
``lower-bound:upper-bound`` expression (see `Arrays`_) in the array subscript
is not supported.

Text search functions and operators
-----------------------------------

The functions and operators provided by PostgreSQL for full-text search (see
`PostgreSQL Fulltext Search`_) are not compatible with those provided by
CrateDB. For more information about the built-in full-text search in CrateDB
refer to :ref:`sql_dql_fulltext_search`.

If you are missing features, functions or dialect improvements and have a great
use case for it, let us know on `Github`_. We're always improving and extending
CrateDB, and we love to hear feedback.

Expression evaluation
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
.. _pgsql_pg_type: https://www.postgresql.org/docs/10/static/catalog-pg-type.html
.. _pgsql_pg_class: https://www.postgresql.org/docs/10/static/catalog-pg-class.html
.. _pgsql_pg_proc: https://www.postgresql.org/docs/10/static/catalog-pg-proc.html
.. _pgsql_pg_namespace: https://www.postgresql.org/docs/10/static/catalog-pg-namespace.html
.. _pgsql_pg_attrdef: https://www.postgresql.org/docs/10/static/catalog-pg-attrdef.html
.. _pgsql_pg_attribute: https://www.postgresql.org/docs/10/static/catalog-pg-attribute.html
.. _pgsql_pg_index: https://www.postgresql.org/docs/10/static/catalog-pg-index.html
.. _pgsql_pg_constraint: https://www.postgresql.org/docs/10/static/catalog-pg-constraint.html
.. _pgsql_pg_database: https://www.postgresql.org/docs/10/static/catalog-pg-database.html
.. _pgsql_pg_settings: https://www.postgresql.org/docs/10/view-pg-settings.html
.. _pg_description: https://www.postgresql.org/docs/10/catalog-pg-description.html
.. _pg_range: https://www.postgresql.org/docs/10/catalog-pg-range.html
.. _pg_enum: https://www.postgresql.org/docs/10/catalog-pg-enum.html
