.. _interface-postgresql:

========================
PostgreSQL wire protocol
========================

CrateDB supports the `PostgreSQL wire protocol v3`_.

If a node is started with PostgreSQL wire protocol support enabled it will bind
to port ``5432`` by default. To use a custom port, set the corresponding
:ref:`conf_ports` in the :ref:`Configuration <config>`.

However, even though connecting PostgreSQL tools and client libraries is
supported, the actual SQL statements have to be supported by CrateDB's SQL
dialect. A notable difference is that CrateDB doesn't support transactions,
which is why clients should generally enable ``autocommit``.

.. NOTE::

    In order to use ``setFetchSize`` in JDBC it is possible to set auto commit
    to false.

    The client will utilize the fetchSize on SELECT statements and only load up
    to fetchSize rows into memory.

    See the `PostgreSQL JDBC Query docs`_ for more information.

    Write operations will still behave as if auto commit was enabled and commit
    or rollback calls are ignored.

.. rubric:: Table of contents

.. contents::
   :local:


.. _postgres-server-compat:

Server compatibility
====================

CrateDB emulates PostgreSQL server version ``14``.


.. _postgres-start-up:

Start-up
--------


.. _postgres-ssl:

SSL Support
'''''''''''

SSL can be configured using :ref:`admin_ssl`.


.. _postgres-auth:

Authentication
''''''''''''''

Authentication methods can be configured using :ref:`admin_hba`.


.. _postgres-parameterstatus:

ParameterStatus
'''''''''''''''

After the authentication succeeded, the server has the possibility to send
multiple ``ParameterStatus`` messages to the client. These are used to
communicate information like ``server_version`` (emulates PostgreSQL 9.5) or
``server_encoding``.

``CrateDB`` also sends a message containing the ``crate_version`` parameter.
This contains the current ``CrateDB`` version number.

This information is useful for clients to detect that they're connecting to
``CrateDB`` instead of a PostgreSQL instance.


.. _postgres-db-selection:

Database selection
''''''''''''''''''

Since CrateDB uses schemas instead of databases, the ``database`` parameter
sets the default schema name for future queries. If no schema is specified, the
schema ``doc`` will be used as default. Additionally, the only supported
charset is ``UTF8``.


.. _postgres-query-modes:

Query modes
-----------


.. _postgres-query-modes-simple:

Simple query
''''''''''''

The `PostgreSQL simple query`_ protocol mode is fully implemented.


.. _postgres-query-modes-extended:

Extended query
''''''''''''''

The `PostgreSQL extended query`_ protocol mode is implemented with the
following limitations:

- The ``ParameterDescription`` message works for the most common use cases
  except for DDL statements.

- To optimize the execution of bulk operations the execution of statements is
  delayed until the ``Sync`` message is received


.. _postgres-copy-na:

Copy operations
---------------

CrateDB does not support the ``COPY`` sub-protocol, see also
:ref:`postgres-copy`.


.. _postgres-fn-call:

Function call
-------------

The :ref:`function call <sql-function-call>` sub-protocol is not supported
since it's a legacy feature.


.. _postgres-cancel-reqs:

Canceling requests
------------------

`PostgreSQL cancelling requests`_ is fully implemented.


.. _postgres-pg_catalog:

``pg_catalog``
--------------

For improved compatibility, the ``pg_catalog`` schema is implemented containing
following tables:

 - `pg_am`_
 - `pg_attrdef <pgsql_pg_attrdef_>`__
 - `pg_attribute <pgsql_pg_attribute_>`__
 - `pg_class <pgsql_pg_class_>`__
 - `pg_constraint <pgsql_pg_constraint_>`__
 - `pg_cursors <pgsql_pg_cursors_>`__
 - `pg_database <pgsql_pg_database_>`__
 - `pg_depend`_
 - `pg_description`_
 - `pg_enum`_
 - `pg_event_trigger`_
 - `pg_index <pgsql_pg_index_>`__
 - `pg_indexes <pgsql_pg_indexes_>`__
 - `pg_locks <pgsql_pg_locks_>`__
 - `pg_namespace <pgsql_pg_namespace_>`__
 - `pg_proc <pgsql_pg_proc_>`__
 - `pg_publication <pgsql_pg_publication_>`__
 - `pg_publication_tables <pgsql_pg_publication_tables_>`__
 - `pg_range`_
 - `pg_roles`_
 - `pg_settings <pgsql_pg_settings_>`__
 - `pg_shdescription`_
 - `pg_stats`_
 - `pg_subscription <pgsql_pg_subscription_>`__
 - `pg_subscription_rel <pgsql_pg_subscription_rel_>`__
 - `pg_tables`_
 - `pg_tablespace`_
 - `pg_type`_
 - `pg_views`_


.. _postgres-pg_type:

``pg_type``
'''''''''''

Some clients require the ``pg_catalog.pg_type`` in order to be able to stream
arrays or other non-primitive types.

For compatibility reasons, there is a trimmed down `pg_type <pgsql_pg_type_>`__
table available in CrateDB::

    cr> SELECT oid, typname, typarray, typelem, typlen, typtype, typcategory
    ... FROM pg_catalog.pg_type
    ... ORDER BY oid;
    +------+--------------+----------+---------+--------+---------+-------------+
    |  oid | typname      | typarray | typelem | typlen | typtype | typcategory |
    +------+--------------+----------+---------+--------+---------+-------------+
    |   16 | bool         |     1000 |       0 |      1 | b       | N           |
    |   18 | char         |     1002 |       0 |      1 | b       | S           |
    |   19 | name         |       -1 |       0 |     64 | b       | S           |
    |   20 | int8         |     1016 |       0 |      8 | b       | N           |
    |   21 | int2         |     1005 |       0 |      2 | b       | N           |
    |   23 | int4         |     1007 |       0 |      4 | b       | N           |
    |   24 | regproc      |     1008 |       0 |      4 | b       | N           |
    |   25 | text         |     1009 |       0 |     -1 | b       | S           |
    |   26 | oid          |     1028 |       0 |      4 | b       | N           |
    |   30 | oidvector    |     1013 |      26 |     -1 | b       | A           |
    |  114 | json         |      199 |       0 |     -1 | b       | U           |
    |  199 | _json        |        0 |     114 |     -1 | b       | A           |
    |  600 | point        |     1017 |       0 |     16 | b       | G           |
    |  700 | float4       |     1021 |       0 |      4 | b       | N           |
    |  701 | float8       |     1022 |       0 |      8 | b       | N           |
    | 1000 | _bool        |        0 |      16 |     -1 | b       | A           |
    | 1002 | _char        |        0 |      18 |     -1 | b       | A           |
    | 1005 | _int2        |        0 |      21 |     -1 | b       | A           |
    | 1007 | _int4        |        0 |      23 |     -1 | b       | A           |
    | 1008 | _regproc     |        0 |      24 |     -1 | b       | A           |
    | 1009 | _text        |        0 |      25 |     -1 | b       | A           |
    | 1014 | _bpchar      |        0 |    1042 |     -1 | b       | A           |
    | 1015 | _varchar     |        0 |    1043 |     -1 | b       | A           |
    | 1016 | _int8        |        0 |      20 |     -1 | b       | A           |
    | 1017 | _point       |        0 |     600 |     -1 | b       | A           |
    | 1021 | _float4      |        0 |     700 |     -1 | b       | A           |
    | 1022 | _float8      |        0 |     701 |     -1 | b       | A           |
    | 1042 | bpchar       |     1014 |       0 |     -1 | b       | S           |
    | 1043 | varchar      |     1015 |       0 |     -1 | b       | S           |
    | 1082 | date         |     1182 |       0 |      8 | b       | D           |
    | 1114 | timestamp    |     1115 |       0 |      8 | b       | D           |
    | 1115 | _timestamp   |        0 |    1114 |     -1 | b       | A           |
    | 1182 | _date        |        0 |    1082 |     -1 | b       | A           |
    | 1184 | timestamptz  |     1185 |       0 |      8 | b       | D           |
    | 1185 | _timestamptz |        0 |    1184 |     -1 | b       | A           |
    | 1186 | interval     |     1187 |       0 |     16 | b       | T           |
    | 1187 | _interval    |        0 |    1186 |     -1 | b       | A           |
    | 1231 | _numeric     |        0 |    1700 |     -1 | b       | A           |
    | 1266 | timetz       |     1270 |       0 |     12 | b       | D           |
    | 1270 | _timetz      |        0 |    1266 |     -1 | b       | A           |
    | 1560 | bit          |     1561 |       0 |     -1 | b       | V           |
    | 1561 | _bit         |        0 |    1560 |     -1 | b       | A           |
    | 1700 | numeric      |     1231 |       0 |     -1 | b       | N           |
    | 2205 | regclass     |     2210 |       0 |      4 | b       | N           |
    | 2210 | _regclass    |        0 |    2205 |     -1 | b       | A           |
    | 2249 | record       |     2287 |       0 |     -1 | p       | P           |
    | 2276 | any          |        0 |       0 |      4 | p       | P           |
    | 2277 | anyarray     |        0 |    2276 |     -1 | p       | P           |
    | 2287 | _record      |        0 |    2249 |     -1 | p       | A           |
    +------+--------------+----------+---------+--------+---------+-------------+
    SELECT 49 rows in set (... sec)

.. NOTE::

   This is just a snapshot of the table.

   Check table :ref:`information_schema.columns <information_schema_columns>`
   to get information for all supported columns.


.. _postgres-pg_type-oid:

OID types
.........

*Object Identifiers* (OIDs) are used internally by PostgreSQL as primary keys
for various system tables.

CrateDB supports the :ref:`oid <type-oid>` type and the following aliases:

+-------------------+----------------------+-------------+-------------+
| Name              | Reference            | Description | Example     |
+===================+======================+=============+=============+
| :ref:`regproc     | `pg_proc             | A function  | ``sum``     |
| <type-regproc>`   | <pgsql_pg_proc_>`__  | name        |             |
+-------------------+----------------------+-------------+-------------+
| :ref:`regclass    | `pg_class            | A relation  | ``pg_type`` |
| <type-regclass>`  | <pgsql_pg_class_>`__ | name        |             |
+-------------------+----------------------+-------------+-------------+

CrateDB also supports the :ref:`oidvector <type-oidvector>` type.

.. NOTE::

    Casting a :ref:`string <data-types-character-data>` or an :ref:`integer
    <type-numeric>` to the ``regproc`` type does not result in a function
    lookup (as it does with PostgreSQL).

    Instead:

    .. rst-class:: open

    - Casting a string to the ``regproc`` type results in an object of the
      ``regproc`` type with a name equal to the string value and an ``oid``
      equal to an integer hash of the string.

    - Casting an integer to the ``regproc`` type results in an object of the
      ``regproc`` type with a name equal to the string representation of the
      integer and an ``oid`` equal to the integer value.

    Consult the :ref:`CrateDB data types reference
    <data-types-postgres-internal>` for more information about each OID type
    (including additional type casting behaviour).


.. _postgres-show-trans-isolation:

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


.. _postgres-begin-start-comit:

``BEGIN``, ``START``, and ``COMMIT`` statements
-----------------------------------------------

For compatibility with clients that use the PostgresSQL wire protocol (e.g.,
the Golang lib/pq and pgx drivers), CrateDB will accept the :ref:`BEGIN
<ref-begin>`, :ref:`COMMIT <ref-commit>`, and :ref:`START TRANSACTION
<sql-start-transaction>` statements. For example::

    cr> BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED,
    ...                   READ ONLY,
    ...                   NOT DEFERRABLE;
    BEGIN OK, 0 rows affected  (... sec)

    cr> COMMIT
    COMMIT OK, 0 rows affected  (... sec)

CrateDB will silently ignore the ``COMMIT``, ``BEGIN``, and ``START
TRANSACTION`` statements and all respective parameters.


.. _postgres-client-compat:

Client compatibility
====================


.. _postgres-client-jdbc:

JDBC
----

`pgjdbc`_ JDBC drivers version ``9.4.1209`` and above are compatible.


.. _postgres-client-jdbc-limit:

Limitations
'''''''''''

- *Reflection* methods like ``conn.getMetaData().getTables(...)`` won't work
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
  Literals <data-types-object-literals>` in case an object key's starting
  character clashes with a JDBC escape keyword (see also `JDBC escape syntax
  <https://docs.oracle.com/javadb/10.10.1.2/ref/rrefjdbc1020262.html>`_).
  Disabling ``escape processing`` will remedy this appropriately for `pgjdbc`_
  version >= ``9.4.1212``.


.. _postgres-client-jdbc-conn:

Connection failover and load balancing
''''''''''''''''''''''''''''''''''''''

Connection failover and load balancing is supported as described here:
`PostgreSQL JDBC connection failover`_.

.. NOTE::

   It is not recommended to use the **targetServerType** parameter since
   CrateDB has no concept of master-replica nodes.


.. _postgres-implementation:

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
execution engine addresses different needs (see :ref:`Clustering
<concept-clustering>`).

The features listed below cover the main differences in implementation and
dialect between CrateDB and PostgreSQL. A detailed comparison between CrateDB's
SQL dialect and standard SQL is outlined in :ref:`appendix-compatibility`.


.. _postgres-copy:

Copy operations
---------------

CrateDB does not support the distinct sub-protocol that is used to serve
``COPY`` operations and provides another implementation for transferring bulk
data using the :ref:`sql-copy-from` and :ref:`sql-copy-to` statements.


.. _postgres-types:

Data types
----------


.. _postgres-date-times:

Dates and times
'''''''''''''''

At the moment, CrateDB does not support ``TIME`` without a time zone.

Additionally, CrateDB does not support the ``INTERVAL`` input units
``MILLENNIUM``, ``CENTURY``, ``DECADE``, ``MILLISECOND``, or ``MICROSECOND``.


.. _postgres-objects:

Objects
'''''''

The definition of structured values by using ``JSON`` types, *composite types*
or ``HSTORE`` are not supported. CrateDB alternatively allows the definition of
nested documents (of type :ref:`type-object`) that store fields containing any
CrateDB supported data type, including nested object types.


.. _postgres-arrays:

Arrays
''''''


.. _postgres-arrays-declare:

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



.. _postgres-type-casts:

Type casts
''''''''''

CrateDB accepts the :ref:`data-types-casting` syntax for conversion of one data
type to another.

.. SEEALSO::

    `PostgreSQL value expressions`_

    :ref:`CrateDB value expressions <sql-value-expressions>`


.. _postgres-search:

Text search functions and operators
-----------------------------------

The :ref:`functions <gloss-function>` and :ref:`operators <gloss-operator>`
provided by PostgreSQL for :ref:`full-text search <sql_dql_fulltext_search>`
(see `PostgreSQL fulltext Search`_) are not compatible with those provided by
CrateDB.

If you are missing features, functions or dialect improvements and have a great
use case for it, let us know on `GitHub`_. We're always improving and extending
CrateDB and we love to hear feedback.



.. _GitHub: https://github.com/crate/crate
.. _pg_am: https://www.postgresql.org/docs/14/catalog-pg-am.html
.. _pg_description: https://www.postgresql.org/docs/14/catalog-pg-description.html
.. _pg_enum: https://www.postgresql.org/docs/14/catalog-pg-enum.html
.. _pg_range: https://www.postgresql.org/docs/14/catalog-pg-range.html
.. _pg_roles: https://www.postgresql.org/docs/14/view-pg-roles.html
.. _pg_tables: https://www.postgresql.org/docs/14/view-pg-tables.html
.. _pg_tablespace: https://www.postgresql.org/docs/14/catalog-pg-tablespace.html
.. _pg_views: https://www.postgresql.org/docs/14/view-pg-views.html
.. _pg_shdescription: https://www.postgresql.org/docs/14/catalog-pg-shdescription.html
.. _pg_stats: https://www.postgresql.org/docs/14/view-pg-stats.html
.. _pg_event_trigger: https://www.postgresql.org/docs/current/catalog-pg-event-trigger.html
.. _pg_depend: https://www.postgresql.org/docs/current/catalog-pg-depend.html
.. _pgjdbc: https://github.com/pgjdbc/pgjdbc
.. _pgsql_pg_attrdef: https://www.postgresql.org/docs/14/static/catalog-pg-attrdef.html
.. _pgsql_pg_attribute: https://www.postgresql.org/docs/14/static/catalog-pg-attribute.html
.. _pgsql_pg_class: https://www.postgresql.org/docs/14/static/catalog-pg-class.html
.. _pgsql_pg_constraint: https://www.postgresql.org/docs/14/static/catalog-pg-constraint.html
.. _pgsql_pg_cursors: https://www.postgresql.org/docs/15/view-pg-cursors.html
.. _pgsql_pg_database: https://www.postgresql.org/docs/14/static/catalog-pg-database.html
.. _pgsql_pg_index: https://www.postgresql.org/docs/14/static/catalog-pg-index.html
.. _pgsql_pg_indexes: https://www.postgresql.org/docs/14/view-pg-indexes.html
.. _pgsql_pg_locks: https://www.postgresql.org/docs/14/view-pg-locks.html
.. _pgsql_pg_namespace: https://www.postgresql.org/docs/14/static/catalog-pg-namespace.html
.. _pgsql_pg_proc: https://www.postgresql.org/docs/14/static/catalog-pg-proc.html
.. _pgsql_pg_publication: https://www.postgresql.org/docs/14/catalog-pg-publication.html
.. _pgsql_pg_publication_tables: https://www.postgresql.org/docs/14/view-pg-publication-tables.html
.. _pgsql_pg_subscription: https://www.postgresql.org/docs/14/catalog-pg-subscription.html
.. _pgsql_pg_subscription_rel: https://www.postgresql.org/docs/14/catalog-pg-subscription-rel.html
.. _pgsql_pg_settings: https://www.postgresql.org/docs/14/view-pg-settings.html
.. _pgsql_pg_type: https://www.postgresql.org/docs/14/static/catalog-pg-type.html
.. _PostgreSQL Arrays: https://www.postgresql.org/docs/14/static/arrays.html
.. _PostgreSQL extended query: https://www.postgresql.org/docs/14/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
.. _PostgreSQL Fulltext Search: https://www.postgresql.org/docs/14/static/functions-textsearch.html
.. _PostgreSQL JDBC connection failover: https://jdbc.postgresql.org/documentation/use/#connection-fail-over
.. _PostgreSQL JDBC Query docs: https://jdbc.postgresql.org/documentation/query
.. _PostgreSQL simple query: https://www.postgresql.org/docs/14/static/protocol-flow.html#id-1.10.5.7.4
.. _PostgreSQL value expressions: https://www.postgresql.org/docs/14/static/sql-expressions.html
.. _PostgreSQL wire protocol v3: https://www.postgresql.org/docs/14/static/protocol.html
.. _PostgreSQL cancelling requests: https://www.postgresql.org/docs/14/protocol-flow.html#id-1.10.5.7.10
