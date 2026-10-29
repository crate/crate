.. _administration-fdw:

=====================
Foreign Data Wrappers
=====================

Foreign data wrappers allow you to make data in foreign systems available as
tables within CrateDB. You can then query these foreign tables like regular user
tables.

For this to work, you'll need several parts:

- A foreign data wrapper implementation, which takes care of managing the
  connection to the remote system and implements the actual data retrieval.
  CrateDB contains a ``jdbc`` data wrapper implementation.

- A :ref:`ref-create-server` definition. This gives your foreign system a name
  and provides information like the host name or port. The concrete options
  depend on the used foreign data wrapper. For example, for ``jdbc`` there's a
  ``url`` option that defines the JDBC connection URL.

- A :ref:`ref-create-foreign-table` definition. This statement is used to create
  the table and define the schema of the foreign data. The statement does *not*
  validate the used schema. You must make sure to use compatible data types,
  otherwise queries on the table will fail at runtime.

- Optionally one or more user mappings, created with
  :ref:`ref-create-user-mapping`. A user mapping allows you to map from a
  CrateDB user to a foreign system user. If no user mapping exists, CrateDB will
  try to connect with the current user.


Query clauses like ``GROUP BY``, ``HAVING``, ``LIMIT`` or ``ORDER BY`` are
executed within CrateDB, not within the foreign system. ``WHERE`` clauses can in
some circumstances be pushed to the foreign system, but that depends on the
concrete foreign data wrapper implementation. You can check if this is the case
by using the :ref:`ref-explain` statement.

For example, in the following explain output there is a dedicated ``Filter``
node, indicating that the filter is executed within CrateDB::

    > explain select * from summits where mountain like 'H%';
    +--------------------------------------------------------------------+
    | QUERY PLAN                                                         |
    +--------------------------------------------------------------------+
    | Filter[(mountain LIKE 'H%')] (rows=0)                              |
    |   └ ForeignCollect[doc.summits | [mountain] | true] (rows=unknown) |
    +--------------------------------------------------------------------+

Compare this to the following output, where the query became part of the
``ForeignCollect`` node, indicating that it evaluates within the foreign
system::


    > explain select * from summits where mountain = 'Monte Verena';
    +---------------------------------------------------------------------------------------+
    | QUERY PLAN                                                                            |
    +---------------------------------------------------------------------------------------+
    | ForeignCollect[doc.summits | [mountain] | (mountain = 'Monte Verena')] (rows=unknown) |
    +---------------------------------------------------------------------------------------+

.. note::

   Only `DQL` (Data Query Language) statements are supported on foreign tables.


.. _administration-fdw-jdbc:

``jdbc``
========

The ``jdbc`` foreign data wrapper allows to connect to a foreign database via
``JDBC``. Bundled JDBC drivers include:

.. _administration-fdw-jdbc-psql:

- PostgreSQL


``CREATE SERVER OPTIONS``
-------------------------

The JDBC foreign data wrapper supports the following ``OPTIONS`` for use with
:ref:`ref-create-server`:

:url:

  A JDBC connection string. This option is required.

  You should avoid specifying user and password information in the URL, and
  instead make use of the :ref:`ref-create-user-mapping` feature.

Example::

  CREATE SERVER my_postgresql FOREIGN DATA WRAPPER jdbc
  OPTIONS (url 'jdbc:postgresql://example.com:5432/');

.. note::

  By default only the ``crate`` user can use server definitions that connect to
  localhost. Other users are not allowed to connect to instances running on the
  same host as CrateDB. This is a security measure to prevent users from
  by-passing :ref:`admin_hba` restrictions. See :ref:`fdw.allow_local`.


``CREATE FOREIGN TABLE OPTIONS``
--------------------------------

The JDBC foreign data wrapper supports the following ``OPTIONS`` for use with
:ref:`ref-create-foreign-table`:

:schema_name:

  The schema name used when accessing a table in the foreign system. If not
  specified this defaults to the schema name of the table created within
  CrateDB.

  Use this if the names between CrateDB and the foreign system are different.

:table_name:

  The table name used when accessing a table in the foreign system. If not
  specified this defaults to the table name of the table within CrateDB.

  Use this if the names between CrateDB and the foreign system are different.

Example::

  CREATE FOREIGN TABLE doc.remote_documents (name text) SERVER my_postgresql
  OPTIONS (schema_name 'public', table_name 'documents');


``CREATE USER MAPPING OPTIONS``
-------------------------------


The JDBC foreign data wrapper supports the following ``OPTIONS`` for use with
:ref:`ref-create-user-mapping`:

:user:

  The name of the user in the foreign system.

:password:

  The password for the user in the foreign system.


Example::

  CREATE USER MAPPING FOR USER SERVER my_postgresql OPTIONS ("user" 'trillian', password 'secret');


.. seealso::

   - :ref:`ref-create-server`
   - :ref:`ref-alter-server`
   - :ref:`ref-create-foreign-table`
   - :ref:`ref-create-user-mapping`

.. _administration-fdw-parquet:

``parquet``
===========

The ``parquet`` foreign data wrapper allows CrateDB to read local parquet
files. Only the ``crate`` superuser is allowed to do this.
The ``parquet`` foreign data wrapper does not support writing data to parquet files.

Usage::

  cr> CREATE SERVER my_parquet FOREIGN DATA WRAPPER parquet
  ... OPTIONS (local_files ['testdata/departments.parquet']);
  CREATE OK, 1 row affected (... sec)

  cr> CREATE FOREIGN TABLE departments (manager_id bigint, name text, location bigint, id bigint) SERVER my_parquet;
  CREATE OK, 1 row affected (... sec)

  cr> SELECT * FROM departments;
  +------------+-----------------+----------+------+
  | manager_id | name            | location |   id |
  +------------+-----------------+----------+------+
  |        501 | Administration  |    17000 | 4001 |
  |        511 | Marketing       |    18000 | 4002 |
  |        518 | Purchasing      |    17000 | 4003 |
  |        517 | Human Resources |    24000 | 4004 |
  |        121 | Shipping        |    15000 | 4005 |
  |        502 | IT              |    14000 | 4006 |
  +------------+-----------------+----------+------+
  SELECT 6 rows in set (... sec)

.. hide:

  cr> DROP SERVER my_parquet CASCADE;
  DROP OK, 1 row affected (... sec)

.. note::

   In order to query :ref:`Object column with dynamic column policy <type-object-columns-dynamic>`,
   :ref:`conf-session-error_on_unknown_object_key` needs to be set to `false` or
   else it will lead to an error


``DEFINING THE FOREIGN TABLE``
------------------------------

The schema for the foreign table is defined according to CrateDB's
:ref:`data-types` while the data stored in the parquet file format is stored
according to parquet's specification. The table below explains the expected
mapping between the CrateDB data type defined in the foreign table schema and
the data type stored in parquet.
If there is a mismatch that cannot be automatically resolved
internally by CrateDB, an error will be raised at runtime. For example, if the foreign
table schema declares a column ``x`` as ``TEXT`` but ``x`` is stored in parquet
as ``INT32``, an error will be raised when trying to read the data.

The table below provides the expected mappings between CrateDB. If a CrateDB
type or a parquet type is not listed in the table below, it is not supported in
the parquet foreign data wrapper.

.. list-table::
    :header-rows: 1
    :widths: 20 20
    :align: left

    * - CrateDB Type
      - Parquet Type
    * - ``BOOLEAN``
      - ``BOOLEAN``
    * - ``VARCHAR(n)`` and ``TEXT``
      - ``STRING``
    * - ``INTEGER`` and ``SMALLINT``
      - ``INT32``
    * - ``BIGINT``
      - ``INT64``
    * - ``REAL``
      - ``FLOAT``
    * - ``DOUBLE PRECISION``
      - ``DOUBLE``
    * - ``NUMERIC(precision, scale)``
      - ``DECIMAL``
    * - ``TIMESTAMP WITH TIME ZONE``
      - ``TIMESTAMP WITH TIME ZONE``
    * - ``TIMESTAMP WITHOUT TIME ZONE``
      - ``TIMESTAMP``
    * - ``DATE``
      - ``DATE``
    * - ``BIT(n)``
      - ``STRING``
    * - ``IP``
      - ``INT64`` or ``STRING``
    * - ``OBJECT (DYNAMIC)`` and ``OBJECT (IGNORED)``
      - ``JSON``
    * - ``OBJECT (STRICT)``
      - ``GROUP``
    * - ``ARRAY``
      - ``LIST``
    * - ``UUID``
      - ``UUID``



``CREATE SERVER OPTIONS``
-------------------------

The parquet foreign data wrapper supports the following ``OPTIONS`` for use with
:ref:`ref-create-server`:

:local_files:

  A list of absolute paths to local parquet files. This is required. The files must be
  present on all nodes.

  You can provide multiple files, but they all must have the same schema.
  Otherwise an error will be raised when executing the query.
