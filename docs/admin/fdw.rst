.. _administration-fdw:

=====================
Foreign Data Wrappers
=====================

.. rubric:: Table of contents

.. contents::
   :local:


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

    cr> explain select * from summits where mountain like 'H%';
    +--------------------------------------------------------------------+
    | QUERY PLAN                                                         |
    +--------------------------------------------------------------------+
    | Filter[(mountain LIKE 'H%')] (rows=0)                              |
    |   └ ForeignCollect[doc.summits | [mountain] | true] (rows=unknown) |
    +--------------------------------------------------------------------+

Compare this to the following output, where the query became part of the
``ForeignCollect`` node, indicating that it evaluates within the foreign
system::


    cr> explain select * from summits where mountain = 'Monte Verena';
    +---------------------------------------------------------------------------------------+
    | QUERY PLAN                                                                            |
    +---------------------------------------------------------------------------------------+
    | ForeignCollect[doc.summits | [mountain] | (mountain = 'Monte Verena')] (rows=unknown) |
    +---------------------------------------------------------------------------------------+

.. note::

   Only `DQL` (Data Query Language) statements are supported on foreign tables.


``jdbc``
========

The ``jdbc`` foreign data wrapper allows to connect to a foreign database via
``JDBC``. Bundled JDBC drivers include:

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
   - :ref:`ref-create-foreign-table`
   - :ref:`ref-create-user-mapping`
