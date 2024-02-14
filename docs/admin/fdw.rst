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

  A JDBC connection string. Defaults to ``'jdbc:postgresql://127.0.0.1:5432/'``.

  You should avoid specifying user and password information in the URL, and
  instead make use of the :ref:`ref-create-user-mapping` feature.

.. note::

  The default of ``127.0.0.1:5432`` only works for the ``crate`` super user. Any
  other user is by default not allowed to connect to instances running on the
  same host as CrateDB.

  This is a security measure to prevent users from by-passing
  :ref:`admin_hba` restrictions. See :ref:`fdw.allow_local`.

Example::

  CREATE SERVER my_postgresql FOREIGN DATA WRAPPER jdbc
  OPTIONS (url 'jdbc:postgresql://example.com:5432/');


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

  CREATE USER MAPPING USER SERVER my_postgresql OPTIONS ("user" 'trillian', password 'secret');


.. seealso::

   - :ref:`ref-create-server`
   - :ref:`ref-create-foreign-table`
   - :ref:`ref-create-user-mapping`
