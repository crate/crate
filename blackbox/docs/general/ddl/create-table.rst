.. _sql_ddl_create:

===============
Creating Tables
===============

.. rubric:: Table of Contents

.. contents::
   :local:

Introduction
============

To create a table use the ``CREATE TABLE`` command. You must at least specify a
name for the table and names and types of the columns.

See :ref:`data-types` for information about the supported data types.

Let's create a simple table with two columns of type ``integer`` and
``string``::

    cr> create table my_table (
    ...   first_column integer,
    ...   second_column string
    ... );
    CREATE OK, 1 row affected (... sec)

A table can be removed by using the ``DROP TABLE`` command::

    cr> drop table my_table;
    DROP OK, 1 row affected (... sec)

The ``DROP TABLE`` command takes the optional clause ``IF EXISTS`` which
prevents the generation of an error if the specified table does not exist::

    cr> drop table if exists my_table;
    DROP OK, 0 rows affected (... sec)

.. _sql_ddl_schemas:

Schemas
=======

Tables can be created in different schemas. These are created implicitly on
table creation and cannot be created explicitly. If a schema did not exist yet,
it will be created.

You can create a table called ``my_table`` in a schema called ``my_schema``
schema like so::

    cr> create table my_schema.my_table (
    ...   pk int primary key,
    ...   label string,
    ...   position geo_point
    ... );
    CREATE OK, 1 row affected (... sec)

We can confirm this by looking up this table in the
:ref:`information_schema.tables <information_schema_tables>` table::

    cr> select table_schema, table_name from information_schema.tables
    ... where table_name='my_table';
    +--------------+------------+
    | table_schema | table_name |
    +--------------+------------+
    | my_schema    | my_table   |
    +--------------+------------+
    SELECT 1 row in set (... sec)

The following schema names are reserved and may not be used:

 - blob
 - information_schema
 - sys

.. TIP::

   Schemas are primarily namespaces for tables.

   In the standard edition of CrateDB, there is no notion of access control and
   everybody can see and manipulate tables in every schema.

   In the :ref:`Enterprise Edition <enterprise_features>` of CrateDB, you can
   use :ref:`privileges <administration-privileges>` to control access to
   schemas.

A user created schema exists as long as there are tables with the same schema
name. If the last table with that schema is dropped, the schema is gone (except
for the ``blob`` and ``doc`` schema)::

    cr> drop table my_schema.my_table ;
    DROP OK, 1 row affected (... sec)

Every table that is created without an explicit schema name, will be created in
the ``doc`` schema::

    cr> create table my_doc_table (
    ...   a_column byte,
    ...   another_one geo_point
    ... );
    CREATE OK, 1 row affected (... sec)

::

    cr> select table_schema, table_name from information_schema.tables
    ... where table_name='my_doc_table';
    +--------------+--------------+
    | table_schema | table_name   |
    +--------------+--------------+
    | doc          | my_doc_table |
    +--------------+--------------+
    SELECT 1 row in set (... sec)

.. Hidden: drop tables::

    cr> drop table my_doc_table;
    DROP OK, 1 row affected (... sec)

.. _sql_ddl_naming_restrictions:

Naming Restrictions
===================

Table, schema and column identifiers cannot have the same names as reserved key
words. Please refer to the :ref:`sql_lexical` section for more information
about naming.

Additionally, table and schema names are restricted in terms of characters and
length. They:

  - may not contain one of the following characters: ``\ / * ? " < > |
    <whitespace> , # .``

  - may not contain upper case letters

  - may not *start* with an underscore: ``_``

  - should not exceed 255 bytes when encoded with ``utf-8`` (this
    limit applies on the optionally schema-qualified table name)

Column names are restricted in terms of patterns:

  - Columns are not allowed to contain a dot (``.``), since this conflicts
    with internal path definitions.

  - Columns that conflict with the naming scheme of
    :ref:`virtual system columns <sql_administration_system_columns>` are
    restricted.

  - Character sequences that conform to the
    :ref:`subscript notation <sql_dql_object_arrays>` (e.g. ``col['id']``) are
    not allowed.
