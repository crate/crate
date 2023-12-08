.. highlight:: psql

.. _sql-restore-snapshot:

====================
``RESTORE SNAPSHOT``
====================

Restore a snapshot into the cluster.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-restore-snapshot-synopsis:

Synopsis
========

::

    RESTORE SNAPSHOT repository_name.snapshot_name
    { ALL |
      METADATA |
      TABLE table_ident [ PARTITION (partition_column = value [, ...])] [, ...] |
      data_section [, ...] }
    [ WITH (restore_parameter [= value], [, ...]) ]

where ``data_section``::

   {  TABLES |
      VIEWS |
      USERS |      -- Deprecated, use USERMANAGEMENT instead
      PRIVILEGES | -- Deprecated, use USERMANAGEMENT instead
      USERMANAGEMENT |
      ANALYZERS |
      UDFS }

.. _sql-restore-snapshot-description:

Description
===========

Restore one or more tables, partitions, or metadata from an existing snapshot
into the cluster. The snapshot must be given as fully qualified reference with
``repository_name`` and ``snapshot_name``.

To restore everything, use the ``ALL`` keyword.

Single tables (or table partitions) can be restored by using ``TABLE`` together
with a ``table_ident`` and a optional partition reference given the
``partition_column`` values.

It is possible to restore all tables using the ``TABLES`` keyword. This will
restore all tables but will not restore metadata.

To restore only the metadata (including views, users, roles, privileges,
analyzers, user-defined-functions, and all cluster settings), instead use the
``METADATA`` keyword.

A single metadata group can be restored by using the related ``data_section``
keyword.

Additionally, multiple ``data_section`` keywords can be used to restore
multiple concrete sections at once.

To cancel a restore operation simply drop the tables that are being restored.

.. CAUTION::

   If you try to restore a table that already exists, CrateDB will return an
   error. However, if you try to restore metadata or cluster settings that
   already exist, they will be overwritten.

.. _sql-restore-snapshot-parameters:

Parameters
==========

:repository_name:
  The name of the repository of the snapshot to restore as ident.

:snapshot_name:
  The name of the snapshot as ident.

:table_ident:
  The name (optionally schema-qualified) of an existing table that is to be
  restored from the snapshot.

:data_section:
  The section name of the data to be restored. Multiple sections can be
  selected.  A section cannot be combined with the ``ALL``, ``METADATA``, or
  ``TABLE`` keywords.

.. _sql-restore-snapshot-clauses:

Clauses
=======


.. _sql-restore-snapshot-partition:

``PARTITION``
-------------

.. EDITORIAL NOTE
   ##############

   Multiple files (in this directory) use the same standard text for
   documenting the ``PARTITION`` clause. (Minor verb changes are made to
   accomodate the specifics of the parent statement.)

   For consistency, if you make changes here, please be sure to make a
   corresponding change to the other files.

If the table is :ref:`partitioned <partitioned-tables>`, the optional
``PARTITION`` clause can be used to restore a snapshot from one partition
exclusively.

::

    [ PARTITION ( partition_column = value [, ...] ) ]

:partition_column:
  One of the column names used for table partitioning

:value:
  The respective column value.

All :ref:`partition columns <gloss-partition-column>` (specified by the
:ref:`sql-create-table-partitioned-by` clause) must be listed inside the
parentheses along with their respective values using the ``partition_column =
value`` syntax (separated by commas).

Because each partition corresponds to a unique set of :ref:`partition column
<gloss-partition-column>` row values, this clause uniquely identifies a single
partition to restore.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.


.. _sql-restore-snapshot-with:

``WITH``
--------

::

    [ WITH (restore_parameter [= value], [, ...]) ]

The following configuration parameters can be used to modify how the snapshot
is restored to the cluster:

:ignore_unavailable:
  (Default ``false``) Per default the restore command fails if a table
  is given that does not exist in the snapshot. If set to ``true`` those
  missing tables are ignored.

:wait_for_completion:
  (Default: ``false``) By default the request returns once the restore
  operation started. If set to ``true`` the request returns after all
  selected tables from the snapshot are restored or an error occurred.
  In order to monitor the restore operation the * :ref:`sys.shards
  <sys-shards>` table can be queried.

:schema_rename_pattern:
  (Default ``(.+)``) Regular expression matching schemas of restored tables.
  Used to restore table into a different schema. Capture groups ``()`` can be
  used to reuse portions of the table schema and then used in
  ``schema_rename_replacement``. Default value matches the entire schema name.

:schema_rename_replacement:
  (Default ``$1``) Replacement pattern used to restore table into a different
  schema. Can include groups, captured in ``schema_rename_pattern``. By default
  no replacement is happening and tables are restored into their original
  schemas.

  Example: ``prefix_$1`` combined with default ``schema_rename_pattern`` adds
  'prefix' to all restored table schemas.

  Example: ``target`` combined with default ``schema_rename_pattern``
  restores all tables into the ``target`` schema.

:table_rename_pattern:
  (Default ``(.+)``) Regular expression matching names of restored tables.
  Used to rename tables on restoring. Capture groups ``()`` can be used to
  reuse portions of the table name and then used in
  ``table_rename_replacement``. Default value matches the entire table name.

:table_rename_replacement:
  (Default ``$1``) Replacement pattern used to rename tables on restoring.
  Can include groups, captured in ``table_rename_pattern``. By default no
  replacement is happening and tables are restored with their original names.
  Example: ``prefix_$1`` combined with default ``table_rename_pattern`` adds
  'prefix' to all restored table names.

.. CAUTION::

   Restore will abort with a failure if there is a name collision after
   evaluating the rename operations, or if a table with the same name as the
   rename target already exists.
