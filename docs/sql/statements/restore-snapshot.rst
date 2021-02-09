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
    { TABLE ( table_ident [ PARTITION (partition_column = value [ , ... ])] [, ...] ) | ALL }
    [ WITH (restore_parameter [= value], [, ...]) ]


.. _sql-restore-snapshot-description:

Description
===========

Restore one or more tables or partitions from an existing snapshot into the
cluster. The snapshot must be given as fully qualified reference with
``repository_name`` and ``snapshot_name``.

It is possible to restore all tables contained in the snapshot using the
``ALL`` keyword. Single tables and/or partitions can be selected for restoring
by giving them as ``table_ident`` or partition reference given the
``partition_column`` values.

Tables that are to be restored must not exist yet.

To cancel a restore operation simply drop the tables that are being restored.


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

    [ PARTITION ( partition_column = value [ , ... ] ) ]

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
