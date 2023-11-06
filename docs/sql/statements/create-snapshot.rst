.. highlight:: psql

.. _sql-create-snapshot:

===================
``CREATE SNAPSHOT``
===================

Create a new incremental snapshot inside a repository that contains the current
state of the given tables and/or partitions and the cluster metadata.

.. rubric:: Table of contents

.. contents::
   :local:


.. _sql-create-snapshot-synopsis:

Synopsis
========

::

    CREATE SNAPSHOT repository_name.snapshot_name
    { TABLE table_ident [ PARTITION (partition_column = value [, ...])] [, ...] | ALL }
    [ WITH (snapshot_parameter [= value], [, ...]) ]


.. _sql-create-snapshot-description:

Description
===========

Create a new incremental snapshot inside a repository.

A snapshot is a backup of the current state of the given tables and the cluster
metadata at the point the ``CREATE SNAPSHOT`` query starts executing. Changes
made after that are not considered for the snapshot.

A snapshot is fully qualified by its ``snapshot_name`` and the name of the
repository it should be created in (``repository_name``). A ``snapshot_name``
must be unique per repository.

.. NOTE::

   For snapshot names the same :ref:`restrictions <ddl-create-table-naming>` as
   for table names apply.

   This is mainly because snapshot names will likely become stored as file or
   directory on disc and hence must be valid filenames.

Creating the snapshot operates on primary shards which are not currently
relocated. If a shard is being relocated the snapshot of the shard is created
when the relocation is completed.

A snapshot only backups the parts of the data that are not yet stored in the
given repository by older snapshots, thus creating snapshots is incremental.

Snapshots can include one or more tables, each given as ``table_ident``. It is
also possible to include only single partitions given the values of the
partition columns.

If ``ALL`` is used, every table in the cluster (except system tables, blob
tables and tables in the ``information_schema`` schema) as well as all
persistent settings and the full cluster metadata is included in the snapshot.


.. _sql-create-snapshot-parameters:

Parameters
==========

:repository_name:
  The name of the repository to create the snapshot in as ident.

:snapshot_name:
  The name of the snapshot as ident.

:table_ident:
  The name (optionally schema-qualified) of an existing table that is to
  be included in the snapshot.


.. _sql-create-snapshot-clauses:

Clauses
=======


.. _sql-create-snapshot-partition:

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
``PARTITION`` clause can be used to create a snapshot from one partition
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
partition to snapshot.

.. TIP::

    The :ref:`ref-show-create-table` statement will show you the complete list
    of partition columns specified by the
    :ref:`sql-create-table-partitioned-by` clause.


.. _sql-create-snapshot-with:

``WITH``
--------

::

    [ WITH (snapshot_parameter [= value], [, ...]) ]

The following configuration parameters can be used to modify how the snapshot
is created:

:wait_for_completion:
  (Default ``false``) By default the request returns once the snapshot
  creation started. If set to ``true`` the request returns after the
  whole snapshot was created or an error occurred. The
  :ref:`sys.snapshots <sys-snapshots>` table can be queried to track the
  snapshot creation progress if ``wait_for_completion`` has been set to
  ``false``.

:ignore_unavailable:
  (Default ``false``) If a given table does not exist the command will
  fail by default. If set to ``true`` these tables are ignored and not
  included in the snapshot.
