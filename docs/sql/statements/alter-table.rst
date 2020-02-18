.. highlight:: psql
.. _ref-alter-table:

===============
``ALTER TABLE``
===============

Alter an existing table.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    ALTER [ BLOB ] TABLE { ONLY table_ident
                           | table_ident [ PARTITION (partition_column = value [ , ... ]) ] }
      { SET ( parameter = value [ , ... ] )
        | RESET ( parameter [ , ... ] )
        | ADD [ COLUMN ] column_name data_type [ column_constraint [ ... ] ]
        | OPEN
        | CLOSE
        | RENAME TO table_ident
        | REROUTE reroute_option
      }

where ``column_constraint`` is::

    { PRIMARY KEY |
      NOT NULL |
      INDEX { OFF | USING { PLAIN |
                            FULLTEXT [ WITH ( analyzer = analyzer_name ) ]  } |
      [ CONSTRAINT check_constraint_name ] CHECK (boolean_expression)
    }


Description
===========

``ALTER TABLE`` can be used to modify an existing table definition. It provides
options to add columns, modify constraints, enabling or disabling
table parameters and allows to execute a shard reroute allocation.

Use the ``BLOB`` keyword in order to alter a blob table (see
:ref:`blob_support`). Blob tables cannot have custom columns which means that
the ``ADD COLUMN`` keyword won't work.

While altering a partitioned table, using ``ONLY`` will apply changes for the
table **only** and not for any possible existing partitions. So these changes
will only be applied to new partitions. The ``ONLY`` keyword cannot be used
together with a `PARTITION`_ clause.

See the CREATE TABLE :ref:`with_clause` for a list of available parameters.

:table_ident:
  The name (optionally schema-qualified) of the table to alter.

.. _ref-alter-table-partition-clause:

Clauses
=======

``PARTITION``
-------------

If the table is partitioned this clause can be used to alter only a single
partition.

.. NOTE::

   BLOB tables cannot be partitioned and hence this clause cannot be used.

This clause identifies a single partition. It takes one or more partition
columns with a value each to identify the partition to alter.

::

    [ PARTITION ( partition_column = value [ , ... ] ) ]

:partition_column:
  The name of the column by which the table is partitioned.

  All partition columns that were part of the :ref:`partitioned_by_clause` of
  the :ref:`ref-create-table` statement must be specified.

:value:
  The columns value.

.. SEEALSO:: :ref:`Alter Partitioned Tables <partitioned_tables_alter>`


Arguments
=========

.. _alter_table_set_reset:

``SET/RESET``
-------------

Can be used to change a table parameter to a different value.
Using ``RESET`` will reset the parameter to its default value.

:parameter:
  The name of the parameter that is set to a new value or its default.

The supported parameters are listed in the :ref:`CREATE TABLE WITH CLAUSE
<with_clause>` documentation. In addition to those, for dynamically
changing the number of allocated shards, the parameter ``number_of_shards``
can be used. For more more info on that, see :ref:`alter_change_number_of_shard`.


``ADD COLUMN``
--------------

Can be used to add an additional column to a table. While columns can be added
at any time, adding a new :ref:`generated column <ref-generated-columns>` is
only possible if the table is empty. In addition, adding a base column with
:ref:`ref-default-clause` is not supported. It is possible to define a CHECK
constraint with the restriction that only the column being added may be used
in the boolean expression.

:data_type:
  Data type of the column which should be added.

:column_name:
  Name of the column which should be added.

``OPEN/CLOSE``
--------------

Can be used to open or close the table, respectively. Closing a table prevents
all operations, except ``ALTER TABLE ... OPEN``, to fail. Operations on closed
partitions will not produce an exception, but will have no effect. Similarly,
like ``SELECT`` and ``INSERT`` on partitioned will exclude closed partitions and
continue working.

.. _alter_table_rename:

``RENAME TO``
-------------

Can be used to rename a table, while maintaining its schema and data. During
this operation the shards of the table will become temporarily unavailable.

.. _alter_table_reroute:

``REROUTE``
-----------

The ``REROUTE`` command provides various options to manually control the
allocation of shards. It allows the enforcement of explicit allocations,
cancellations and the moving of shards between nodes in a cluster. See
:ref:`ddl_reroute_shards` to get the convenient use-cases.

The rowcount defines if the reroute or allocation process of a shard was
acknowledged or rejected.

.. NOTE::

   Partitioned tables require a :ref:`Partition Clause <ref-alter-table-partition-clause>`
   in order to specify a unique ``shard_id``.

::

    [ REROUTE reroute_option]


where ``reroute_option`` is::

    { MOVE SHARD shard_id FROM node TO node
      | ALLOCATE REPLICA SHARD shard_id ON node
      | PROMOTE REPLICA SHARD shard_id ON node [ WITH (accept_data_loss = { TRUE | FALSE }) ]
      | CANCEL SHARD shard_id ON node [ WITH (allow_primary = {TRUE|FALSE}) ]
    }

:shard_id:
  The shard id. Ranges from 0 up to the specified number of :ref:`sys-shards`
  shards of a table.

:node:
  The ID or name of a node within the cluster.

  See :ref:`sys-nodes` how to gain the unique ID.


``REROUTE`` supports the following options to start/stop shard allocation:

**MOVE**
  A started shard gets moved from one node to another. It requests a
  ``table_ident`` and a ``shard_id`` to identify the shard that receives the
  new allocation. Specify ``FROM node`` for the node to move the shard from and
  ``TO node`` to move the shard to.

**ALLOCATE REPLICA**
  Allows to force allocation of an unassigned replica shard on a specific node.

.. _alter-table-reroute-promote-replica:

**PROMOTE REPLICA**
  Force promote a stale replica shard to a primary.
  In case a node holding a primary copy of a shard had a failure and the
  replica shards are out of sync, the system won't promote the replica to
  primary automatically, as it would result in a silent data loss.

  Ideally the node holding the primary copy of the shard would be brought back
  into the cluster, but if that is not possible due to a permanent system
  failure, it is possible to accept the potential data loss and force promote a
  stale replica using this command.

  The parameter ``accept_data_loss`` needs to be set to ``true`` in order for
  this command to work. If it is not provided or set to false, the command will
  error out.

**CANCEL**
  This cancels the allocation/recovery of a ``shard_id`` of a ``table_ident``
  on a given ``node``. The ``allow_primary`` flag indicates if it is allowed to
  cancel the allocation of a primary shard.
