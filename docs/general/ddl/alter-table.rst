.. _sql_ddl_alter_table:

===============
Altering tables
===============

.. rubric:: Table of contents

.. contents::
   :local:

.. NOTE::

   ``ALTER COLUMN`` action is not currently supported.
   See :ref:`appendix-compatibility`.

.. hide:

    cr> CREATE TABLE my_table (id BIGINT);
    CREATE OK, 1 row affected (... sec)

Updating parameters
===================

The parameters of a table can be modified using the ``ALTER TABLE`` clause::

    cr> alter table my_table set (number_of_replicas = '0-all');
    ALTER OK, -1 rows affected (... sec)

In order to set a parameter to its default value use ``reset``::

    cr> alter table my_table reset (number_of_replicas);
    ALTER OK, -1 rows affected (... sec)

.. NOTE::
    Changing compression using the ``codec`` parameter only takes effect for
    newly written segments. To enforce a rewrite of already existing segments,
    run :ref:`sql-optimize` with the parameter ``max_num_segments = 1``.

.. _alter-shard-number:

Changing the number of shards
-----------------------------

Changing the number of shards in general works in the following steps.

1. A new target table is created but with more/less number of primary shards.
#. The segments from the source table (the underling Lucene index to be
   precise) are hard-linked into the target table at file system level.
#. The source table is dropped while the new table is renamed into the
   source and then recovered in the cluster.

.. NOTE::
    Segment hard-linking makes this operation relevantly cheap as it involves
    no data copying. If the file system, however, does not support hard-linking,
    then all segments will be copied into the new table, resulting in much more
    time and resource consuming operation.

To change the number of primary shards of a table, it is necessary to first
satisfy certain conditions.


.. _alter-shard-number-decrease:

Decreasing the number of shards
...............................

To decrease the number of shards, it is necessary to ensure the following
two conditions:

First, a (primary or replica) copy of every shard of the table must be present
on the **same** node. The user can choose the most suitable node for this
operation and then restrict table :ref:`shard allocation
<gloss-shard-allocation>` on that node using the :ref:`shard allocation
filtering <ddl_shard_allocation>`.

The second condition for decreasing a table's number of shards is to block write
operations to the table::

    cr> alter table my_table set ("blocks.write" = true);
    ALTER OK, -1 rows affected (... sec)

Afterwards the number of shards can be decreased::

    cr> alter table my_table set (number_of_shards = 1);
    ALTER OK, 0 rows affected (... sec)

The user should then revert the restrictions applied on the table, for instance
::

    cr> alter table my_table reset ("routing.allocation.require._name", "blocks.write");
    ALTER OK, -1 rows affected (... sec)

It is necessary to use a factor of the current number of primary shards as
the target number of shards. For example, a table with 8 shards can be shrunk
into 4, 2 or 1 primary shards.


.. _alter-shard-number-increase:

Increase the number of shards
.............................

Increasing the number of shards is limited to tables which have been created
with a ``number_of_routing_shards`` setting. For such tables the shards can be
increased by a factor that depends on this setting. For example, a table with 5
shards, with  ``number_of_routing_shards`` set to 20 can be changed to have
either 10 or 20 shards. (5 x 2 (x 2)) = 20 or (5 x 4) = 20.

The only condition required for increasing the number of shards is to block
operations to the table::

    cr> alter table my_table set ("blocks.write" = true);
    ALTER OK, -1 rows affected (... sec)

Afterwards, the table shards can be increased::

    cr> alter table my_table set (number_of_shards = 2);
    ALTER OK, 0 rows affected (... sec)

Similarly, the user should revert the restrictions applied on the table,
for instance::

    cr> alter table my_table set ("blocks.write" = false);
    ALTER OK, -1 rows affected (... sec)

Read :ref:`Alter Partitioned Tables <partitioned-alter>` to see how to
alter parameters of partitioned tables.

.. _alter-table-add-column:

Adding columns
==============

In order to add a column to an existing table use ``ALTER TABLE`` with the
``ADD COLUMN`` clause::

    cr> alter table my_table add column new_column_name text;
    ALTER OK, -1 rows affected (... sec)

The inner schema of object columns can also be extended, as shown in the
following example.

First a column of type object is added::

    cr> alter table my_table add column obj_column object as (age int);
    ALTER OK, -1 rows affected (... sec)

And now a nested column named ``name`` is added to the ``obj_column``::

    cr> alter table my_table add column obj_column['name'] text;
    ALTER OK, -1 rows affected (... sec)

::

    cr> select column_name, data_type from information_schema.columns
    ... where table_name = 'my_table' and column_name like 'obj_%';
    +--------------------+-----------+
    | column_name        | data_type |
    +--------------------+-----------+
    | obj_column         | object    |
    | obj_column['age']  | integer   |
    | obj_column['name'] | text      |
    +--------------------+-----------+
    SELECT 3 rows in set (... sec)

.. _alter-table-rename-column:

Renaming columns
================

To rename a column of an existing table, use ``ALTER TABLE`` with the
``RENAME COLUMN`` clause::

    cr> alter table my_table rename new_column_name to renamed_column;
    ALTER OK, -1 rows affected (... sec)

This also works on object columns::

    cr> alter table my_table rename column obj_column to renamed_obj_column;
    ALTER OK, -1 rows affected (... sec)

To rename a sub-column of an object column, you can use subscript expressions::

    cr> alter table my_table rename column renamed_obj_column['age'] to
    ...  renamed_obj_column['renamed_age'];
    ALTER OK, -1 rows affected (... sec)


    cr> select column_name, data_type from information_schema.columns
    ... where table_name = 'my_table' and column_name like 'renamed_obj_%';
    +-----------------------------------+-----------+
    | column_name                       | data_type |
    +-----------------------------------+-----------+
    | renamed_obj_column                | object    |
    | renamed_obj_column['renamed_age'] | integer   |
    | renamed_obj_column['name']        | text      |
    +-----------------------------------+-----------+
    SELECT 3 rows in set (... sec)
    
Closing and opening tables
==========================

A table can be closed by using ``ALTER TABLE`` with the ``CLOSE`` clause::

    cr> alter table my_table close;
    ALTER OK, -1 rows affected (... sec)

Closing a table will cause all operations beside ``ALTER TABLE ... OPEN`` to
fail.

A table can be reopened again by using ``ALTER TABLE`` with the ``OPEN``
clause::

    cr> alter table my_table open;
    ALTER OK, -1 rows affected (... sec)

.. NOTE::

    This setting is *not* the same as :ref:`sql-create-table-blocks-read-only`.
    Closing and opening a table will preserve these settings if they are
    already set.

Renaming tables
===============

You can rename a table or view using ``ALTER TABLE`` with the ``RENAME TO`` clause::

     cr> ALTER TABLE my_table RENAME TO my_new_table;
     ALTER OK, -1 rows affected (... sec)

If renaming a table, the shards of the table become temporarily unavailable.

.. _ddl_reroute_shards:

Reroute shards
==============

With the ``REROUTE`` command it is possible to control the :ref:`allocations
<gloss-shard-allocation>` of shards. This gives you the ability to re-balance
the cluster state manually. The supported reroute options are listed in the
reference documentation of :ref:`ALTER TABLE REROUTE
<sql-alter-table-reroute>`.

Shard rerouting can help solve several problems:

* **Unassigned shards**: Due to cause of lack of space, shard awareness or
  any other failure that happens during the automatic shard allocation it is
  possible to gain unassigned shards in the cluster.

* **"Hot Shards"**: Most of your queries affect certain shards only. These
  shards lie on a node that has insufficient resources.

This command takes these :ref:`Routing Allocation Settings <conf_routing>` into
account. Once an allocation occurs CrateDB tries (by default) to re-balance
shards to an even state. CrateDB can be set to disable shard re-balancing
with the setting ``cluster.routing.rebalance.enable=None`` to perform only the
explicit triggered allocations.
.

.. NOTE::

    The command only triggers the allocation and reports back if the process has
    been acknowledged or rejected. Moving or allocating large shards takes more
    time to complete.

In those two cases it may be necessary to move shards manually to another node
or force the retry of the allocation process.
