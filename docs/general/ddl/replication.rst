.. _ddl-replication:

===========
Replication
===========

You can configure CrateDB to *replicate* tables. When a table is replicated,
table data is stored on multiple primary shards, with each primary shard having
one or more replica shards.

When a primary shard is lost (e.g., due to node failure), CrateDB will promote
a replica shard to primary. Hence, more table replicas mean a smaller chance of
permanent data loss (through increased `data redundancy`) in exchange for more
disk space utilization and intra-cluster network traffic.

Replica shards can also improve read performance because more copies of the
data spread across more nodes. Having more copies of the data on more nodes can
also increases the opportunities for CrateDB to `parallelize`_ query
execution.

.. TIP::

    The `CrateDB Admin UI`_ provides visual indicators of cluster health that
    take replication status into account. Alternatively, you can query health
    information for yourself directly from the :ref:`sys.health <sys-health>`
    table.

You can configure the number of per-shard replicas :ref:`WITH
<sql-create-table-with>` the :ref:`sql-create-table-number-of-replicas` table
setting.

For example::

    cr> create table my_table10 (
    ...   first_column integer,
    ...   second_column text
    ... ) with (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

As well as configuring a single fixed number of pre-replica shards, you can use
a string to configure a range by specifying a minimum and a maximum (dependent
on the number of nodes in the cluster).

Here are some examples of replica ranges:

========= =====================================================================
Range     Explanation
========= =====================================================================
``0-1``   If you only have one node, CrateDB will not create any replicas.
          Having no replicas will result in a *yellow* cluster health. If you
          have more than one node, CreateDB will create one replica per shard.
--------- ---------------------------------------------------------------------
``2-4``   Each table will require at least two replicas for CrateDB to consider
          it fully replicated (i.e., a *green status*).

          If the cluster has four nodes, CrateDB will create four replicas,
          with each replica located on a different node from its respective
          primary to improve :ref:`resiliency <concept-resiliency>` (e.g., due
          to node failure).

          If a cluster has three or fewer nodes, CreateDB will have to locate one
          or more  replica shards will be on the same node as the respective
          primary shard. If this happens, your cluster will have a *yellow*
          cluster health.
--------- ---------------------------------------------------------------------
``0-all`` CrateDB will create as many replicas as you have nodes available.
========= =====================================================================

If you do not specify a ``number_of_replicas``, CrateDB will create one or zero
replicas, depending on the number of available nodes at the cluster (e.g., on a
single-node cluster, ``number_of_replicas`` will be set to zero to allow fast
write operations with the default setting of
:ref:`sql-create-table-write-wait-for-active-shards`).

You can change the :ref:`sql-create-table-number-of-replicas` setting at any
time.

.. SEEALSO::

    :ref:`CREATE TABLE: WITH clause <sql-create-table-number-of-replicas>`


.. _CrateDB Admin UI: https://crate.io/docs/clients/admin-ui/en/latest/
.. _data redundancy: https://en.wikipedia.org/wiki/Data_redundancy
.. _parallelize: https://en.wikipedia.org/wiki/Distributed_computing
