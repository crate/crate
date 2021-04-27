.. _ddl-replication:

===========
Replication
===========

You can configure CrateDB to *replicate* tables. When you configure
replication, CrateDB will try to ensure that every table :ref:`shard
<ddl-sharding>` has one or more copies available at all times.

When there are multiple copies of the same shard, CrateDB will mark one as the
*primary shard* and treat the rest as *replica shards*. Write operations
always go to the primary shard, whereas read operations can go to any
shard. CrateDB continually synchronizes data from the primary shard to all
replica shards (through a process known as :ref:`shard recovery
<gloss-shard-recovery>`).

When a primary shard is lost (e.g., due to node failure), CrateDB will promote
a replica shard to primary. Hence, more table replicas mean a smaller chance of
permanent data loss (through increased `data redundancy`) in exchange for more
disk space utilization and intra-cluster network traffic.

CrateDB :ref:`allocates <gloss-shard-allocation>` every shard to a specific
node. CrateDB will try to allocate the primary shard and replica shards to
different nodes to maximize :ref:`durability <concept-durability>` and
:ref:`resiliency <concept-resiliency>`.

Replication can also improve read performance because any increase in the
number of shards distributed across a cluster also increases the opportunities
for CrateDB to `parallelize`_ query execution across multiple nodes.

.. TIP::

    The `CrateDB Admin UI`_ provides visual indicators of cluster health that
    take replication status into account. Alternatively, you can query health
    information for yourself directly from the :ref:`sys.health <sys-health>`
    table. You can query detailed replication information from the
    :ref:`sys.shards <sys-shards>` and :ref:`sys.allocations <sys-allocations>`
    tables.

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
``0-1``   If you only have one node, CrateDB will not create any replicas. If
          you have more than one node, CreateDB will create one replica per
          shard.

          This range is the default value.
--------- ---------------------------------------------------------------------
``2-4``   Each table will require at least two replicas for CrateDB to consider
          it fully replicated (i.e., a *green status*).

          If the cluster has five nodes, CrateDB will create four replicas,
          with each replica located on a different node from its respective
          primary.

          If a cluster has four or fewer nodes, CreateDB will have to locate
          one or more replica shards will be on the same node as the respective
          primary shard. If this happens, your cluster will have a *yellow*
          cluster health.
--------- ---------------------------------------------------------------------
``0-all`` CrateDB will create one replica shard for every node that is
          available in addition to the node that holds the primary shard.
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
