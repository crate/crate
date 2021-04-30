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

Replication can also improve read performance because any increase in the
number of shards distributed across a cluster also increases the opportunities
for CrateDB to `parallelize`_ query execution across multiple nodes.


.. _ddl-replication-health:

Table health
============

CrateDB :ref:`allocates <gloss-shard-allocation>` each shard to a specific
node. Normally, CrateDB dynamically allocates shards to satisfy the requirement
that the primary shard and replica shards must all reside on different
nodes. This requirement means that for *one* shard and *n* replicas, you must
have *n + 1* nodes. If CrateDB is unable to satisfy this requirement, it will
give the table a *yellow* :ref:`health status <sys-health>`.

.. TIP::

    The `CrateDB Admin UI`_ provides visual indicators of cluster health that
    take replication status into account. Alternatively, you can query health
    information for yourself directly from the :ref:`sys.health <sys-health>`
    table. You can query detailed replication information from the
    :ref:`sys.shards <sys-shards>` and :ref:`sys.allocations <sys-allocations>`
    tables.


.. _ddl-replication-config:

Table configuration
===================

You can configure the number of per-shard replicas :ref:`WITH
<sql-create-table-with>` the :ref:`sql-create-table-number-of-replicas` table
setting.

For example::

    cr> CREATE TABLE my_table (
    ...   first_column integer,
    ...   second_column text
    ... ) WITH (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

As well as being able to configure a fixed number of replicas, you can
configure a range of values by using a string to specify a minimum and a
maximum (dependent on the number of nodes in the cluster).

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
          it fully replicated (i.e., a *green* :ref:`health status
          <ddl-replication-health>`).

          If the cluster has five nodes, CrateDB will create four replicas,
          with each replica located on a different node from its respective
          primary.

          If a cluster has four nodes or fewer, CrateDB would have to locate
          one or more replica shards on the same node as the respective primary
          shard. As a result, the table would have a *yellow* :ref:`health
          status <ddl-replication-health>`.
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
