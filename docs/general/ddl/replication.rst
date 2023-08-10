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
a replica shard to a primary. Hence, more table replicas mean a smaller chance
of permanent data loss (through increased `data redundancy`) in exchange for
more disk space utilization and intra-cluster network traffic.

Replication can also improve read performance because any increase in the
number of shards distributed across a cluster also increases the opportunities
for CrateDB to `parallelize`_ query execution across multiple nodes.

.. rubric:: Table of contents

.. contents::
   :local:


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
          you have more than one node, CrateDB will create one replica per
          shard.

          This range is the default value.
--------- ---------------------------------------------------------------------
``2-4``   Each table will require at least two replicas for CrateDB to consider
          it fully replicated (i.e., a *green* replication :ref:`health status
          <sys-health-def>`).

          If the cluster has five nodes, CrateDB will create four replicas and
          allocate each one to a node that does not hold the corresponding
          primary.

          Suppose a cluster has four nodes or fewer. In that case, CrateDB will
          be unable to allocate every replica to a node that does not hold the
          corresponding primary, putting the table into :ref:`underreplication
          <ddl-replication-underreplication>`. As a result, CrateDB will give
          the table a *yellow* replication :ref:`health status
          <sys-health-def>`.
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


.. _ddl-replication-recovery:

Shard recovery
==============

CrateDB :ref:`allocates <gloss-shard-allocation>` each primary and replica
shard to a specific node. You can control this behavior by configuring the
:ref:`allocation <conf_routing>` settings.

If one or more nodes become unavailable (e.g., due to hardware failure or
network issues), CrateDB will try to recover a replicated table by doing the
following:

.. rst-class:: open

- For every lost primary shard, locate a replica and promote it to a primary.

  When CrateDB promotes a replica to primary, it can no longer function as a
  replica, and so the total number of replicas decreases by one. Because each
  primary requires a fixed :ref:`sql-create-table-number-of-replicas`, a new
  replica has to be created (see next item).

- For every primary with too few replicas (due to node loss or replica
  promotion), use the primary shard to :ref:`recover <gloss-shard-recovery>`
  the required number of replicas.

Shard recovery is one of the features that allows CrateDB to provide continuous
`availability`_ and `partition tolerance`_ in exchange for some
:ref:`consistency trade-offs <concept-resiliency-consistency>`.

.. SEEALSO::

    `Wikipedia: CAP theorem`_

.. _ddl-replication-underreplication:

Underreplication
================

Having more replicas per primary and distributing shards as thinly as possible
(i.e., fewer shards per node) can both increase chances of a :ref:`successful
recovery <ddl-replication-recovery>` in the event of node loss.

A single node can hold multiple shards belonging to the same table. For
example, suppose a table has more shards (primaries and replicas) than nodes
available in the cluster. In that case, CrateDB will determine the best way to
allocate shards to the nodes available.

However, there is never a benefit to allocating multiple copies of the same
shard to a single node (e.g., the primary and a replica of the same shard or
two replicas of the same shard).

For example:

.. rst-class:: open

- Suppose a single node held the primary and a replica of the same
  shard. If that node were lost, CrateDB would be unable to use either copy of
  the shard for :ref:`recovery <ddl-replication-recovery>` (because both were
  lost), effectively making the replica useless.

- Suppose a single node held two replicas of the same shard. If the primary
  shard were lost (on a different node), CrateDB would only need one of the
  replica shards on this node to promote a new primary, effectively making the
  second replica useless.

In both cases, the second copy of the shard serves no purpose.

For this reason, CrateDB will never allocate multiple copies of the same shard
to a single node.

The above rule means that for *one* primary shard and *n* replicas, a cluster
must have at least *n + 1* available nodes for CrateDB to fully replicate all
shards. When CrateDB cannot fully replicate all shards, the table enters a
state known as *underreplication*.

CrateDB gives underreplicated tables a *yellow* :ref:`health status
<sys-health-def>`.

.. TIP::

    The `CrateDB Admin UI`_ provides visual indicators of cluster health that
    take replication status into account.

    Alternatively, you can query health information directly from the
    :ref:`sys.health <sys-health>` table and replication information from the
    :ref:`sys.shards <sys-shards>` and :ref:`sys.allocations <sys-allocations>`
    tables.


.. _availability: https://en.wikipedia.org/wiki/Availability
.. _CrateDB Admin UI: https://crate.io/docs/clients/admin-ui/en/latest/
.. _data redundancy: https://en.wikipedia.org/wiki/Data_redundancy
.. _parallelize: https://en.wikipedia.org/wiki/Distributed_computing
.. _partition tolerance: https://en.wikipedia.org/wiki/Network_partitioning
.. _Wikipedia\: CAP theorem: https://en.wikipedia.org/wiki/CAP_theorem
