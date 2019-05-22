.. _replication:

===========
Replication
===========

Replication of a table in CrateDB means that each primary shard of a table is
stored additionally on so called secondary shards. This can be useful for
better read performance and high availability.

If not specified, CrateDB creates zero to one replica depending on the number
of available nodes at the cluster. At a single-node cluster, replicas are set
to zero to allow fast write operations with the default setting of
:ref:`sql_ref_write_wait_for_active_shards`.

.. rubric:: Table of contents

.. contents::
    :local:

Configuration
=============

Defining the number of replicas is done using the ``number_of_replicas``
property.

Example::

    cr> create table my_table10 (
    ...   first_column integer,
    ...   second_column text
    ... ) with (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

The ``number_of_replicas`` property also accepts an string as parameter that
contains a ``range``.

A range is a definition of minimum number of replicas to maximum number of
replicas depending on the number of nodes in the cluster. The table below shows
some examples.

===== =========================================================================
Range Explanation
===== =========================================================================
0-1   Will create no replicas if only one node. This will result in a yellow
      cluster health.

      One replica if more than one node.
----- -------------------------------------------------------------------------
2-4   Table requires at least two replicas to be fully replicated.

      Will create up to four replicas if nodes are added.

      If you have less than three nodes, one or more replica shards will be
      located on the same node as the primary shard. This will result in a
      yellow cluster health.
----- -------------------------------------------------------------------------
0-all Will expand the number of replicas to the available number of nodes.
===== =========================================================================

For details of the range syntax refer to :ref:`number_of_replicas`.

.. NOTE::

  The number of replicas can be changed at any time.

.. SEEALSO::

    The `Admin UI`_ indicates the health of your data.  This information call
    also be queried via the :ref:`sys.health <sys-health>` table.

.. _Admin UI: https://crate.io/docs/clients/admin-ui/en/latest/
