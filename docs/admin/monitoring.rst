.. _jmx_monitoring:

==============
JMX monitoring
==============

The JMX monitoring feature exposes query metrics via the `JMX`_ API.

.. rubric:: Table of contents

.. contents::
   :local:

Setup
=====

Enable collecting stats
-----------------------

.. highlight:: psql

By default, :ref:`conf_collecting_stats` is enabled.
You can disable collecting stats via the CrateDB configuration file
or by running this statement::

  cr> SET GLOBAL "stats.enabled" = FALSE;

Enable the JMX API
------------------

.. highlight:: none

To monitor CrateDB using the JMX API, you must set the following system
properties before you start CrateDB::

  com.sun.management.jmxremote
  com.sun.management.jmxremote.port=<JMX_PORT>
  com.sun.management.jmxremote.ssl=false
  com.sun.management.jmxremote.authenticate=false

Here, ``<JMX_PORT>`` sets the port number of your JMX server. JMX SSL and
authentication are currently not supported.

More information about the JMX monitoring properties can be found in the `JMX
documentation`_.

.. highlight:: sh

You can set the Java system properties with the ``-D`` option::

  sh$ ./bin/crate -Dcom.sun.management.jmxremote \
  ...             -Dcom.sun.management.jmxremote.port=7979 \
  ...             -Dcom.sun.management.jmxremote.ssl=false \
  ...             -Dcom.sun.management.jmxremote.authenticate=false

However, the recommended way to set system properties is via the
``CRATE_JAVA_OPTS`` environment variable, like so::

  sh$ export CRATE_JAVA_OPTS="$CRATE_JAVA_OPTS \
        -Dcom.sun.management.jmxremote \
        -Dcom.sun.management.jmxremote.port=7979 \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.authenticate=false"
  sh$ ./bin/crate

If you're using the CrateDB Debian or RPM packages, you can set this
environment variable via the ``/etc/default/crate`` configuration file.

Using Docker
============

.. highlight:: none

To enable JMX monitoring when running CrateDB in a Docker container you have to
set the following additional Java system properties::

  -Djava.rmi.server.hostname=<RMI_HOSTNAME>
  -Dcom.sun.management.jmxremote.rmi.port=<RMI_PORT>

Here, ``<RMI_HOSTNAME>`` is the IP address or hostname of the Docker host and
``<RMI_PORT>`` is the statically assigned port of the RMI server. For
convenience, ``<RMI_PORT>`` can be set to the same port the JMX server listens on.

The ``<RMI_HOSTNAME>`` and ``<RMI_PORT>`` can be used by JMX clients (e.g.
`JConsole`_ or `VisualVM`_) to connect to the JMX server.

Here's an example Docker command::

  sh> docker run -d --env CRATE_HEAP_SIZE=1g -e CRATE_JAVA_OPTS="\
        -Dcom.sun.management.jmxremote
        -Dcom.sun.management.jmxremote.port=7979 \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.rmi.port=7979 \
        -Djava.rmi.server.hostname=<RMI_HOSTNAME>" \
        -p 7979:7979 crate -Cnetwork.host=_site_

Here, again, ``<RMI_HOSTNAME>`` is the IP address or hostname of the Docker
host.

JMX Beans
=========

.. _query_stats_mbean:

QueryStats MBean
----------------

The ``QueryStats`` MBean exposes the sum of durations, in milliseconds, total
and failed count of all statements executed since the node was started, grouped
by type, for ``SELECT``, ``UPDATE``, ``DELETE``, ``INSERT``, ``MANAGEMENT``,
``DDL``, ``COPY`` and ``UNDEFINED`` queries.

Metrics can be accessed using the JMX MBean object name
``io.crate.monitoring:type=QueryStats`` and the following attributes:

Statements total count since the node was started:

 - ``SelectQueryTotalCount``
 - ``InsertQueryTotalCount``
 - ``UpdateQueryTotalCount``
 - ``DeleteQueryTotalCount``
 - ``ManagementQueryTotalCount``
 - ``DDLQueryTotalCount``
 - ``CopyQueryTotalCount``
 - ``UndefinedQueryTotalCount``

Statements failed count since the node was started:

 - ``SelectQueryFailedCount``
 - ``InsertQueryFailedCount``
 - ``UpdateQueryFailedCount``
 - ``DeleteQueryFailedCount``
 - ``ManagementQueryFailedCount``
 - ``DDLQueryFailedCount``
 - ``CopyQueryFailedCount``
 - ``UndefinedQueryFailedCount``

The sum of the durations, in milliseconds, since the node was started, of all
statement executions grouped by type:

 - ``SelectQuerySumOfDurations``
 - ``InsertQuerySumOfDurations``
 - ``UpdateQuerySumOfDurations``
 - ``DeleteQuerySumOfDurations``
 - ``ManagementQuerySumOfDurations``
 - ``DDLQuerySumOfDurations``
 - ``CopyQuerySumOfDurations``
 - ``UndefinedQuerySumOfDurations``

NodeStatus MBean
----------------

The ``NodeStatus`` JMX MBean exposes the status of the current node as boolean values.

NodeStatus can be accessed using the JMX MBean object name
``io.crate.monitoring:type=NodeStatus`` and the following attributes:

 - ``Ready``

   Defines if the node is able to process SQL statements.

.. _node_info_mxbean:

NodeInfo MXBean
---------------

The ``NodeInfo`` JMX MXBean exposes information about the current node.

NodeInfo can be accessed using the JMX MXBean object name
``io.crate.monitoring:type=NodeInfo`` and the following attributes:

+-------------------------+---------------------------------------------------+
| Name                    | Description                                       |
+=========================+===================================================+
| ``NodeId``              | Provides the unique identifier of the node in the |
|                         | cluster.                                          |
+-------------------------+---------------------------------------------------+
| ``NodeName``            | Provides the human friendly name of the node.     |
+-------------------------+---------------------------------------------------+
| ``ClusterStateVersion`` | Provides the version of the current applied       |
|                         | cluster state.                                    |
+-------------------------+---------------------------------------------------+
| ``ShardStats``          | Statistics about the number of shards located on  |
|                         | the node.                                         |
+-------------------------+---------------------------------------------------+
| ``ShardInfo``           | Detailed information about the shards located on  |
|                         | the node.                                         |
+-------------------------+---------------------------------------------------+

``ShardStats`` returns a `CompositeData`_ object containing statistics about
the number of shards located on the node with the following attributes:

+-------------------+---------------------------------------------------------+
| Name              | Description                                             |
+===================+=========================================================+
| ``Total``         | The number of shards located on the node.               |
+-------------------+---------------------------------------------------------+
| ``Primaries``     | The number of primary shards located on the node.       |
+-------------------+---------------------------------------------------------+
| ``Replicas``      | The number of replica shards located on the node.       |
+-------------------+---------------------------------------------------------+
| ``Unassigned``    | The number of unassigned shards in the cluster. If the  |
|                   | node is the elected master node in the cluster, this    |
|                   | will show the total number of unassigned shards in the  |
|                   | cluster, otherwise 0.                                   |
+-------------------+---------------------------------------------------------+

``ShardInfo`` returns an Array of `CompositeData`_ objects containing detailed
information about the shards located on the node with the following attributes:

+--------------------+--------------------------------------------------------+
| Name               | Description                                            |
+====================+========================================================+
| ``Id``             | The shard id. This shard id is managed by the system,  |
|                    | ranging from 0 up to the number of configured shards   |
|                    | of the table.                                          |
+--------------------+--------------------------------------------------------+
| ``Table``          | The name of the table this shard belongs to.           |
+--------------------+--------------------------------------------------------+
| ``PartitionIdent`` | The partition ident of a partitioned table. Empty for  |
|                    | non-partitioned tables.                                |
+--------------------+--------------------------------------------------------+
| ``RoutingState``   | The current state of the shard in the routing table.   |
|                    | Possible states are:                                   |
|                    |                                                        |
|                    | * UNASSIGNED                                           |
|                    | * INITIALIZING                                         |
|                    | * STARTED                                              |
|                    | * RELOCATING                                           |
+--------------------+--------------------------------------------------------+
| ``State``          | The current state of the shard. Possible states are:   |
|                    |                                                        |
|                    | * CREATED                                              |
|                    | * RECOVERING                                           |
|                    | * POST_RECOVERY                                        |
|                    | * STARTED                                              |
|                    | * RELOCATED                                            |
|                    | * CLOSED                                               |
|                    | * INITIALIZING                                         |
|                    | * UNASSIGNED                                           |
+--------------------+--------------------------------------------------------+
| ``Size``           | The estimated cumulated size in bytes of all files of  |
|                    | this shard.                                            |
+--------------------+--------------------------------------------------------+

Connections MBean
-----------------

The ``Connections`` MBean exposes information about any open connections to a
``CrateDB`` node.

It can be accessed using the ``io.crate.monitoring:type=Connections`` object
name and has the following attributes:

+----------------------+---------------------------------------------------------+
| Name                 | Description                                             |
+======================+=========================================================+
| ``HttpOpen``         | The number of currently established connections via     |
|                      | HTTP                                                    |
+----------------------+---------------------------------------------------------+
| ``HttpTotal``        | The number of total connections established via HTTP    |
|                      | over the life time of a node                            |
+----------------------+---------------------------------------------------------+
| ``PsqlOpen``         | The number of currently established connections via the |
|                      | PostgreSQL protocol                                     |
+----------------------+---------------------------------------------------------+
| ``PsqlTotal``        | The number of total connections established via the     |
|                      | PostgreSQL protocol over the life time of a node        |
+----------------------+---------------------------------------------------------+
| ``TransportOpen``    | The number of currently established connections via the |
|                      | transport protocol                                      |
+----------------------+---------------------------------------------------------+

ThreadPools MXBean
------------------

The ``ThreadPools`` MXBean exposes statistical information about the used thread
pools of a ``CrateDB`` node.

It can be accessed using the ``io.crate.monitoring:type=ThreadPools`` object
name and has following attributes:

+-----------------------+---------------------------------------------------------+
| Name                  | Description                                             |
+=======================+=========================================================+
| ``Generic``           | Thread pool statistics of the ``generic`` thread pool.  |
+-----------------------+---------------------------------------------------------+
| ``Search``            | Thread pool statistics of the ``search`` thread pool    |
|                       | used by read statements on user generated tables.       |
+-----------------------+---------------------------------------------------------+
| ``Write``             | Thread pool statistics of the ``write`` thread pool     |
|                       | used for writing and deleting data.                     |
+-----------------------+---------------------------------------------------------+
| ``Management``        | Thread pool statistics of the ``management`` thread     |
|                       | pool used by management tasks like stats collecting,    |
|                       | repository information, :ref:`shard allocations         |
|                       | <gloss-shard-allocation>`, etc.                         |
+-----------------------+---------------------------------------------------------+
| ``Flush``             | Thread pool statistics of the ``flush`` thread pool     |
|                       | used for fsyncing to disk and merging segments in the   |
|                       | storage engine.                                         |
+-----------------------+---------------------------------------------------------+
| ``Refresh``           | Thread pool statistics of the ``refresh`` thread pool   |
|                       | used for automatic and on-demand refreshing of tables   |
+-----------------------+---------------------------------------------------------+
| ``Snapshot``          | Thread pool statistics of the ``snapshot`` thread pool  |
|                       | used for creating and restoring snapshots.              |
+-----------------------+---------------------------------------------------------+
| ``ForceMerge``        | Thread pool statistics of the ``force_merge`` thread    |
|                       | pool used when running an ``optimize`` statement.       |
+-----------------------+---------------------------------------------------------+
| ``Listener``          | Thread pool statistics of the ``listener`` thread pool  |
|                       | used on client nodes for asynchronous result listeners. |
+-----------------------+---------------------------------------------------------+
| ``Get``               | Thread pool statistics of the ``get`` thread pool       |
|                       | used when querying ``sys.nodes`` or ``sys.shards``.     |
+-----------------------+---------------------------------------------------------+
| ``FetchShardStarted`` | Thread pool statistics of the ``fetch_shard_started``   |
|                       | thread pool used on :ref:`shard allocation              |
|                       | <gloss-shard-allocation>` .                             |
+-----------------------+---------------------------------------------------------+
| ``FetchShardStore``   | Thread pool statistics of the ``fetch_shard_store``     |
|                       | used on shard replication.                              |
+-----------------------+---------------------------------------------------------+

Each of them returns a `CompositeData`_ object containing detailed statistics
of each thread pool with the following attributes:

+---------------------+-----------------------------------------------------+
| Name                | Description                                         |
+=====================+=====================================================+
| ``poolSize``        | The current number of threads in the pool.          |
+---------------------+-----------------------------------------------------+
| ``largestPoolSize`` | The largest number of threads that have ever        |
|                     | simultaneously been in the pool.                    |
+---------------------+-----------------------------------------------------+
| ``queueSize``       | The current number of tasks in the queue.           |
+---------------------+-----------------------------------------------------+
| ``active``          | The approximate number of threads that are actively |
|                     | executing tasks.                                    |
+---------------------+-----------------------------------------------------+
| ``completed``       | The approximate total number of tasks that have     |
|                     | completed execution.                                |
+---------------------+-----------------------------------------------------+
| ``rejected``        | The number of rejected executions.                  |
+---------------------+-----------------------------------------------------+

CircuitBreakers MXBean
----------------------

The ``CircuitBreaker`` MXBean exposes statistical information about all
available circuit breakers of a ``CrateDB`` node.

It can be accessed using the ``io.crate.monitoring:type=CircuitBreakers`` object
name and has following attributes:

+----------------------+----------------------------------------------------------+
| Name                 | Description                                              |
+======================+==========================================================+
| ``Parent``           | Statistics of the ``parent`` circuit breaker             |
|                      | containing summarized counters across all circuit        |
|                      | breakers.                                                |
+----------------------+----------------------------------------------------------+
| ``Query``            | Statistics of the ``query`` circuit breaker used to      |
|                      | account memory usage of SQL execution including          |
|                      | intermediate states e.g. on aggregation and resulting    |
|                      | rows.                                                    |
+----------------------+----------------------------------------------------------+
| ``JobsLog``          | Statistics of the ``jobs_log`` circuit breaker used to   |
|                      | account memory usage of the ``sys.jobs_log`` table.      |
+----------------------+----------------------------------------------------------+
| ``OperationsLog``    | Statistics of the ``operations_log`` circuit breaker     |
|                      | used to account memory usage of the                      |
|                      | ``sys.operations_log`` table.                            |
+----------------------+----------------------------------------------------------+
| ``FieldData``        | Statistics of the ``field_data`` circuit breaker used    |
|                      | for estimating the amount of memory a field will require |
|                      | to be loaded into memory.                                |
+----------------------+----------------------------------------------------------+
| ``InFlightRequests`` | Statistics of the ``in_flight_requests`` circuit breaker |
|                      | used to account memory usage of all incoming requests    |
|                      | on transport or HTTP level.                              |
+----------------------+----------------------------------------------------------+
| ``Request``          | Statistics of the ``request`` circuit breaker used to    |
|                      | account memory usage of per-request data structure.      |
+----------------------+----------------------------------------------------------+

Each of them returns a `CompositeData`_ object containing detailed statistics
of each circuit breaker with the following attributes:

+------------------+------------------------------------------------------+
| Name             | Description                                          |
+==================+======================================================+
| ``name``         | The circuit breaker name this statistic belongs to.  |
+------------------+------------------------------------------------------+
| ``used``         | The currently accounted used memory estimations.     |
+------------------+------------------------------------------------------+
| ``limit``        | The configured limit when to trip.                   |
+------------------+------------------------------------------------------+
| ``trippedCount`` | The total number of occurred trips.                  |
+------------------+------------------------------------------------------+

Exposing JMX via HTTP
=====================

The JMX metrics and a readiness endpoint can be exposed via HTTP (e.g. to be
used by `Prometheus`_) by using the `Crate JMX HTTP Exporter`_ Java agent. See
the `README`_ in the `Crate JMX HTTP Exporter`_ repository for more information.

.. _Prometheus: https://prometheus.io/
.. _README: https://github.com/crate/jmx_exporter/blob/master/README.rst
.. _Crate JMX HTTP Exporter: https://github.com/crate/jmx_exporter
.. _JMX: https://docs.oracle.com/javase/8/docs/technotes/guides/jmx/
.. _JMX documentation: https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html#gdeum
.. _JConsole: https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html#gdeum
.. _VisualVM: https://visualvm.github.io/
.. _CompositeData: https://www.oracle.com/java/technologies/javase/management-extensions-best-practices.html#mozTocId931827
