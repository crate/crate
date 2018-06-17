.. _jmx_monitoring:

==============
JMX Monitoring
==============

The JMX monitoring feature exposes query metrics via the `JMX`_ API.

.. NOTE::

   JMX monitoring is an :ref:`enterprise feature <enterprise_features>`.

.. rubric:: Table of Contents

.. contents::
   :local:

Setup
=====

Enable the Enterprise License
-----------------------------

You can enable the :ref:`conf-node-license` via the CrateDB configuration file.

Enable Collecting Stats
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

  -Djava.rmi.server.hostname=<RMI_PORT>
  -Dcom.sun.management.jmxremote.rmi.port=<RMI_HOSTNAME>

Here, ``<RMI_HOSTNAME>`` is the IP address or hostname of the Docker host and
``<RMI_PORT>`` is the statically assigned port of the RMI server. For
convenience, ``<RMI_PORT>`` can be set to the same port the JMX server listens on.

The ``<RMI_HOSTNAME>`` and ``<RMI_PORT>`` can be used by JMX clients (e.g.
`JConsole`_ or `VisualVM`_) to connect to the JMX server.

Here's an example Docker command::

  sh> docker run -d -e CRATE_JAVA_OPTS='\
        -Dcom.sun.management.jmxremote
        -Dcom.sun.management.jmxremote.port=7979 \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.rmi.port=<RMI_HOSTNAME> \
        -Djava.rmi.server.hostname=7979' \
        -p 7979:7979 crate \
        crate -Cnetwork.host=_site_

Here, again, ``<RMI_HOSTNAME>`` is the IP address or hostname of the Docker
host.

JMX Beans
=========

QueryStats MBean
----------------

The ``QueryStats`` JMX MBean exposes query frequency and average duration in
milliseconds for ``SELECT``, ``UPDATE``, ``DELETE``, and ``INSERT`` queries.

Metrics can be accessed using the JMX MBean object name
``io.crate.monitoring:type=QueryStats`` and the following attributes:

Frequency:

 - ``SelectQueryFrequency``
 - ``InsertQueryFrequency``
 - ``UpdateQueryFrequency``
 - ``DeleteQueryFrequency``
 - ``OverallQueryFrequency``

Average duration:

 - ``SelectQueryAverageDuration``
 - ``InsertQueryAverageDuration``
 - ``UpdateQueryAverageDuration``
 - ``DeleteQueryAverageDuration``
 - ``OverallQueryAverageDuration``

NodeStatus MBean
----------------

The ``NodeStatus`` JMX MBean exposes the status of the current node as boolean values.

NodeStatus can be accessed using the JMX MBean object name
``io.crate.monitoring:type=NodeStatus`` and the following attributes:

 - ``Ready``

   Defines if the node is able to process SQL statements.

NodeInfo MBean
--------------

The ``NodeInfo`` JMX MBean exposes information about the current node;

NodeInfo can be accessed using the JMX MBean object name
``io.crate.monitoring:type=NodeInfo`` and the following attributes:

 - ``ClusterStateVersion``

   Provides the version of the current applied cluster state

 - ``NodeId``

   Provides the unique identifier of the node in the cluster

 - ``NodeName``

   Provides the human friendly name of the node

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
| ``Bulk``              | Thread pool statistics of the ``bulk`` thread pool      |
|                       | used for writing and deleting data.                     |
+-----------------------+---------------------------------------------------------+
| ``Management``        | Thread pool statistics of the ``management`` thread     |
|                       | pool used by management tasks like stats collecting,    |
|                       | repository information, shard allocations, etc.         |
+-----------------------+---------------------------------------------------------+
| ``Index``             | Thread pool statistics of the ``index`` thread pool     |
|                       | used for writing blobs.                                 |
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
|                       | thread pool used on shard allocation.                   |
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


Exposing JMX via HTTP
=====================

The JMX metrics and a readiness endpoint can be exposed via HTTP (e.g. to be
used by `Prometheus`_) by using the `Crate JMX HTTP Exporter`_ Java agent. See
the `README`_ in the `Crate JMX HTTP Exporter`_ repository for more information.

.. _Prometheus: https://prometheus.io/
.. _README: https://github.com/crate/jmx_exporter/blob/master/README.rst
.. _Crate JMX HTTP Exporter: https://github.com/crate/jmx_exporter
.. _JMX: http://docs.oracle.com/javase/8/docs/technotes/guides/jmx/
.. _JMX documentation: http://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html#gdeum
.. _JConsole: http://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html#gdeum
.. _VisualVM: https://visualvm.java.net/
.. _CompositeData: http://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html#mozTocId99420
