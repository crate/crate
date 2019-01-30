.. highlight:: sh

.. _conf-cluster-settings:

=====================
Cluster Wide Settings
=====================

All current applied cluster settings can be read by querying the
:ref:`sys.cluster.settings <sys-cluster-settings>` column. Most
cluster settings can be :ref:`changed at runtime
<administration-runtime-config>`. This is documented at each setting.

.. rubric:: Table of Contents

.. contents::
   :local:

.. _applying-cluster-settings:

Non-Runtime Cluster Wide Settings
---------------------------------

Cluster wide settings which cannot be changed at runtime need to be specified
in the configuration of each node in the cluster.

.. CAUTION::

   Cluster settings specified via node configurations are required to be
   exactly the same on every node in the cluster for proper operation of the
   cluster.

.. _conf_collecting_stats:

Collecting Stats
----------------

.. _stats.enabled:

**stats.enabled**
  | *Default:*    ``true``
  | *Runtime:*   ``yes``

  A boolean indicating whether or not to collect statistical information about
  the cluster.

  .. CAUTION::

     The collection of statistical information incurs a slight performance
     penalty, as details about every job and operation across the cluster will
     cause data to be inserted into the corresponding system tables.

.. _stats.jobs_log_size:

**stats.jobs_log_size**
  | *Default:*   ``10000``
  | *Runtime:*  ``yes``

  The maximum number of job records kept to be kept in the :ref:`sys.jobs_log
  <sys-logs>` table on each node.

  A job record corresponds to a single SQL statement to be executed on the
  cluster. These records are used for performance analytics. A larger job log
  produces more comprehensive stats, but uses more RAM.

  Older job records are deleted as newer records are added, once the limit is
  reached.

  Setting this value to ``0`` disables collecting job information.

.. _stats.jobs_log_expiration:

**stats.jobs_log_expiration**
  | *Default:*  ``0s`` (disabled)
  | *Runtime:*  ``yes``

  The job record expiry time in seconds.

  Job records in the :ref:`sys.jobs_log <sys-logs>` table are periodically
  cleared if they are older than the expiry time. This setting overrides
  :ref:`stats.jobs_log_size <stats.jobs_log_size>`.

  If the value is set to ``0``, time based log entry eviction is disabled.

  .. NOTE::

     If both the :ref:`stats.operations_log_size <stats.operations_log_size>`
     and
     :ref:`stats.operations_log_expiration <stats.operations_log_expiration>`
     settings are disabled, jobs will not be recorded.

.. _stats.jobs_log_filter:

**stats.jobs_log_filter**
  | *Default:* ``true`` (Include everything)
  | *Runtime:* ``yes``

  An expression to determine if a job should be recorded into ``sys.jobs_log``.
  The expression must evaluate to a boolean. If it evaluates to ``true`` the
  statement will show up ``sys.jobs_log`` until it's evicted due to one of the
  other rules. (expiration or size limit reached).

  The expression may reference all columns contained in ``sys.jobs_log``. A
  common use case is to include only jobs that took a certain amount of time to
  execute::

    cr> SET GLOBAL "stats.jobs_log_filter" = 'ended - started > 100';

.. _stats.jobs_log_persistent_filter:

**stats.jobs_log_persistent_filter**
  | *Default:* ``false`` (Include nothing)
  | *Runtime:* ``yes``

  An expression to determine if a job should also be recorded to the regular
  ``CrateDB`` log. Entries that match this filter will be logged under the
  ``StatementLog`` logger with the ``INFO`` level.

  This is similar to ``stats.jobs_log_filter`` except that these entries are
  persisted to the log file. This should be used with caution and shouldn't be
  set to an expression that matches many queries as the logging operation will
  block on IO and can therefore affect performance.

  A common use case is to use this for slow query logging.

.. _stats.operations_log_size:

**stats.operations_log_size**
  | *Default:*   ``10000``
  | *Runtime:*  ``yes``

  The maximum number of operations records to be kept in the
  :ref:`sys.operations_log <sys-logs>` table on each node.

  A job consists of one or more individual operations. Operations records are
  used for performance analytics. A larger operations log produces more
  comprehensive stats, but uses more RAM.

  Older operations records are deleted as newer records are added, once the
  limit is reached.

  Setting this value to ``0`` disables collecting operations information.

.. _stats.operations_log_expiration:

**stats.operations_log_expiration**
  | *Default:*  ``0s`` (disabled)
  | *Runtime:*  ``yes``

  Entries of :ref:`sys.operations_log <sys-logs>` are cleared by a periodically
  job when they are older than the specified expire time. This setting
  overrides :ref:`stats.operations_log_size <stats.operations_log_size>`. If
  the value is set to ``0`` the time based log entry eviction is disabled.

  .. NOTE::

    If both setttings :ref:`stats.operations_log_size
    <stats.operations_log_size>` and :ref:`stats.operations_log_expiration
    <stats.operations_log_expiration>` are disabled, no job information will be
    collected.

.. _stats.service.interval:

**stats.service.interval**
  | *Default:*    ``1h``
  | *Runtime:*   ``yes``

  Defines the refresh interval to refresh tables statistics used to produce
  optimal query execution plans.

  This field expects a time value either as a long or double or alternatively
  as a string literal with a time suffix (``ms``, ``s``, ``m``, ``h``, ``d``,
  ``w``).

  If the value provided is ``0`` then the refresh is disabled.

  .. CAUTION::

    Using a very small value can cause a high load on the cluster.

.. _conf_usage_data_collector:

Usage Data Collector
--------------------

The settings of the Usage Data Collector are read-only and cannot be set during
runtime. Please refer to :ref:`usage_data_collector` to get further information
about its usage.

.. _udc_enabled:

**udc.enabled**
  | *Default:*  ``true``
  | *Runtime:*  ``no``

  ``true``: Enables the Usage Data Collector.

  ``false``: Disables the Usage Data Collector.

.. _cluster.udc.initial_delay:

**udc.initial_delay**
  | *Default:*  ``10m``
  | *Runtime:*  ``no``

  The delay for first ping after start-up.

  This field expects a time value either as a long or double or alternatively
  as a string literal with a time suffix (``ms``, ``s``, ``m``, ``h``, ``d``,
  ``w``).

.. _cluster.udc.interval:

**udc.interval**
  | *Default:*  ``24h``
  | *Runtime:*  ``no``

  The interval a UDC ping is sent.

  This field expects a time value either as a long or double or alternatively
  as a string literal with a time suffix (``ms``, ``s``, ``m``, ``h``, ``d``,
  ``w``).

.. _cluster.udc.url:

**udc.url**
  | *Default:*  ``https://udc.crate.io``
  | *Runtime:*  ``no``

  The URL the ping is sent to.

.. _conf_graceful_stop:

Graceful Stop
-------------

By default, when the CrateDB process stops it simply shuts down, possibly
making some shards unavailable which leads to a *red* cluster state and lets
some queries fail that required the now unavailable shards. In order to
*safely* shutdown a CrateDB node, the graceful stop procedure can be used.

The following cluster settings can be used to change the shutdown behaviour of
nodes of the cluster:

.. _cluster.graceful_stop.min_availability:

**cluster.graceful_stop.min_availability**
  | *Default:*   ``primaries``
  | *Runtime:*  ``yes``
  | *Allowed Values:*   ``none | primaries | full``

  ``none``: No minimum data availability is required. The node may shut down
  even if records are missing after shutdown.

  ``primaries``: At least all primary shards need to be available after the node
  has shut down. Replicas may be missing.

  ``full``: All records and all replicas need to be available after the node
  has shut down. Data availability is full.

  .. NOTE::

     This option is ignored if there is only 1 node in a cluster!

.. _cluster.graceful_stop.reallocate:

**cluster.graceful_stop.reallocate**
  | *Default:*   ``true``
  | *Runtime:*  ``yes``

  ``true``: The ``graceful stop`` command allows shards to be reallocated
  before shutting down the node in order to ensure minimum data availability
  set with ``min_availability``.

  ``false``: The ``graceful stop`` command will fail if the cluster would need
  to reallocate shards in order to ensure the minimum data availability set
  with ``min_availability``.

  .. WARNING::

     Make sure you have enough nodes and enough disk space for the
     reallocation.

.. _cluster.graceful_stop.timeout:

**cluster.graceful_stop.timeout**
  | *Default:*   ``2h``
  | *Runtime:*  ``yes``

  Defines the maximum waiting time in milliseconds for the reallocation process
  to finish. The ``force`` setting will define the behaviour when the shutdown
  process runs into this timeout.

  The timeout expects a time value either as a long or double or alternatively
  as a string literal with a time suffix (``ms``, ``s``, ``m``, ``h``, ``d``,
  ``w``).

.. _cluster.graceful_stop.force:

**cluster.graceful_stop.force**
  | *Default:*   ``false``
  | *Runtime:*  ``yes``

  Defines whether ``graceful stop`` should force stopping of the node if it
  runs into the timeout which is specified with the
  `cluster.graceful_stop.timeout`_ setting.

.. _conf_bulk_operations:

Bulk Operations
---------------

SQL DML Statements involving a huge amount of rows like :ref:`copy_from`,
:ref:`ref-insert` or :ref:`ref-update` can take an enormous amount of time and
resources. The following settings change the behaviour of those queries.

.. _bulk.request_timeout:

**bulk.request_timeout**
  | *Default:* ``1m``
  | *Runtime:* ``yes``

  Defines the timeout of internal shard-based requests involved in the
  execution of SQL DML Statements over a huge amount of rows.

.. _conf_discovery:

Discovery
---------

.. _discovery.zen.minimum_master_nodes:

**discovery.zen.minimum_master_nodes**
  | *Default:*   ``1``
  | *Runtime:*  ``yes``

  Set to ensure a node sees N other master eligible nodes to be considered
  operational within the cluster. It's recommended to set it to a higher value
  than 1 when running more than 2 nodes in the cluster.

.. _discovery.zen.ping_interval:

**discovery.zen.ping_interval**
  | *Default:*   ``1s``
  | *Runtime:*  ``yes``

  How often to ping other nodes.

  Nodes must remain responsive to pings or they will be marked as failed and
  removed from the cluster.

.. _discovery.zen.ping_timeout:

**discovery.zen.ping_timeout**
  | *Default:*   ``3s``
  | *Runtime:*  ``yes``

  The time to wait for ping responses from other nodes when discovering.
  Set this option to a higher value on a slow or congested network to minimize
  discovery failures.

.. _discovery.zen.ping_retries:

**discovery.zen.ping_retries**
  | *Default:*   ``3``
  | *Runtime:*  ``yes``

  How many ping failures (network timeouts) indicate that a node has failed.

.. _discovery.zen.publish_timeout:

**discovery.zen.publish_timeout**
  | *Default:*   ``30s``
  | *Runtime:*  ``yes``

  Time a node is waiting for responses from other nodes to a published cluster
  state.

.. NOTE::

   Multicast used to be an option for node discovery, but was deprecated in
   CrateDB 1.0.3 and removed in CrateDB 1.1.

.. _conf_host_discovery:

Unicast Host Discovery
......................

CrateDB has built-in support for several different mechanisms of node
discovery. The simplest mechanism is to specify a list of hosts in the
configuration file.

**discovery.zen.ping.unicast.hosts**
  | *Default:*  ``not set``
  | *Runtime:*  ``no``

Currently there are three other discovery types: via DNS, via EC2 API and via
Microsoft Azure mechanisms.

When a node starts up with one of these discovery types enabled, it performs a
lookup using the settings for the specified mechanism listed below. The hosts
and ports retrieved from the mechanism will be used to generate a list of
unicast hosts for node discovery.

The same lookup is also performed by all nodes in a cluster whenever the master
is re-elected (see `Cluster Meta Data`).

**discovery.zen.hosts_provider**
  | *Default:*   ``not set``
  | *Runtime:*   ``no``
  | *Allowed Values:* ``srv``, ``ec2``, ``azure``

See also: `Discovery`_.

.. _conf_dns_discovery:

Discovery via DNS
`````````````````

Crate has built-in support for discovery via DNS. To enable DNS discovery the
``discovery.zen.hosts_provider`` setting needs to be set to ``srv``.

The order of the unicast hosts is defined by the priority, weight and name of
each host defined in the SRV record. For example::

    _crate._srv.example.com. 3600 IN SRV 2 20 4300 crate1.example.com.
    _crate._srv.example.com. 3600 IN SRV 1 10 4300 crate2.example.com.
    _crate._srv.example.com. 3600 IN SRV 2 10 4300 crate3.example.com.

would result in a list of discovery nodes ordered like::

    crate2.example.com:4300, crate3.example.com:4300, crate1.example.com:4300

**discovery.srv.query**
  | *Runtime:*  ``no``

  The DNS query that is used to look up SRV records, usually in the format
  ``_service._protocol.fqdn`` If not set, the service discovery will not be
  able to look up any SRV records.

**discovery.srv.resolver**
  | *Runtime:*  ``no``

  The hostname or IP of the DNS server used to resolve DNS records. If this is
  not set, or the specified hostname/IP is not resolvable, the default (system)
  resolver is used.

  Optionally a custom port can be specified using the format ``hostname:port``.

.. _conf_ec2_discovery:

Discovery on Amazon EC2
```````````````````````

CrateDB has built-in support for discovery via the EC2 API. To enable EC2
discovery the ``discovery.zen.hosts_provider`` settings needs to be set to
``ec2``.

**discovery.ec2.access_key**
  | *Runtime:*  ``no``

  The access key ID to identify the API calls.

**discovery.ec2.secret_key**
  | *Runtime:*  ``no``

  The secret key to identify the API calls.

Following settings control the discovery:

.. _discovery_ec2_groups:

**discovery.ec2.groups**
  | *Runtime:*  ``no``

  A list of security groups; either by ID or name. Only instances with the
  given group will be used for unicast host discovery.

**discovery.ec2.any_group**
  | *Runtime:*  ``no``
  | *Default:*  ``true``

  Defines whether all (``false``) or just any (``true``) security group must
  be present for the instance to be used for discovery.

.. _discovery_ec2_host_type:

**discovery.ec2.host_type**
  | *Runtime:*  ``no``
  | *Default:*  ``private_ip``
  | *Allowed Values:*  ``private_ip``, ``public_ip``, ``private_dns``, ``public_dns``

  Defines via which host type to communicate with other instances.

.. _discovery_ec2_zones:

**discovery.ec2.availability_zones**
  | *Runtime:*  ``no``

  A list of availability zones. Only instances within the given availability
  zone will be used for unicast host discovery.

.. _discovery_ec2_tags:

**discovery.ec2.tag.<name>**
  | *Runtime:*  ``no``

  EC2 instances for discovery can also be filtered by tags using the
  ``discovery.ec2.tag.`` prefix plus the tag name.

  E.g. to filter instances that have the ``environment`` tags with the value
  ``dev`` your setting will look like: ``discovery.ec2.tag.environment: dev``.

**discovery.ec2.endpoint**
  | *Runtime:*  ``no``

  If you have your own compatible implementation of the EC2 API service you can
  set the endpoint that should be used.

.. _conf_azure_discovery:

Discovery on Microsoft Azure
````````````````````````````

CrateDB has built-in support for discovery via the Azure Virtual Machine API.
To enable Azure discovery set the ``discovery.zen.hosts_provider`` setting to
``azure``.

**cloud.azure.management.resourcegroup.name**
  | *Runtime:*  ``no``

  The name of the resource group the CrateDB cluster is running on.

  All nodes need to be started within the same resource group.

**cloud.azure.management.subscription.id**
  | *Runtime:*  ``no``

  The subscription ID of your Azure account.

  You can find the ID on the `Azure Portal`_.

**cloud.azure.management.tenant.id**
  | *Runtime:*  ``no``

  The tenant ID of the `Active Directory application`_.

**cloud.azure.management.app.id**
  | *Runtime:*  ``no``

  The application ID of the `Active Directory application`_.

**cloud.azure.management.app.secret**
  | *Runtime:*  ``no``

  The password of the `Active Directory application`_.

**discovery.azure.method**
  | *Runtime:* ``no``
  | *Default:* ``vnet``
  | *Allowed Values:* ``vnet | subnet``

  Defines the scope of the discovery. ``vnet`` will discover all VMs within the
  same virtual network (default), ``subnet`` will discover all VMs within the
  same subnet of the CrateDB instance.

.. _`Azure Portal`: https://portal.azure.com
.. _`Active Directory application`: https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal-cli/#_create-ad-application-with-password

.. _conf_routing:

Routing Allocation
------------------

.. _cluster.routing.allocation.enable:

**cluster.routing.allocation.enable**
  | *Default:*   ``all``
  | *Runtime:*  ``yes``
  | *Allowed Values:* ``all | none | primaries | new_primaries``

  ``all`` allows all shard allocations, the cluster can allocate all kinds of
  shards.

  ``none`` allows no shard allocations at all. No shard will be moved or
  created.

  ``primaries`` only primaries can be moved or created. This includes existing
  primary shards.

  ``new_primaries`` allows allocations for new primary shards only. This means
  that for example a newly added node will not allocate any replicas. However
  it is still possible to allocate new primary shards for new indices. Whenever
  you want to perform a zero downtime upgrade of your cluster you need to set
  this value before gracefully stopping the first node and reset it to ``all``
  after starting the last updated node.

.. NOTE::

   This allocation setting has no effect on recovery of primary shards! Even
   when ``cluster.routing.allocation.enable`` is set to ``none``, nodes will
   recover their unassigned local primary shards immediatelly after restart.

.. _cluster.routing.rebalance.enable:

**cluster.routing.rebalance.enable**
  | *Default:*   ``all``
  | *Runtime:*  ``yes``
  | *Allowed Values:* ``all | none | primaries | replicas``

  Enables/Disables rebalancing for different types of shards.

  ``all`` allows shard rebalancing for all types of shards.

  ``none`` disables shard rebalancing for any types.

  ``primaries`` allows shard rebalancing only for primary shards.

  ``replicas`` allows shard rebalancing only for replica shards.

.. _cluster.routing.allocation.allow_rebalance:

**cluster.routing.allocation.allow_rebalance**
  | *Default:*   ``indices_all_active``
  | *Runtime:*  ``yes``
  | *Allowed Values:* ``always | indices_primary_active | indices_all_active``

  Allow to control when rebalancing will happen based on the total state of all
  the indices shards in the cluster. Defaulting to ``indices_all_active`` to
  reduce chatter during initial recovery.

.. _cluster.routing.allocation.cluster_concurrent_rebalance:

**cluster.routing.allocation.cluster_concurrent_rebalance**
  | *Default:*   ``2``
  | *Runtime:*  ``yes``

  Define how many concurrent rebalancing tasks are allowed cluster wide.

.. _cluster.routing.allocation.node_initial_primaries_recoveries:

**cluster.routing.allocation.node_initial_primaries_recoveries**
  | *Default:*   ``4``
  | *Runtime:*  ``yes``

  Define the number of initial recoveries of primaries that are allowed per
  node. Since most times local gateway is used, those should be fast and we can
  handle more of those per node without creating load.

.. _cluster.routing.allocation.node_concurrent_recoveries:

**cluster.routing.allocation.node_concurrent_recoveries**
  | *Default:*   ``2``
  | *Runtime:*  ``yes``

  How many concurrent recoveries are allowed to happen on a node.

.. _conf-routing-allocation-awareness:

Awareness
.........

Cluster allocation awareness allows to configure shard and replicas allocation
across generic attributes associated with nodes.

.. _cluster.routing.allocation.awareness.attributes:

**cluster.routing.allocation.awareness.attributes**
  | *Runtime:*  ``no``

  Define node attributes which will be used to do awareness based on the
  allocation of a shard and its replicas. For example, let's say we have
  defined an attribute ``rack_id`` and we start 2 nodes with
  ``node.attr.rack_id`` set to rack_one, and deploy a single table with 5
  shards and 1 replica. The table will be fully deployed on the current nodes
  (5 shards and 1 replica each, total of 10 shards).

  Now, if we start two more nodes, with ``node.attr.rack_id`` set to rack_two,
  shards will relocate to even the number of shards across the nodes, but a
  shard and its replica will not be allocated in the same rack_id value.

  The awareness attributes can hold several values

.. _cluster.routing.allocation.awareness.force.*.values:

**cluster.routing.allocation.awareness.force.\*.values**
  | *Runtime:*  ``no``

  Attributes on which shard allocation will be forced. ``*`` is a placeholder
  for the awareness attribute, which can be defined using the
  `cluster.routing.allocation.awareness.attributes`_ setting. Let's say we
  configured an awareness attribute ``zone`` and the values ``zone1, zone2``
  here, start 2 nodes with ``node.attr.zone`` set to ``zone1`` and create a
  table with 5 shards and 1 replica. The table will be created, but only 5
  shards will be allocated (with no replicas). Only when we start more shards
  with ``node.attr.zone`` set to ``zone2`` the replicas will be allocated.

Balanced Shards
...............

All these values are relative to one another. The first three are used to
compose a three separate weighting functions into one. The cluster is balanced
when no allowed action can bring the weights of each node closer together by
more then the fourth setting. Actions might not be allowed, for instance, due
to forced awareness or allocation filtering.

.. _cluster.routing.allocation.balance.shard:

**cluster.routing.allocation.balance.shard**
  | *Default:*   ``0.45f``
  | *Runtime:*  ``yes``

  Defines the weight factor for shards allocated on a node (float). Raising
  this raises the tendency to equalize the number of shards across all nodes in
  the cluster.

.. _cluster.routing.allocation.balance.index:

**cluster.routing.allocation.balance.index**
  | *Default:*   ``0.55f``
  | *Runtime:*  ``yes``

  Defines a factor to the number of shards per index allocated on a specific
  node (float). Increasing this value raises the tendency to equalize the
  number of shards per index across all nodes in the cluster.

.. _cluster.routing.allocation.balance.threshold:

**cluster.routing.allocation.balance.threshold**
  | *Default:*   ``1.0f``
  | *Runtime:*  ``yes``

  Minimal optimization value of operations that should be performed (non
  negative float). Increasing this value will cause the cluster to be less
  aggressive about optimising the shard balance.

Cluster-Wide Allocation Filtering
.................................

Allow to control the allocation of all shards based on include/exclude filters.

E.g. this could be used to allocate all the new shards on the nodes with
specific IP addresses or custom attributes.

.. _cluster.routing.allocation.include.*:

**cluster.routing.allocation.include.***
  | *Runtime:*  ``no``

  Place new shards only on nodes where one of the specified values matches the
  attribute. e.g.: cluster.routing.allocation.include.zone: "zone1,zone2"

.. _cluster.routing.allocation.exclude.*:

**cluster.routing.allocation.exclude.***
  | *Runtime:*  ``no``

  Place new shards only on nodes where none of the specified values matches the
  attribute. e.g.: cluster.routing.allocation.exclude.zone: "zone1"

.. _cluster.routing.allocation.require.*:

**cluster.routing.allocation.require.***
  | *Runtime:*  ``no``

  Used to specify a number of rules, which all MUST match for a node in order
  to allocate a shard on it. This is in contrast to include which will include
  a node if ANY rule matches.

Disk-based Shard Allocation
...........................

.. _cluster.routing.allocation.disk.threshold_enabled:

**cluster.routing.allocation.disk.threshold_enabled**
  | *Default:*   ``true``
  | *Runtime:*  ``yes``

  Prevent shard allocation on nodes depending of the disk usage.

.. _cluster.routing.allocation.disk.watermark.low:

**cluster.routing.allocation.disk.watermark.low**
  | *Default:*   ``85%``
  | *Runtime:*  ``yes``

  Defines the lower disk threshold limit for shard allocations. New shards will
  not be allocated on nodes with disk usage greater than this value. It can
  also be set to an absolute bytes value (like e.g. ``500mb``) to prevent the
  cluster from allocating new shards on node with less free disk space than
  this value.

.. _cluster.routing.allocation.disk.watermark.high:

**cluster.routing.allocation.disk.watermark.high**
  | *Default:*   ``90%``
  | *Runtime:*  ``yes``

  Defines the higher disk threshold limit for shard allocations. The cluster
  will attempt to relocate existing shards to another node if the disk usage on
  a node rises above this value. It can also be set to an absolute bytes value
  (like e.g. ``500mb``) to relocate shards from nodes with less free disk space
  than this value.

.. _cluster.routing.allocation.disk.watermark.flood_stage:

**cluster.routing.allocation.disk.watermark.flood_stage**
  | *Default:*  ``95%``
  | *Runtime:*  ``yes``

  Defines the threshold on which CrateDB enforces a read-only block on every
  index that has at least one shard allocated on a node with at least one disk
  exceeding the flood stage.
  Note, that the read-only blocks are not automatically removed from the
  indices if the disk space is freed and the threshold is undershot. To remove
  the block, execute ``ALTER TABLE ... SET ("blocks.read_only_allow_delete" =
  FALSE)`` for affected tables (see :ref:`table-settings-blocks.read_only_allow_delete`).

``cluster.routing.allocation.disk.watermark`` settings may be defined as
percentages or bytes values. However, it is not possible to mix the value
types.

By default, the cluster will retrieve information about the disk usage of the
nodes every 30 seconds. This can also be changed by setting the
`cluster.info.update.interval`_ setting.

.. NOTE::

   The watermark settings are also used for the
   :ref:`node_checks_watermark_low` and :ref:`node_checks_watermark_high` node
   check. Setting ``cluster.routing.allocation.disk.threshold_enabled`` to
   false will disable the allocation decider, but the node checks will still be
   active and warn users about running low on disk space.

Recovery
--------

.. _indices.recovery.max_bytes_per_sec:

**indices.recovery.max_bytes_per_sec**
  | *Default:*   ``40mb``
  | *Runtime:*  ``yes``

  Specifies the maximum number of bytes that can be transferred during shard
  recovery per seconds. Limiting can be disabled by setting it to ``0``. This
  setting allows to control the network usage of the recovery process. Higher
  values may result in higher network utilization, but also faster recovery
  process.

.. _indices.recovery.retry_delay_state_sync:

**indices.recovery.retry_delay_state_sync**
  | *Default:*  ``500ms``
  | *Runtime:*  ``yes``

  Defines the time to wait after an issue caused by cluster state syncing
  before retrying to recover.

.. _indices.recovery.retry_delay_network:

**indices.recovery.retry_delay_network**
  | *Default:*  ``5s``
  | *Runtime:*  ``yes``

  Defines the time to wait after an issue caused by the network before retrying
  to recover.

.. _indices.recovery.internal_action_timeout:

**indices.recovery.internal_action_timeout**
  | *Default:*  ``15m``
  | *Runtime:*  ``yes``

  Defines the timeout for internal requests made as part of the recovery.

.. _indices.recovery.internal_action_long_timeout:

**indices.recovery.internal_action_long_timeout**
  | *Default:*  ``30m``
  | *Runtime:*  ``yes``

  Defines the timeout for internal requests made as part of the recovery that
  are expected to take a long time. Defaults to twice
  :ref:`internal_action_timeout <indices.recovery.internal_action_timeout>`.

.. _indices.recovery.recovery_activity_timeout:

**indices.recovery.recovery_activity_timeout**
  | *Default:*  ``30m``
  | *Runtime:*  ``yes``

  Recoveries that don't show any activity for more then this interval will
  fail. Defaults to :ref:`internal_action_long_timeout
  <indices.recovery.internal_action_long_timeout>`.

Query Circuit Breaker
---------------------

The Query circuit breaker will keep track of the used memory during the
execution of a query. If a query consumes too much memory or if the cluster is
already near its memory limit it will terminate the query to ensure the cluster
keeps working.

**indices.breaker.query.limit**
  | *Default:*   ``60%``
  | *Runtime:*   ``yes``

  Specifies the limit for the query breaker. Provided values can either be
  absolute values (interpreted as a number of bytes), byte sizes (eg. 1mb) or
  percentage of the heap size (eg. 12%). A value of ``-1`` disables breaking
  the circuit while still accounting memory usage.

**indices.breaker.query.overhead**
  | *Default:*   ``1.09``
  | *Runtime:*   ``no``

  A constant that all data estimations are multiplied with to determine a final
  estimation.

Field Data Circuit Breaker
--------------------------

The field data circuit breaker allows estimation of needed heap memory required
for loading field data into memory. If a certain limit is reached an exception
is raised.

**indices.breaker.fielddata.limit**
  | *Default:*   ``60%``
  | *Runtime:*  ``yes``

  Specifies the JVM heap limit for the fielddata breaker.

**indices.breaker.fielddata.overhead**
  | *Default:*   ``1.03``
  | *Runtime:*  ``yes``

  A constant that all field data estimations are multiplied with to determine a
  final estimation.

Request Circuit Breaker
-----------------------

The request circuit breaker allows an estimation of required heap memory per
request. If a single request exceeds the specified amount of memory, an
exception is raised.

**indices.breaker.request.limit**
  | *Default:*   ``60%``
  | *Runtime:*  ``yes``

  Specifies the JVM heap limit for the request circuit breaker.

**indices.breaker.request.overhead**
  | *Default:*   ``1.0``
  | *Runtime:*  ``yes``

  A constant that all request estimations are multiplied with to determine a
  final estimation.

Accounting Circuit Breaker
--------------------------

Tracks things that are held in memory independent of queries. For example the
memory used by Lucene for segments.

**indices.breaker.accounting.limit**
  | *Default:*  ``100%``
  | *Runtime:*  ``yes``

  Specifies the JVM heap limit for the accounting circuit breaker


**indices.breaker.accounting.overhead**
  | *Default:*  ``1.0``
  | *Runtime:*  ``yes``

  A constant that all accounting estimations are multiplied with to determine a
  final estimation.

.. _stats.breaker.log:

Stats Circuit Breakers
----------------------

Settings that control the behaviour of the stats circuit breaker. There are two
breakers in place, one for the jobs log and one for the operations log. For
each of them, the breaker limit can be set.

.. _stats.breaker.log.jobs.limit:

**stats.breaker.log.jobs.limit**
  | *Default:*    ``5%``
  | *Runtime:*   ``yes``

  The maximum memory that can be used from :ref:`CRATE_HEAP_SIZE
  <conf-env-heap-size>` for the :ref:`sys.jobs_log <sys-logs>` table on each
  node.

  When this memory limit is reached, the job log circuit breaker logs an error
  message and clears the :ref:`sys.jobs_log <sys-logs>` table completely.

.. _stats.breaker.log.operations.limit:

**stats.breaker.log.operations.limit**
  | *Default:*    ``5%``
  | *Runtime:*   ``yes``

  The maximum memory that can be used from :ref:`CRATE_HEAP_SIZE
  <conf-env-heap-size>` for the :ref:`sys.operations_log <sys-logs>` table on
  each node.

  When this memory limit is reached, the operations log circuit breaker logs an
  error message and clears the :ref:`sys.operations_log <sys-logs>` table
  completely.

Thread Pools
------------

Every node holds several thread pools to improve how threads are managed within
a node. There are several pools, but the important ones include:

* ``index``: For index/delete operations, defaults to fixed
* ``search``: For count/search operations, defaults to fixed
* ``get``: For queries that are optimized to do a direct lookup by primary key,
  defaults to fixed
* ``bulk``: For bulk operations, defaults to fixed
* ``refresh``: For refresh operations, defaults to cache

**thread_pool.<name>.type**
  | *Runtime:*  ``no``
  | *Allowed Values:* ``fixed | scaling``

  ``fixed`` holds a fixed size of threads to handle the requests. It also has a
  queue for pending requests if no threads are available.

  ``scaling`` ensures that a thread pool holds a dynamic number of threads that
  are proportional to the workload.

Settings for fixed thread pools
...............................

If the type of a thread pool is set to ``fixed`` there are a few optional
settings.

**thread_pool.<name>.size**
  | *Runtime:*  ``no``

  Number of threads. The default size of the different thread pools depend on
  the number of available CPU cores.

**thread_pool.<name>.queue_size**
  | *Default index:*  ``200``
  | *Default search:* ``1000``
  | *Default get:* ``1000``
  | *Default bulk:* ``50``
  | *Runtime:*  ``no``

  Size of the queue for pending requests. A value of ``-1`` sets it to
  unbounded.

Metadata
--------

.. _cluster.info.update.interval:

**cluster.info.update.interval**
  | *Default:*  ``30s``
  | *Runtime:*  ``yes``

  Defines how often the cluster collect metadata information (e.g. disk usages
  etc.) if no concrete  event is triggered.

.. _metadata_gateway:

Metadata Gateway
................

  The gateway persists cluster meta data on disk every time the meta data
  changes. This data is stored persistently across full cluster restarts and
  recovered after nodes are started again.

.. _gateway.expected_nodes:

**gateway.expected_nodes**
  | *Default:*   ``-1``
  | *Runtime:*  ``no``

  The setting ``gateway.expected_nodes`` defines the number of nodes that
  should be waited for until the cluster state is recovered immediately. The
  value of the setting should be equal to the number of nodes in the cluster,
  because you only want the cluster state to be recovered after all nodes are
  started.

.. _gateway.recover_after_time:

**gateway.recover_after_time**
  | *Default:*   ``0ms``
  | *Runtime:*  ``no``

  The ``gateway.recover_after_time`` setting defines the time to wait before
  starting starting the recovery once the number of nodes defined in
  ``gateway.recover_after_nodes`` are started. The setting is relevant if
  ``gateway.recover_after_nodes`` is less than ``gateway.expected_nodes``.

.. _gateway.recover_after_nodes:

**gateway.recover_after_nodes**
  | *Default:*   ``-1``
  | *Runtime:*  ``no``

  The ``gateway.recover_after_nodes`` setting defines the number of nodes that
  need to be started before the cluster state recovery will start. Ideally the
  value of the setting should be equal to the number of nodes in the cluster,
  because you only want the cluster state to be recovered once all nodes are
  started. However, the value must be bigger than the half of the expected
  number of nodes in the cluster.

.. _s3-credentials:

Credentials for S3 Repositories
...............................

CrateDB has built-in support for configuring
:ref:`S3 buckets as repositories for snapshots
<ref-create-repository-types-s3>`. If no credentials are provided as parameters
to the SQL statement the following default credentials will be used:

.. _s3-credentials-access-key:

**s3.client.default.access_key**
  | *Runtime:*  ``no``

  The access key ID to identify the API calls.

.. _s3-credentials-secret-key:

**s3.client.default.secret_key**
  | *Runtime:*  ``no``

  The secret key to identify the API calls.


.. TIP::

   Configuring the settings above in the ``crate.yml`` file, is an easy way to
   prevent credentials from being exposed.

   If a repository is created with the credentials passed as parameters to the
   SQL statement, then those credentials will be visible as plain text to
   anyone querying the :ref:`sys.repositories table <sys-repositories>`.
