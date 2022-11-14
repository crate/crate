.. _ddl_shard_allocation:

============================
 Shard allocation filtering
============================

:ref:`Shard allocation <gloss-shard-allocation>` filters allows to configure
shard and replicas allocation per table across generic attributes associated
with nodes.

.. NOTE::

   The per-table shard allocation filtering works in conjunction with
   :ref:`Cluster Level Allocation <conf_routing>`.

It is possible to assign certain attributes to a node, see
:ref:`conf-node-attributes`.

These attributes can be used with `routing.allocation.*` settings to allocate a
table to a particular group of nodes.

Settings
========

The following settings are dynamic, allowing tables to be :ref:`allocated
<gloss-shard-allocation>` (when defined on table creation) or moved (when
defined by altering a table) from one set of nodes to another:

:routing.allocation.include.{attribute}:
   Assign the table to a node whose *{attribute}* has **at least one** of the
   comma-separated values.

:routing.allocation.require.{attribute}:
   Assign the table to a node whose *{attribute}* has **all** of the comma-separated
   values.

:routing.allocation.exclude.{attribute}:
   Assign the table to a node whose *{attribute}* has **none** of the
   comma-separated values.

.. NOTE::

    These settings are not mutually exclusive. You can for instance have a
    table with ``"routing.allocation.require.storage" = 'ssd'`` and 
    ``"routing.allocation.exclude.datacenterzone" = 'zoneA'``.

Special attributes
==================

Following special attributes are supported:

.. vale off

:_name:
   Match nodes by node name.

:_host_ip:
   Match nodes by host IP address (IP associated with hostname).

:_publish_ip:
   Match nodes by publish IP address.

:_ip:
   Match either _host_ip or _publish_ip.

:_host:
   Match nodes by hostname.

.. vale on
