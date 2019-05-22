.. _ddl_shard_allocation:

============================
 Shard allocation filtering
============================

Shard allocation filters allows to configure shard and replicas allocation per
table across generic attributes associated with nodes.

.. NOTE::

   The per-table shard allocation filtering works in conjunction with
   :ref:`Cluster Level Allocation <conf_routing>`.

It is possible to assign certain attributes to a node, see
:ref:`conf-node-attributes`.

These attributes can be used with `routing.allocation.*` settings to allocate a
table to a particular group of nodes.

Settings
========

The following settings are dynamic, allowing tables to be allocated (when
defined on table creation) or moved (when defined by altering a table) from one
set of nodes to another:

:routing.allocation.include.{attribute}:
   Assign the table to a node whose *{attribute}* has **at least one** of the
   comma-separated values.

:routing.allocation.require.{attribute}:
   Assign the table to a node whose *{attribute}* has **all** of the comma-separated
   values.

:routing.allocation.exclude.{attribute}:
   Assign the table to a node whose *{attribute}* has **none** of the
   comma-separated values.

Special attributes
==================

Following special attributes are supported:

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
