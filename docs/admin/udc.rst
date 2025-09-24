.. _usage_data_collector:

====================
Usage Data Collector
====================

The CrateDB Usage Data Collector (UDC) is a sub-system that gathers usage data,
reporting it to the UDC server at https://udc.crate.io. It is easy to disable,
and does not collect any data that is confidential. For more information about
what is being sent, see below.

CrateDB uses this information as a form of automatic, effortless feedback from
the community. We want to verify that we are doing the right thing by matching
download statistics with usage statistics. After each release, we can see if
there is a larger retention span of the server software.

The data collected is clearly stated here. If any future versions of this
system collect additional data, we will clearly announce those changes.

CrateDB is concerned about your privacy. We do not disclose any personally
identifiable information.

Technical information
=====================

To gather good statistics about CrateDB usage, UDC collects this information:

================  =========================================================
Name              Description
================  =========================================================
Kernel Version    The build number, and if there are any modifications to
                  the kernel.
Cluster Id        A randomized globally unique ID created every time the
                  whole cluster is restarted.
Master            Boolean whether the current node is master.
Ping Count        UDC holds an internal counter per node which is
                  incremented for every ping, and reset on every restart of
                  the a node.
CrateDB Version   The CrateDB version.
Java Version      The Java version CrateDB is currently running with.
Hardware Address  MAC address to uniquely identify instances behind
                  firewalls.
Processor count   Number of available CPUs as reported by
                  ``Runtime.availableProcessors``
Enterprise        Identifies whether the Enterprise Edition is used. [#f1]_
================  =========================================================

After startup, UDC waits for 10 minutes before sending the first ping. It does
this for two reasons; first, we don't want the startup to be slower because of
UDC, and secondly, we want to keep pings from automatic tests to a minimum. By
default, UDC is sending pings every 24 hours. The ping to the UDC servers is
done with a HTTP GET.

Admin UI tracking
=================

Since Admin UI v0.16.0 we are tracking the user ID along with the cluster ID to
know how many active users are currently using CrateDB.

Configuration
=============

The Usage Data Collector can be configured by adapting the ``crate.yml``
configuration file or adding a system property setting. Refer to
:ref:`conf_usage_data_collector` to see how these settings can be accessed and
how they are configured.

How to disable UDC
==================

Below are two ways you can disable UDC. However we hope you support us offering
the open source edition, and leave UDC on, so we learn how many people use
CrateDB.


.. highlight:: yaml

By configuration
----------------

Just add following to your ``crate.yml`` configuration file::

    udc.enabled:  false

By system property
------------------

If you do not want to make any change to the jars or to the configuration,
a system property setting like this will also make sure that UDC is never
activated::

    -Cudc.enabled=false


.. [#f1] The "CrateDB Enterprise Edition" has been dissolved starting with
         CrateDB 4.5.0, see also `Farewell to the CrateDB Enterprise License`_.


.. _Farewell to the CrateDB Enterprise License: https://cratedb.com/blog/farewell-to-the-cratedb-enterprise-license-faq
