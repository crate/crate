.. highlight:: psql
.. _analyze:

===========
``ANALYZE``
===========

Synopsis
========

::

    ANALYZE


Description
===========


The ``ANALYZE`` command can be used to collect statistics about the contents of
the tables in the CrateDB cluster. 

The generated statistics can be viewed in the :ref:`pg_catalog.pg_stats
<pg_stats>` table.

The query optimizer uses some of those statistics to generate better execution
plans.

The statistics are also periodically updated. How often can be configured with
the :ref:`stats.service.interval <stats.service.interval>` setting.

I/O throughput during collection of statistics can be throttled with the
:ref:`stats.service.max_bytes_per_sec <stats.service.max_bytes_per_sec>`
setting. Changes to this setting can be made and take effect while an analysis
is in progress, thus it's possible to adjust optimal value for a concrete setup
by trying different values while ``ANALYZE`` is running.
