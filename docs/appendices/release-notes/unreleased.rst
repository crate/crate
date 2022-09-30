==================
Unreleased Changes
==================

.. NOTE::

    These changes have not yet been released.

    If you are viewing this document on the Crate.io website, these changes
    reflect what exists on `the master branch`_ in Git. This is where we
    collect changes before they are ready for release.

.. WARNING::

    Unreleased changes may not be ready for general use and could lead to data
    corruption or data loss. You should `back up your data`_ before
    experimenting with unreleased changes.

.. _the master branch: https://github.com/crate/crate
.. _back up your data: https://crate.io/docs/crate/reference/en/latest/admin/snapshots.html

.. DEVELOPER README
.. ================

.. Changes should be recorded here as you are developing CrateDB. When a new
.. release is being cut, changes will be moved to the appropriate release notes
.. file.

.. When resetting this file during a release, leave the headers in place, but
.. add a single paragraph to each section with the word "None".

.. Always cluster items into bigger topics. Link to the documentation whenever feasible.
.. Remember to give the right level of information: Users should understand
.. the impact of the change without going into the depth of tech.

.. rubric:: Table of contents

.. contents::
   :local:


Breaking Changes
================

- Removed the ``node.store.allow_mmapfs`` setting. It was deprecated in 4.1.0
  in favour of the ``node.store.allow_mmap`` setting.

- Removed the ``indices.breaker.fielddata.limit`` setting and the ``*.overhead``
  settings for all circuit breakers. They were deprecated in 4.3.0 and had no
  effect since then.

- Removed the deprecated ``discovery.zen.publish_timeout``,
  ``discovery.zen.commit_timeout``, ``discovery.zen.no_master_block``,
  ``discovery.zen.publish_diff.enable`` settings.
  They had no effect since 4.0.0 and have been deprecated in 4.4.0.

- Removed the deprecated azure discovery functionality.

- Fields referencing ``catalog`` in :ref:`information_schema <information_schema>`
  tables now return ``'crate'`` (the only catalog in CrateDB) instead of the
  table ``schema``.

Deprecations
============

- Deprecated the ``upgrade_segments`` option of the
  :ref:`OPTIMIZE TABLE <sql-optimize>` statement. The option will now longer
  have any effect and will be removed in the future.


Changes
=======

SQL Statements
--------------

- Added initial support for cursors. See :ref:`DECLARE <sql-declare>`,
  :ref:`FETCH <sql-fetch>` and :ref:`CLOSE <sql-close>`.

- Added support for the :ref:`EXISTS <sql_dql_exists>` expression.

- Added support for correlated scalar sub-queries within the select list of a
  query. See :ref:`Scalar subquery <sql-scalar-subquery>`.

- Added support of ``GROUP BY`` on :ref:`ARRAY <type-array>` typed columns.

SQL Standard And PostgreSQL Schema Compatibility
------------------------------------------------

- Added support for ``SET TIME ZONE`` to improve PostgreSQL Compatibility.
  Timezone will be ignored on the server side.

- Added a :ref:`application_name <conf-session-application-name>` session
  setting that can be used to identify clients or applications which connect to
  a CrateDB node.

- Added support for ``catalog`` in fully qualified table and column names,
  i.e.::

    SELECT * FROM crate.doc.t1;
    SELECT crate.doc.t1.a, crate.doc.t1.b FROM crate.doc.t1;

- Made the commas between successive ``transaction_modes`` of the
  :ref:`BEGIN <ref-begin>` and its SQL equivalent
  :ref:`START TRANSACTION <sql-start-transaction>` statement optional to support
  compatibility with clients and tools using an older (< 8.0) PostgreSQL syntax.

- Changed the :ref:`interval <type-interval>` parameter of
  :ref:`date_trunc <scalar-date_trunc>` to be case insensitive.

- Added support for ``'YES'``, ``'ON'`` and ``'1'`` as alternative way to
  specify a ``TRUE`` boolean constant and ``'NO'``, ``'OFF'`` and ``'0'`` as
  alternative way to specify ``FALSE`` boolean constant improving compatibility
  with PostgreSQL.

- Added support for casting :ref:`TIMESTAMP <type-timestamp>` and
  :ref:`TIMESTAMP WITHOUT TIME ZONE <type-timestamp-without-tz>` values to the
  :ref:`DATE <type-date>` data type and vice versa.

Performance Improvements
------------------------

- Improve performance of queries on :ref:`sys.snapshots <sys-snapshots>`.

Administration and Operations
-----------------------------

- Updated to Admin UI 1.23.1, which improves scrolling behavior on wide result
  sets, and fixes formatting of :ref:`TIMESTAMP WITHOUT TIME ZONE
  <type-timestamp-without-tz>` values in query console result table.

- Added I/O throughput throttling of the :ref:`analyze` statement as well as of
  the periodic statistic collection controlled by the
  :ref:`stats.service.interval` setting to lower the impact on the cluster
  load. This throttling can be controlled by a new setting
  :ref:`stats.service.max_bytes_per_sec <stats.service.max_bytes_per_sec>` and
  is set 40MB/s by default.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue causing queries with matching on ``_id`` to not get rows
  from :ref:`translog <concept-addressing-documents>`, and therefore only
  rows that were visible from the latest manual or automatic
  :ref:`REFRESH <sql-refresh>` were returned.

- Fixed an issue causing an ``IllegalArgumentException`` to be thrown when the
  optimizer attempts to convert a ``LEFT JOIN`` to an ``INNER JOIN`` and there
  is also a subquery in the ``WHERE`` clause.

- Fixed a file descriptor leak that was triggered by querying the ``os`` column
  of the ``sys.nodes`` table.

- Fixed an issue that could lead to a ``NoSuchElementException`` when using the
  JDBC client and mixing different DML statements using the ``addBatch``
  functionality.

- Fixed an issue that could lead to stuck queries.

- Fixed ``EXPLAIN`` plan output for queries with a ``WHERE`` clause containing
  implicit cast symbols. A possible optimization of our planner/optimizer was
  not used, resulting in different output than actually used on plan execution.
