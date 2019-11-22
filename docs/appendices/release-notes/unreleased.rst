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
.. _back up your data: https://crate.io/a/backing-up-and-restoring-crate/

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

- Changed arithmetic operations ``*``, ``+`` and ``-`` of types ``integer``
  and ``bigint`` to throw an exception instead of rolling over from positive
  to negative or the other way around.

- Changed how columns of type :ref:`geo_point_data_type` are being communicated
  to PostgreSQL clients: Before clients were told that those columns are double
  arrays, now they're correctly mapped to the PostgreSQL ``point`` type. This
  means that applications using clients like ``JDBC`` will have to be adapted
  to use ``PgPoint``. (See `Geometric DataTypes in JDBC
  <https://jdbc.postgresql.org/documentation/head/geometric.html>`_)

Deprecations
============

- Deprecated the `node.store.allow_mmapfs` setting in favour of
  :ref:`node.store.allow_mmap <node.store_allow_mmap>`.

Changes
=======

- The optimizer now utilizes internal statistics to approximate the number of
  rows returned by various parts of a query plan. This should result in more
  efficient execution plans for joins.

- Added a :ref:`ANALYZE <analyze>` command that can be used to update
  statistical data about the contents of the tables in the CrateDB cluster.
  This data is visible in a newly added :ref:`pg_stats <pg_stats>` table.

- Added support for casting to :ref:`geo_point_data_type`,
  :ref:`geo_shape_data_type` and :ref:`object_data_type` array data types.
  For example: ``cast(['POINT(2 3)','POINT(1 3)'] AS array(geo_point))``

- Added a ``failures`` column to the :ref:`sys.snapshots <sys-snapshots>`
  table.

- Changed the default garbage collector that is being used from Concurrent Mark
  Sweep to G1GC.

- Added support for CIDR notation comparisons through special purpose
  operator ``<<`` associated with type ip.
  Statements like ``192.168.0.0 << 192.168.0.1/24`` are true,
  ``select ip from ips_table where ip << 192.168.0.1/24`` returns
  matching :ref:`ip <ip-type>` addresses.

- Added the ``ltrim`` and ``rtrim`` scalar functions.

- Improved the error messages that were returned if a relation or schema is not
  found. They now may include suggestions for similarly named tables. This
  should make typos more apparent and can help users figure out that they were
  missing double quotes in case the table names contain upper case letters.

- Added a ``seq_no_stats`` and a ``translog_stats`` column to the
  :ref:`sys.shards <sys-shards>` table.

- Added new system table :ref:`sys.segments <sys-segments>` which contains
  information about the Lucene segments of a shard.

- Added a ``node`` column to :ref:`sys.jobs_log <sys-logs>`.

- Added support for the operator :ref:`ILIKE <sql_dql_like>`, the case
  insensitive complement to ``LIKE``.

- Reduced recovery time by sending file-chunks concurrently. It applies
  only for when transport communication is secured or compressed. The number of
  chunks is controlled by the :ref:`indices.recovery.max_concurrent_file_chunks
  <indices.recovery.max_concurrent_file_chunks>` setting.

- Allow user to control how table data is stored and accessed on a disk
  via the :ref:`store.type <table_parameter.store_type>` table parameter and
  :ref:`node.store.allow_mmap <node.store_allow_mmap>` node setting.

- Changed the default table data store type from ``mmapfs`` to ``hybridfs``.

- Added ``phonetic`` token filter with following encoders: ``metaphone``,
  ``double_metaphone``, ``soundex``, ``refined_soundex``, ``caverphone1``,
  ``caverphone2``, ``cologne``, ``nysiis``, ``koelnerphonetik``,
  ``haasephonetik``, ``beider_morse``, ``daitch_mokotoff``.

- Removed a restriction for predicates in the ``WHERE`` clause involving
  partitioned by columns which could result in a failure response with the
  message ``logical conjunction of the conditions in the WHERE clause which
  involve partitioned columns led to a query that can't be executed``.

- Added a dynamic bulk sizing mechanism that should prevent ``INSERT INTO ...
  FROM query`` operations to run into out of memory errors if the individual
  records of a table are very large.

- Improved resiliency of ``ALTER TABLE ADD`` operation.

- Improved resiliency of ``ALTER TABLE`` operation.

- Extended :ref:`CONCAT <scalar_concat>` to do implicit casts, so that calls
  like ``SELECT 't' || 5`` are supported.

- Added the :ref:`INTERVAL <interval_data_type>` datatype and extended
  :ref:`table-functions-generate-series` to work with timestamps and the
  new :ref:`INTERVAL <interval_data_type>` type

- Added the :ref:`PG_TYPEOF <pg_typeof>` system function.

- Support implicit object creation in update statements. E.g. ``UPDATE t SET
  obj['x'] = 10`` will now implicitly set ``obj`` to ``{obj: {x: 10}}`` on rows
  where ``obj`` was previously ``null``.

- Added :ref:`LPAD <scalar-lpad>` and :ref:`RPAD <scalar-rpad>` scalar functions.

- Added the :ref:`table_parameter.codec` parameter to :ref:`ref-create-table`
  to control the compression algorithm used to store data.

- Added :ref:`AT TIME ZONE <timestamp-at-time-zone>` syntax.

- Added the :ref:`cluster.routing.allocation.total_shards_per_node
  <cluster.routing.allocation.total_shards_per_node>` setting.

- Added :ref:`TIMEZONE <scalar-timezone>` scalar function.

- Added support for the filter clause in
  :ref:`aggregate expressions <aggregate-expressions>` and
  :ref:`window functions <window-function-call>` that are
  :ref:`aggregates <aggregation>`.

- Added support for `offset PRECEDING` and `offset FOLLOWING`
  :ref:`window definitions <window-definition>`.

- Added support for using :ref:`ref-values` as top-level relation.

- Added an optimization that allows to run `WHERE` clauses on top of
  derived tables containing :ref:`table functions <ref-table-functions>`
  more efficiently in some cases.

- Statements containing limits, filters, window functions or table functions
  will now be labelled accordingly in :ref:`sys-jobs-metrics`.

- Added support for the :ref:`named window definition <named-windows>`.
  It allows a user to define a list of window definitions in the
  :ref:`sql_reference_window` clause that can be referenced in :ref:`over`
  clauses.

- Add support for ``ROWS`` frame definitions in the context of window functions
  :ref:`window definitions <window-definition>`.

- The ``node`` argument of the :ref:`REROUTE <alter_table_reroute>` commands of
  :ref:`ref-alter-table` can now either be the id or the name of a node.

- Added a :ref:`PROMOTE REPLICA <alter_table_reroute>` sub command to
  :ref:`ref-alter-table`.

- Added support for the :ref:`lag <window-function-lag>` and
  :ref:`lead <window-function-lead>` window functions as enterprise features.

- Changed the default for :ref:`sql_ref_write_wait_for_active_shards` from
  ``ALL`` to ``1``. This will improve the out of box experience as it allows a
  subset of nodes to become unavailable without blocking write operations. See
  the documentation for more details about the implications.

- Added left and right scalar functions.

Fixes
=====

- Fixed an incompatibility in the ``PostgreSQL`` wire protocol which could
  cause queries being sent using the ``asyncpg`` python client to get stuck.
  (Using version 0.20 of the client).

- Fixed two display issues in the Admin UI:

  - ``0`` values in the partitions view for tables could be incorrectly
    displayed as ``NULL``.

  - The node health in the cluster view was not displayed when the name of a
    node was too long.

- Fixed a regression introduced in 4.0.8 which could cause queries with an
  explicit limit larger than 1000 to fail with a ``numHits must be > 0`` error.

- Fixed case sensitivity of unquoted column names inside ``ON CONFLICT`` clauses.

- Fixed an resiliency issue on snapshot creation while dynamic columns are
  created concurrently which may result in incompatibility problems on restore.
