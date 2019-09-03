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


Changes
=======

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

- Fixed an issue resulting in ``NULL`` values when the ``ORDER BY`` symbol is a
  child of an ignored object column.

- Fixed the ``Tables need to be recreated`` :ref:`cluster check <sys-checks>`
  to list partitioned tables only once instead of once per partition.

- Fixed the :ref:`ssl.resource_poll_interval <ssl.resource_poll_interval>`
  setting processing and documentation.
