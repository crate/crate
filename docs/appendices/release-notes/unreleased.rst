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

None


Deprecations
============

- Deprecated the :ref:`node.max_local_storage_nodes
  <node.max_local_storage_nodes>` setting.


Changes
=======

- Improved the performance of ``INSERT FROM query`` statements where the
  ``query`` contains a ``GROUP BY`` clause.

- Changed the privileges model to allow users with ``DDL`` privileges on a
  table to use the :ref:`OPTIMIZE TABLE <sql-optimize>` statement.

- Added the :ref:`bit(n) <data-type-bit>` data type.

- Added support for encrypting node-to-node communication.

- Added a ``closed`` column to :ref:`sys-shards <sys-shards>` exposing
  the state of the table associated with the shard.

- Added :ref:`array_to_string <scalar-array-to-string>` scalar function
  that concatenates array elements into a single string using a separator and
  an optional null-string.

- Added :ref:`array_min <scalar-array-min>` and :ref:`array_max
  <scalar-array-max>` scalar functions returning the minimal and maximal
  element in array respectively.

- Added the :ref:`array_sum <scalar-array-sum>` scalar function
  that returns the sum of all elements in an array.

- Added the :ref:`array_avg <scalar-array-avg>` scalar function that returns
  the sum of all elements in an array.

- Added support for reading ``cgroup`` information in the ``cgroup v2`` format.

- Improved the internal throttling mechanism used for ``INSERT FROM QUERY`` and
  ``COPY FROM`` operations. This should lead to these queries utilizing more
  resources if the cluster can spare them.

- Included the shard information for closed tables in ``sys.shards`` table.

- Users can now read tables within the ``pg_catalog`` schema without explicit
  ``DQL`` permission. They will only see records the user has privileges on.

- CrateDB now accepts the ``START TRANSACTION`` statement for :ref:`PostgreSQL
  wire protocol <postgres_wire_protocol>` compatibility. However, CrateDB does
  not support transactions and will silently ignore this statement.

- Added support of hostnames in HBA configuration.

- Added support for directory-level wild card expansion for URIs passed to
  ``COPY FROM`` statements.

- Improved the performance of the :ref`hyperloglog_distinct
  <aggregation-hyperloglog-distinct>` aggregation function.

- Users with AL privileges (or DDL on both tables) can now run the following
  ALTER CLUSTER commands:
  ``ALTER CLUSTER SWAP TABLE source TO target``,
  ``ALTER CLUSTER REROUTE RETRY FAILED``,
  ``ALTER CLUSTER GC DANGLING ARTIFACTS``.

- Added an optimization that improves the performance of `count()` aggregations
  on object columns that have at least one inner column with a `NOT NULL`
  constraint.

Fixes
=====

- Fixed an issue that caused ``INSERT FROM VALUE`` statements to insert
  records, despite failing validation and returning an error to the client.

- Fixed an issue that caused the ``NOW()`` and ``CURRENT_USER`` functions to
  get normalized to a literal value when used as part of a generated column or
  ``DEFAULT`` expression in a ``CREATE TABLE`` statement.

- Fixed a HBA SSL configuration parsing issue. The ``on`` value for the ``ssl``
  configuration option was not recognized and got interpreted as 'true'.

