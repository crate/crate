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

None


Changes
=======

- Added a ``closed`` column to :ref:`sys-shards <sys-shards>` exposing
  the state of the table associated with the shard.

- Added :ref:`array_to_string <scalar-array-to-string>` scalar function
  that concatenates array elements into a single string using a separator and
  an optional null-string.

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

Fixes
=====

- Updated the bundled JDK to 16.0.1+9

- Fixed an issue that would cause columns of type ``varchar`` with a length
  limited to be incorrectly casted to another type if used as argument in a
  function that has several type overloads.

- Fixed an issue that caused ``ALTER TABLE ADD COLUMN`` statements to remove
  constraints like analyzers or ``NOT NULL`` from existing columns in the same
  table.

- Allow executing ``CREATE TABEL .. AS`` as a regular user with ``DDL``
  permission on the target schema, and ``DQL`` permission on the source
  relations.

- Changed the ``RowDescription`` message that is sent to PostgreSQL clients to
  avoid that the JDBC client triggers queries against ``pg_catalog`` schema
  tables each time information from the ``MetaData`` of a ``ResultSet`` is
  accessed.

- Fixed ``crate-node`` auxiliary program to use the bundled Java runtime on
  Linux.
