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

None


Fixes
=====

- Fixed a second issue that could cause clients to receive a ``400 Bad
  Request`` error when using the HTTP interface early during node startup. The
  previous fix within the ``4.6.3`` release was incomplete.

- Reduced the default :ref:`initial concurrency limit
  <overload_protection.dml.initial_concurrency>` for operations like ``INSERT
  INTO FROM QUERY`` from 50 to 5. This is closer to the behavior before 4.6.0.
  If the nodes have spare capacity for a higher concurrency the effective
  concurrency limit will grow dynamically over time, but it will start out
  lower to avoid overloading a cluster with an initial spike of internal
  requests.

- Added various :ref:`overload protection <overload_protection>` settings to
  control the concurrency of operations like ``INSERT INTO FROM QUERY``.

- Fixed an issue that caused ``ALTER TABLE <tbl> ADD COLUMN <columName> INDEX
  USING FULLTEXT`` statements to ignore the ``INDEX USING FULLTEXT`` part.

- Fixed a performance regression introduced in 4.2 which could cause queries
  including joins, virtual tables and ``LIMIT`` operators to run slower than
  before.

- Fixed an issue that caused ``INSERT INTO`` statements to fail on partitioned
  tables where the partitioned column is generated and the column and value are
  provided in the statement.

- Fixed an issue that caused showing an incorrect log message in case of an
  authentication failure. "Password authentication" used to be shown instead
  of the actual authentication method name.

- Fixed an issue that caused ``NullPointerException`` when inserting into
  previously altered tables that were partitioned and had generated columns.
