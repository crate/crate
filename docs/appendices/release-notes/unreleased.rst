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

None


Changes
=======

None


Fixes
=====

- Fixed a BWC issue with aggregation function resolving in a mixed version
  cluster where at least one node is on version < 4.2.

- Improved the throttling behavior of ``INSERT INTO .. <query>``, it is now
  more aggressive to reduce the amount of memory used by a ``INSERT INTO``
  operation.

- Fixed an issue which resulted in an error when a parameter symbol
  (placeholder) is used inside an aggregation.

- Fixed an issue that could lead to the incorrect result of joining more than
  two tables even if the join condition is satisfied. Only the hash join
  implementation was affected by the issue.

- Fixed a regression introduced in 4.2.3 that prevented primary key lookups
  with parameter placeholders from working in some cases.

- Fixed an issue resulting wrongly in a `RED` ``sys.health.health`` state for
  healthy partitions with less shards configured than the actual partitioned
  table.

- Fixed the resulting value for ``sys.health.partition_ident`` for
  non-partitioned tables. As documented, a `NULL` value should be returned
  instead of an empty string.

- Fixed a performance regression that caused unnecessary traffic and load to
  the active master node when processing ``INSERT`` statements.
