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

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that caused queries with ``ORDER BY`` clause and ``LIMIT 0`` to
  fail.

- Fixed an issue that prevented rows inserted after the last refresh from
  showing up in the result if a shard had been idle for more than 30 seconds.
  This affected tables without an explicit ``refresh_interval`` setting.

- Fixed an issue that caused NPE to be thrown, instead of a user-friendly error
  message when ``NULL`` is passed as shardId for the
  ``ALTER TABLE XXX REROUTE XXX`` statements (MOVE, ALLOCATE, PROMOTE, CANCEL).

- Fixed an issue that caused queries operating on expressions with no defined
  type to fail. Examples are queries with ``GROUP BY`` on an ignored object
  column or ``UNION`` on ``NULL`` literals.

- Fixed an issue that caused ``GROUP BY`` and ``ORDER BY`` statements with
  ``NULL`` ordinal casted to a specific type, throw an error. Example:
  ``SELECT NULL, count(*) from unnest([1, 2]) GROUP BY NULL::integer``.

- Fixed an issue that not-null constraints used to be shown in the
  ``pg_constraint`` table which contradicts with PostgreSQL.

- Fixed an issue that caused ``IllegalArgumentException`` to be thrown when
  attempting to insert values into a partitioned table, using less columns than
  the ones defined in the table's ``PARTITIONED BY`` clause.

- Fixed an issue that caused failure of ``ALTER TABLE`` statements when updating
  dynamic or non-dynamic table settings on closed tables.
