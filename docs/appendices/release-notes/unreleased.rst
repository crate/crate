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

- Fixed an issue with the handling of intervals in generated columns. The table
  creation failed when an interval is included in a function call as part of a
  generated column.

- Fixed an issue with the handling of quoted identifiers in column names where
  certain characters break the processing. This makes sure any special characters
  can be used as column name.

- Fixed a race condition that could cause a ``blocked by: [FORBIDDEN/4/Table or
  partition preparing to close`` error when inserting into a partitioned table
  where a single partition got closed.

- Fixed an issue that caused an ``Relation unknown`` error while trying to
  close an empty partitioned table using ``ALTER TABLE ... CLOSE``.

- Fixed an issue that caused ``COPY FROM RETURN SUMMARY`` fail non-gracefully
  in case of import from CSV containing invalid line(s).

- Updated to Admin UI 1.20.2, which fixes duplicate entries in query history.

- Fixed an issue that threw ``SQLParseException`` when a ``ILIKE`` operand
  contained '{' or '}'.
