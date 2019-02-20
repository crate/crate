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

.. rubric:: Table of Contents

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

- Fixed an issue that caused a ``stream has already been operated upon or
  closed`` exception to be thrown when joining on a right subquery that
  contained a ``group by`` clause on one number column.

- Fixed an issue that caused ``INSERT INTO`` with a subquery to not insert into
  partitioned tables where the partitioned by columns had a ``NOT NULL``
  constraint.

- Fixed a regression that caused inserts which create new dynamic columns to
  fail if the table was created in an earlier version of CrateDB.

- Fixed an issue that caused inserts into partitioned tables where the
  partitioned column is generated and based on the child of an object to fail.

- Fixed an issue that caused the Basic Authentication prompt to fail in Safari.
