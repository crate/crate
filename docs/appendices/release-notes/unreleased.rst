
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

- Fixed a regression introduced in 4.7.0 which caused aggregations used in
  ``INSERT INTO`` statements returning null instead of the aggregation result.

- Fixed a regression introduced in 5.3.0 which caused ``INSERT INTO`` statements
  with a ``ON CONFLICT`` clause on tables with generated primary key columns to
  fail with an ``ArrayIndexOutOfBoundsException``.

- Fixed an issue that caused an ``NullPointerException`` while inserting
  a ``TIMETZ`` typed value dynamically which is not supported.

- Fixed a regression introduced in 5.3.0 which caused ``INSERT INTO`` statements
  to reject invalid dynamic columns and their value without raising an error or
  skipping the whole record. An example ::

    CREATE TABLE t(a INT) WITH (column_policy='dynamic');
    INSERT INTO t(a, _b) VALUES (1, 2);
    INSERT OK, 1 row affected  (0.258 sec)
    INSERT INTO t(a, _b) (SELECT 2, 2);
    INSERT OK, 1 row affected  (0.077 sec)
    SELECT * FROM t;
    +---+
    | a |
    +---+
    | 1 |
    | 2 |
    +---+
    SELECT 2 rows in set (0.594 sec)

  In 5.2.0 neither variant inserted a record. The first ``INSERT`` raised an
  error, and the second resulted in row count 0.

- Fixed an issue which caused ``INSERT INTO`` statements
  to skip generated column validation for sub-columns if provided value is
  ``NULL``.

- Fixed an issue introduced with CrateDB ``5.3.0`` resulting in failing writes
  and/or broken replica shards when ingesting into a table using a number type
  column with explicit turned off indexing by declaring ``INDEX OFF``.


