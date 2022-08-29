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

- Fixed a regression introduced in CrateDB ``4.3.0`` causing an
  ``IndexOutOfBoundsException`` when applying aggregations on literals.
  Example::

    SELECT SUM(10) FROM test HAVING COUNT(1) > 0

- Fixed an issue, preventing users from defining a constraint on a generated
  column, when creating a table or when adding a generated column. Example::

    CREATE TABLE test(
        col1 INT,
        col2 INT GENERATED ALWAYS AS col1*2 CHECK (col2 > 0)
   )

- Fixed an issue causing ``IndexOutOfBoundsException`` to be thrown when using
  ``LEFT``/``RIGHT`` or ``FULL`` ``OUTER JOIN`` and one of the tables (or
  sub-selects) joined has 0 rows.

- Updated the bundled JDK from 18.0.1+10 to 18.0.2+9.

- Fixed a race condition that could cause a ``INSERT INTO`` operation to get
  stuck.

- Fixed an issue that could cause queries with ``objectColumn = ?`` expressions
  to fail if the object contains inner arrays.

- Fixed a ``NullPointerException`` when using a ``IS NULL`` expression on an
  object column that just had a child column added.

- Fixed an issue that caused ``array_upper`` and ``array_lower`` scalar
  functions return wrong results on multidimensional arrays.

- Fixed exposure of the correct version a partitioned table was created on at
  the ``information_schema.tables.version.created`` column for newly created
  partitioned tables. This won't fix it for existing tables as the version
  information the table was created with was not stored and such is not
  known. The issue doesn't affect the version exposed for individual partitions.

- Fixed a column positions issue that caused an ``INSERT`` or any other
  statements that adds columns dynamically to throw an exception.

- Updated the Admin UI to version 1.22.2. It includes a fix for a rendering
  issue causing jumping behavior on selected views in view list.

- Fixed ``UPDATE``, ``INSERT`` and ``COPY FROM`` to preserve the implied column
  order when columns are added.

- Fixed casts of strings to the ``DATE`` type, any possible time parts
  of a timestamp formatted will be ignored instead of raising a cast error.

- Fixed casts of numeric and timestamp values to the ``DATE`` type, any time
  values weren't removed from the returning epoch in milliseconds.
