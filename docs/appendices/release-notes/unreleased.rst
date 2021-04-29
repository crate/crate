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
