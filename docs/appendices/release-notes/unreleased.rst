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

- Added support for the :ref:`CREATE TABLE AS <ref-create-table-as>` statement.

Fixes
=====

- Fixed an issue in the error handling of ``CREATE REPOSITORY`` statements
  which could lead to a ``NullPointerException`` instead of a more meaningful
  error message.

- Fixed an issue that could lead to a ``Can't handle Symbol`` error when
  using views which are defined with column aliases.

- Fixed a regression that led to ``max`` aggregations on columns of type
  ``double precision`` or ``real`` to return ``null`` instead of ``0.0`` if all
  aggregated values are ``0.0``.

- Fixed an issue that could lead to an ``OutOfMemoryError`` when retrieving
  large result sets with large individual records.

- Fixed an issue that could lead to a ``NullPointerException`` when using a
  SELECT statement containing an INNER JOIN.

- Fixed an issue in the PostgreSQL wire protocol that would cause
  de-serialization of arrays to fail if they contained unquoted strings
  starting with digits.

- Fixed an issue that could lead to a serialization error when streaming values
  of the ``TIMESTAMP WITHOUT TIME ZONE`` type in text format using the
  PostgreSQL wire protocol.
