==================
Unreleased Changes
==================

This file collects *unreleased* changes only.

For release notes, see:

  https://crate.io/docs/reference/en/latest/release_notes/index.html

For developers: changes should be recorded here (using RST syntax) as you are
developing CrateDB. When a new release is being cut, changes will be moved to
the appropriate section of the docs.

Breaking Changes
================

Changes
=======

Fixes
=====

- Fixed an issue which prevents adding new string typed columns into dynamic
  objects if a cluster was initially created with a version between
  ``1.1.0 and 2.0.0``.

- Fixed an issue that caused runtime changes to the
  ``indices.breaker.query.limit`` and ``indices.breaker.query.overhead``
  settings by using the ``SET GLOBAL [TRANSIENT]`` command, to get ignored.

- Store the correct name (``timestamptz``) for the timestamp type in the
  ``pg_type`` table.

- Fixed an issue that caused an ``UnsupportedFeatureException`` to be thrown
  when deleting or updating by query on an empty partitioned table, instead of
  just returning 0 rows deleted/updated.
