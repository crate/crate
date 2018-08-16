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

- Fixed an issue where, when checking the privileges of an aliased relation,
  the default schema of "doc" would always be used, rather than the default
  schema of the current session.

- Fixed correct processing of the ``operator`` option of the ``MATCH``
  predicate.
