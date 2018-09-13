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

- Trying to create a table with a generated column inside an object column now
  results in a friendly error message instead of a ``NullPointerException``.

- Fixed processing of the ``endpoint``, ``protocol`` and ``max_retries`` S3
  repository parameters.
