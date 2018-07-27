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

- Fixed an issue which was introduced with ``3.0.4`` and could result in
  ``IllegalStateException`` being thrown during the startup of a CrateDB node,
  which prevents its successful bootstrap and one cannot recover from this
  state. ``3.0.4`` is a testing release and is not available in the stable
  channels.

