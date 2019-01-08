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


Changes
=======


Fixes
=====

- Casts to nested arrays are now properly supported.

- The type of parameter placeholders in sub-queries in the FROM clause of a
  query can now be resolved to support postgresql clients relying on the
  ``ParameterDescription`` message.
  This enables queries like ``select * from (select $1::int + $2) t``

- Fixed error readability of certain ``ALTER TABLE`` operations.

- Fixed sql parser to not allow repeated ``PARTITION BY`` or ``CLUSTERED BY |
  INTO`` tokens on ``CREATE TABLE`` statements.
