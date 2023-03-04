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

- Fixed a performance regression for ``IS NOT NULL`` expressions on object
  columns which was introduced in 5.0.3 and 5.1.1.

- Fixed an issue that could cause ``DELETE FROM`` statements which match a large
  amount of records to cause a node to crash with an out of memory error.

- Fixed an issue that caused expressions like ``<column> !=
  ANY(<array-literal>)`` to match on partitions where the column didn't exist or
  on records where ``<column>`` had a ``null`` value.

- Fixed an issue that allowed users to execute
  :ref:`user-defined functions <user-defined-functions>` without ``DQL``
  privileges on the schemas that the functions are defined in.

- Fixed an issue that translated ``ColumnUnknownException`` to a misleading
  ``SchemaUnknownException`` when users without ``DQL`` privilege on ``doc``
  schema queried unknown columns from :ref:`table functions <table-functions>`.
  An example ::

    SELECT unknown_col FROM abs(1);
    SchemaUnknownException[Schema 'doc' unknown]

- Fixed an issue that translated an ``AmbiguousColumnException`` to a
  misleading ``IllegalStateException`` when aliased columns are queried that
  are also ambiguous.
  An example ::

    SELECT r FROM (SELECT a AS r, a AS r FROM t) AS q
    IllegalStateException[Symbol 'io.crate.expression.symbol.Symbol' not supported]
    // r is an alias of a and is ambiguous from the perspective of the outer query

