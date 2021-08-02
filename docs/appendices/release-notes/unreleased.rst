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

- Fixed an issue in the execution plan generation for ``SELECT COUNT(*) FROM
  ...`` statements with predicates like ``'a' in ANY(varchar_array_column)``.
  Such predicates resulted in a cast on the column (``'a' in
  ANY(varchar_array_column::array(varchar(1)))``), leading to poor performance
  because the indices couldn't get utilized. This fix significantly improves
  the performance of such queries. In a test over 100000 records, the query
  runtime improved from 320ms to 2ms.

- Fixed an issue that could cause a ``NullPointerException`` if a user invoked
  a ``SELECT`` statement with a predicate on a ``OBJECT (ignored)`` column
  immediately after a ``DELETE`` statement.
