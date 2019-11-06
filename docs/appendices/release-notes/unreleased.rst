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

.. Always cluster items into bigger topics. Link to the documentation whenever feasible.
.. Remember to give the right level of information: Users should understand
.. the impact of the change without going into the depth of tech.

.. rubric:: Table of contents

.. contents::
   :local:

Changes
=======

None

Fixes
=====

- Improved the handling of sorted queries with a large limit, to reduce the
  chance of them causing a out of memory error.

- Fixed a ``NullPointerException`` that could occur when querying the
  ``settings`` column of ``information_schema.table_partitions``.

- Fixed an issue in the Admin interface that caused the pagination ``Previous``
  button to not display the whole list of results for that page in the console
  view.

- Fixed an issue that could prevent ``CREATE SNAPSHOT`` from succeeding,
  resulting in a partial snapshot which contained failure messages incorrectly
  indicating that the index is corrupt.

- Fixed an issue resulting in a parsing exception on ``SHOW TABLE`` statements
  when a default expression is implicitly cast to the related column type and
  the column type contains a ``SPACE`` character (like e.g. ``double precision``).
