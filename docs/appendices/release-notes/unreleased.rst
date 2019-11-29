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

- Fixed a possible OutOfMemory issue which may happen on ``GROUP BY`` statement
  using a group key of type ``TEXT`` on tables containing at least one shard
  with a low to medium cardinality on the group key.

- Fixed an issue that caused an error when using ``ALTER TABLE .. ADD`` on a
  table which contains nested primary key columns.

- Fixed an issue where values of type ``array(varchar)`` were decoded
  incorrectly if they contained a ``,`` character. This occurred when
  the PostgreSQL wire protocol was used in ``text`` mode.

- Improved performance of snapshot finalization as https://github.com/crate/crate/pull/9327
  introduced a performance regression on the snapshot process.

- Fixed a ``ClassCastException`` that could occur when using ``unnest`` on
  multi dimensional arrays.
