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

- Fixed an issue that led to a ``NullPointerException`` when using ``GROUP BY``
  on a nested ``PARTITIONED BY`` column.

- Fixed an issue that led to an ``ArrayIndexOutOfBoundsException`` if using
  ``ON CONFLICT (...) UPDATE SET`` in an ``INSERT`` statement.

- Fixed an issue that could lead to a ``Values less than -1 bytes are not
  supported`` error, if one or more CrateDB nodes have few disk space
  available.

- Fixed an issue that would cause ``COPY FROM`` statements that used a HTTPS
  source using a Let's Encrypt certificate to fail.

- Fixed an issue that caused :ref:`RTRIM <scalar-rtrim>` to behave like
  :ref:`LRTRIM <scalar-ltrim>`.

