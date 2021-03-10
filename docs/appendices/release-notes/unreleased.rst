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

- Fixed an issue that led to a ``CircuitBreakingException`` when using the
  ``ANALYZE`` statement.

- Fixed an issue that led to ``Values less than -1 bytes`` errors if ``TRACE``
  logging was activated for the circuit breaker package.

- Fixed shard allocation on downgraded nodes where only the ``HOTFIX`` version
  part differs to fully support rolling downgrades to same ``MAJOR.MINOR``
  versions.

- Fixed an issue that could lead to a stuck ``INNER JOIN`` query involving the
  ``sys.shards`` table on a cluster without user tables.

- Adjusted ``crate.bat`` to work with spaces in directory names.

- Fixed a ``NullPointerException`` when trying to kill a job as normal user
  which is no longer running.
