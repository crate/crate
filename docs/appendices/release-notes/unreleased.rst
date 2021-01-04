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

- Fixed a regression introduced in CrateDB ``4.2.0`` leading to a NPE when
  copying data from one table to another using ``INSERT INTO ...`` while the
  source table contains more than 128 columns.

- Fixed an issue resulting in the full generated expression as the column name
  inside data exported by ``COPY TO`` statements.

- Fixed an issue resulting double-quoted column names inside data exported by
  ``COPY TO`` statements.

- Fixed a memory leak in the DNS discovery seed provider. The memory leak
  occurred if you configured ``discovery.seed_providers=srv``.

- Fixed a regression introduced in CrateDB ``4.0`` preventing the global setting
  ``cluster.info.update.interval`` to be changed.

- Fixed handling of spaces in `$CRATE_HOME`. Users would get a `No such file or
  directory` error if the path set via `$CRATE_HOME` contained spaces.
