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

- Made the documented :ref:`indices.breaker.total.limit
  <indices.breaker.total.limit>` setting public, so that it can be adjusted
  using :ref:`SET GLOBAL <ref-set>`.

- Improved the migration logic for partitioned tables which have been created
  in CrateDB 2.x. If all current partitions of a partitioned tables have been
  created in CrateDB 3.x, the table won't have to be re-indexed anymore to
  upgrade to CrateDB 4.0+. 

- Changed the error message returned when a :ref:`CREATE REPOSITORY
  <ref-create-repository>` statement fails so that it includes more information
  about the cause of the failure.
