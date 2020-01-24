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

- Fixed an issue that resulted in the values for nested partitioned columns to
  be missing from the result.

- Fixed an issue that caused ``SELECT *`` to include nested columns of type
  ``geo_shape`` instead of only selecting top-level columns.

- Fixed an issue that caused subscript expressions on top of child relations in
  which an object column is selected to fail.

- Fixed a `ClassCastException` that occurred when querying certain columns from
  ``information_schema.tables``, ``sys.jobs_log`` or ``sys.jobs_metrics`` with
  a client connected via PostgreSQL wire protocol.

- Fixed a regression introduced in ``4.0.11`` which caused a
  ``ClassCastException`` when querying ``sys.allocations``.
