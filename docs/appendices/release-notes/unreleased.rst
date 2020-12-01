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

- Added the ``to_timestamp`` scalar function.

- Added the ``to_char`` scalar function for timestamp, interval and numeric types.

- Added support for the ``split_part`` scalar function

- Improved the performance of queries on the ``sys.health`` table.

- Added support for the ``dense_rank`` window function, which is available as an
  enterprise feature.

- Added support for the ``rank`` window function, which is available as an
  enterprise feature.

- Added the ``delimiter`` option for :ref:`copy_from` CSV files. The option is
  used to specify the character that separates columns within a row.

- Added the ``empty_string_as_null`` option for :ref:`copy_from` CSV files.
  If the option is enabled, all column's values represented by an empty string,
  including a quoted empty string, are set to ``NULL``.

- Added the :ref:`sys.snapshot_restore <sys-snapshot-restore>` table to track the
  progress of the :ref:`snapshot restore <snapshot-restore>` operations.

- Added support for using the optimized primary key lookup plan if additional
  filters are combined via ``AND`` operators.

- Improved the performance of queries on the ``sys.allocations`` table in cases
  where there are filters restricting the result set or if only a sub-set of
  the columns is selected.

Fixes
=====

- Fixed a memory leak in the DNS discovery seed provider. The memory leak
  occurred if you configured ``discovery.seed_providers=srv``.

- Fixed a regression introduced in CrateDB ``4.0`` preventing the global setting
  ``cluster.info.update.interval`` to be changed.
