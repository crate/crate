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

- Improved the help section of the admin-ui and added Spanish translations.

Fixes
=====

- Fixed an issue in the admin-ui to no longer display all columns as being
  generated columns in the table/column view section.

- Fixed an issue introduced in CrateDB 4.0 resulting in dysfunctional disk-based
  allocation thresholds.

- Fixed an issue resulting in ``pg_catalog.pg_attribute.attnum`` and
  ``information_schema.columns.ordinal_position`` being ``NULL`` on tables
  created with CrateDB < 4.0.

- Fixed an issue resulting in ``NULL`` values when the ``ORDER BY`` symbol is a
  child of an ignored object column.

- Fixed the ``Tables need to be recreated`` :ref:`cluster check <sys-checks>`
  to list partitioned tables only once instead of once per partition.

- Fixed the :ref:`ssl.resource_poll_interval <ssl.resource_poll_interval>`
  setting processing and documentation.
