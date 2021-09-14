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

- Added an empty ``pg_catalog.pg_indexes`` table for compatibility with
  PostgreSQL.

- Changed the type precedence rules for ``INSERT FROM VALUES`` statements. The
  target column types now take higher precedence to avoid errors in statements
  like ``INSERT INTO tbl (text_column) VALUES ('a'), (3)``. Here ``3``
  (``INTEGER``) used to take precedence, leading to a cast error because ``a``
  cannot be converted to an ``INTEGER``.

  This doesn't change the behavior of standalone ``VALUES`` statements.
  ``VALUES ('a'), (3)`` as a standalone statement will still fail.

- Added a new ``table_partitions`` column to the :ref:`sys.snapshots
  <sys-snapshots>` table.

- Added ``float4`` type as alias to ``real`` and ``float8`` type as alias to
  ``double precision``

- Added the :ref:`JSON type <data-type-json>`.

- Added the :ref:`date_bin <date-bin>` scalar function that truncates timestamp
  into specified interval aligned with specified origin.

- Introduced ``RESPECT NULLS`` and ``IGNORE NULLS`` flags to window function
  calls. The following window functions can now utilize the flags: ``LEAD``,
  ``LAG``, ``NTH_VALUE``, ``FIRST_VALUE``, and ``LAST_VALUE``.

- Added the :ref:`scalar-area` scalar function that calculates the area for a
  ``GEO_SHAPE``.

- Added support of ``numeric`` type to the ``avg`` aggregation function.


Fixes
=====

- Fixed an issue that caused ``INSERT INTO`` statements to fail on partitioned
  tables where the partitioned column is generated and the column and value are
  provided in the statement.
