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

- The settings ``discovery.zen.publish_timeout``, ``discovery.zen.commit_timeout``,
  ``discovery.zen.no_master_block``, ``discovery.zen.publish_diff.enable``
  have been marked as deprecated and will be removed in a future version.

Changes
=======

Performance improvements
------------------------

- Improved the performance of queries on the ``sys.health`` table.

- Added support for using the optimized primary key lookup plan if additional
  filters are combined via ``AND`` operators.

- Improved the performance of queries on the ``sys.allocations`` table in cases
  where there are filters restricting the result set or if only a sub-set of
  the columns is selected.

- Improved the performance for queries which select a subset of the columns
  available in a wide table.

SQL and PostgreSQL compatibility improvements
---------------------------------------------

- Added arithmetic operation support for the :ref:`numeric <numeric_type>`.

- Added support for the :ref:`numeric <numeric_type>` data type and allow the
  ``sum`` aggregation on the :ref:`numeric <numeric_type>` type.
  Note that the storage of the ``numeric`` data type is not supported.

- Extended the ``RowDescription`` message that can be sent while communicating
  with PostgreSQL clients to include a ``table_oid`` and a ``attr_num`` based
  on the values that are also exposed via the ``pg_catalog.pg_attribute``
  table. This improves compatibility with clients which make use of these
  attributes.

- Changed the :ref:`format_type <format_type>` function to use the PostgreSQL
  compatible type name notation with ``[]`` suffixes for arrays, instead of
  ``_array``.

- Added the ``delimiter`` option for :ref:`copy_from` CSV files. The option is
  used to specify the character that separates columns within a row.

- Added the ``empty_string_as_null`` option for :ref:`copy_from` CSV files.
  If the option is enabled, all column's values represented by an empty string,
  including a quoted empty string, are set to ``NULL``.


Administration and Operations
-----------------------------

- Added information about the shards located on the node to the :ref:`NodeInfo
  MXBean <node_info_mxbean>` which is available as an enterprise feature.

- Added the :ref:`sys.snapshot_restore <sys-snapshot-restore>` table to track
  the progress of the :ref:`snapshot restore <snapshot-restore>` operations.


New scalar and window functions
-------------------------------

- Added the :ref:`to_char <scalar-to_char>` scalar function for ``timestamp``
  and ``interval`` argument data types.

- Added the :ref:`split_part <scalar-split_part>` scalar function.

- Added the :ref:`dense_rank <window-function-dense_rank>` window function,
  which is available as an enterprise feature.

- Added the :ref:`rank <window-function-rank>` window function, which is
  available as an enterprise feature.


Fixes
=====

- Fixed an issue inside the table version and node version compatibility check
  which prevented downgrading from one hotfix version to another one.

- Fixed an issue that could lead to a ``String index out of range`` error when
  streaming values of type ``TIMESTAMP WITH TIME ZONE`` using a PostgreSQL
  client.

- Fixed an issue that could lead to errors like ``Can't map PGType with oid=26
  to Crate type`` using a PostgreSQL client.

- Fixed an issue that could result in a ``The assembled list of
  ParameterSymbols is invalid. Missing parameters.`` error if using the
  ``MATCH`` predicate and parameter placeholders within a query.

- Bumped JNA library to version 5.6.0. This will make CrateDB start flawlessly
  and without warnings on recent versions of Windows.
