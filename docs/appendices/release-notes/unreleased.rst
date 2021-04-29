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

- Creating tables with soft deletes disabled is no longer supported.
  The setting :ref:`sql-create-table-soft-deletes-enabled` will
  always be set to ``true`` and removed in CrateDB 6.0.

- Removed deprecated ``node.max_local_storage_nodes`` setting. Set different
  :ref:`path.data <path.data>` values to run multiple CrateDB processes on the
  same machine.

- Removed deprecated ``delimited_payload_filter`` built-in token filter which
  has been renamed to ``delimited_payload`` since CrateDB 3.2.0.

- Removed ``simplefs`` store type as a follow-up of its removal in Lucene
  9.0.0. Refer to :ref:`store types <sql-create-table-store-type>` for
  alternatives.

- Renamed column names returned from ``Table Functions``. If the table function
  is aliased and is returning a base data type (scalar type), the table alias
  is used as the column name.

Deprecations
============

None


Changes
=======

- Added support for using ``NULL`` literals in a ``UNION`` without requiring an
  explicit cast.

- Added an optimization to push down constant join conditions to the relation
  in an inner join, which results in a more efficient execution plan.

- Updated the bundled JDK to 18.0.1+10

- Moved the :ref:`scalar-quote_ident` function to `pg_catalog` for improved
  compatibility with PostgreSQL.

- Added the :ref:`concat_ws <scalar-concat-ws>` scalar function which allows
  concatenation with a custom separator.

- Added ``decimal`` type as alias to ``numeric``

- Users with AL privileges can now run ``ANALYZE``

- Added ``typsend`` column to ``pg_catalog.pgtype`` table for improved
  compatibility with PostgreSQL.

- Added the :ref:`object_keys <scalar-object_keys>` scalar function which returns
  the set of first level keys of an ``object``.

- Added support for non-recursive :ref:`sql_dql_with`.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Enabled to alter the setting ``blocks.read_only_allow_delete`` on blob tables
  to make it possible to drop read-only blob tables.

- Fixed an issue that could cause queries on ``sys.snapshots`` to get stuck and
  consume a significant amount of resources.

- Fixed an issue with primary key columns that have a ``DEFAULT`` clause. That
  could lead to queries on the primary key column not matching the row.

- Fixed an issue with the logical replication of tables metadata which caused
  to stop if the master node of the subscriber changed.

- Fixed an issue with aliased sub-relation outputs when used inside the outer
  where clause expression, resulting in a planner error. Example:
  ``SELECT * FROM (SELECT id, true AS a FROM t1) WHERE a``

- Fixed an edge case with the initial restore of subscribed tables when the
  restore operation finish almost instantly (e.g. restoring small tables).

- Fixed an issue with table functions parameter binding in ``SELECT`` queries
  without ``FROM`` clause. Example: ``SELECT unnest(?)``.

- Improved error handling when creating a subscription with unknown
  publications. Instead of successfully creating the subscription, an error
  is now presented to the user.

- Fixed an issue with client caching which lead to authentication error when
  creating a subscription with bad credentials and ``pg_tunnel`` followed by
  re-creating it second time with the same name and valid credentials.

- Fixed an issue with ``VARCHAR`` and ``BIT`` columns with a length
  limited used in primary key, generated or default column expression. An
  ``ALTER TABLE`` statement removed the length limit from such columns.

- Fixed an issue resulting in a broken subscription when a subscription is
  dropped and re-created within a short period of time.

