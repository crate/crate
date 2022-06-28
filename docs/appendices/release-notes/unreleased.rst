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

- Changed the type of :ref:`TIMESTAMP <type-timestamp>` from :ref:`TIMESTAMP WITH
  TIME ZONE <type-timestamp-with-tz>` to :ref:`TIMESTAMP WITHOUT TIME ZONE
  <type-timestamp-without-tz>` to be compatible with the SQL standard.

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

- Renamed column names returned from ``Table Functions``. 1) If the table
  function is aliased and is returning a base data type (scalar type), the
  table alias is used as the column name. 2) If the table function is not
  aliased and is returning a base data type, the table function name is used
  as the column name.

- Fields of type ``pg_node_tree`` in PostgreSQL in the ``pg_class`` and
  ``pg_index`` tables now return ``TEXT`` instead of ``ARRAY(OBJECT)`` for
  improved compatibility with PostgreSQL.

Deprecations
============

None


Changes
=======

- Added support for ``FETCH [FIRST | NEXT] <noRows> [ROW | ROWS] ONLY`` clause
  as and alternative to the ``LIMIT`` clause.

- Allowed ``LIMIT`` and ``OFFSET`` clauses to be declared in any order, i.e.:
  ``SELECT * FROM t LIMIT 10 OFFSET 5`` or
  ``SELECT * FROM t OFFSET 5 LIMIT 10``.

- Added support for ``LIMIT NULL``, ``LIMIT ALL``, ``OFFSET NULL``,
  ``OFFSET 10 ROW`` and ``OFFSET 10 ROWS``.

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

- Added :ref:`has_schema_privilege <scalar-has-schema-priv>` scalar function
  which checks whether user (or current user if not specified) has specific
  privilege(s) for the specific schema.

- Updated Admin UI to 1.22.0, including an update with the new logo and colors.

- Added ``SUBSTRING`` to non-reserved SQL keywords in order to support the
  generic function call syntax for improved PostgreSQL compatibility.
  Example: ``SUBSTRING('crate', 1, 3)``
  
- Added ``pg_catalog.pg_tables`` and ``pg_catalog.pg_views`` tables for improved 
  PostgreSQL compatibility.

- Added identity columns information to ``information_schema.columns`` table for
  improved PostgreSQL compatibility. CrateDB does not support identity columns.
  
- Added the :ref:`pg_get_serial_sequence <scalar-pg_get_serial_sequence>` scalar 
  function for improved compatibility with PostgreSQL. CrateDB does not support 
  sequences.

- Added primary key and check constraint column positions into ``conkey`` field
  of the ``pg_constraint`` table for improved compatibility with PostgreSQL.

- Updated Admin UI to 1.22.1, including an optimization to the web fonts.
  Admin UI now stops making requests to external resources completely.

- Defined a node setting, :ref:`legacy.table_function_column_naming
  <legacy.table_function_column_naming>`. This setting can be set to revert the
  breaking change that caused the output column names of ``unnest``,
  ``regexp_matches``, and ``generate_series`` to be the respective table
  function names.

- Added support for an optional boolean argument ``pretty`` at the 
  :ref:`pg_get_expr <scalar-pg_get_expr>` scalar function for improved
  PostgreSQL compatibility.

- Added the :ref:`pg_get_partkeydef <scalar-pg_get_partkeydef>` scalar 
  function for improved compatibility with PostgreSQL. Partitioning in CrateDB
  is different from PostgreSQL, therefore this function always returns ``NULL``.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that caused queries with ``ORDER BY`` clause and ``LIMIT 0`` to
  fail.

- Fixed an issue that prevented rows inserted after the last refresh from
  showing up in the result if a shard had been idle for more than 30 seconds.
  This affected tables without an explicit ``refresh_interval`` setting.

- Fixed an issue that caused NPE to be thrown, instead of a user-friendly error
  message when ``NULL`` is passed as shardId for the
  ``ALTER TABLE XXX REROUTE XXX`` statements (MOVE, ALLOCATE, PROMOTE, CANCEL).

- Fixed an issue that caused queries operating on expressions with no defined
  type to fail. Examples are queries with ``GROUP BY`` on an ignored object
  column or ``UNION`` on ``NULL`` literals.

- Fixed an issue that caused ``GROUP BY`` and ``ORDER BY`` statements with
  ``NULL`` ordinal casted to a specific type, throw an error. Example:
  ``SELECT NULL, count(*) from unnest([1, 2]) GROUP BY NULL::integer``.

- Fixed an issue that not null constraints used to be shown in the
  ``pg_constraint`` table which contradicts with the PostgreSQL.

- Fixed an issue that caused ``IllegalArgumentException`` to be thrown when
  attempting to insert values into a partitioned table, using less columns than
  the ones defined in the table's ``PARTITIONED BY`` clause.
