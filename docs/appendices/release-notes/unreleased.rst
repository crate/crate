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

- Added support for flag ``g`` to function
  :ref:`regexp_matches <table-functions-regexp-matches>` and changed
  its type from ``scalar`` to ``table`` type. It now returns a table where each
  row contains a single column ``groups`` of type ``array(text)``.


Changes
=======

- Changed the error code for the psql protocol when a column reference
  is ambiguous from `XX000` `internal_error` to `42702` `ambiguous_column`.

- Changed the error code for the psql protocol when a relation exists
  already from `XX000` `internal_error` to `42P07` `duplicate_table`.

- Changed the error code for the psql protocol when a column does not exist
  from `XX000` `internal_error` to `42703` `undefined_column`.

- Changed the error code for the psql protocol when a relation does not exist
  from `XX000` `internal_error` to `42P01` `undefined_table`.

- Changed the error code for the psql protocol when a document exists
  already from `XX000` `internal_error` to `23505` `unique_violation`.

- Added scalar function :ref:`pg_function_is_visible <pg_function_is_visible>`.

- Added the ``read_only_allow_delete`` setting to the ``settings['blocks']``
  column of the :ref:`information_schema.tables <information_schema_tables>`
  and :ref:`information_schema.table_partitions <is_table_partitions>` tables.

- Changed the error code for dropping a missing view from the undefined 4040
  to 4041.

- Changed the error handling so it returns the error message and the related
  exception without being wrapped in a ``SqlActionException``. Error codes
  remain the same.

- Changed :ref:`OPTIMIZE <sql_ref_optimize>` to no longer implicitly refresh a
  table.

- Added table function :ref:`generate_subscripts <table-functions-generate-subscripts>`

- Changed the privileges for ``KILL``, all users are now allowed to kill their
  own statements.

- Added the `pg_catalog.pg_roles table <postgres_pg_catalog>`


Fixes
=====

- Fixed an issue resulting wrongly in a `RED` ``sys.health.health`` state for
  healthy partitions with less shards configured than the actual partitioned
  table.

- Fixed the resulting value for ``sys.health.partition_ident`` for
  non-partitioned tables. As documented, a `NULL` value should be returned
  instead of an empty string.

- Fixed a performance regression that caused unnecessary traffic and load to
  the active master node when processing ``INSERT`` statements.
