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

.. rubric:: Table of Contents

.. contents::
   :local:

Breaking Changes
================

- Renamed CrateDB data types to the corresponding PostgreSQL data types.

   +--------------+----------------------+
   | Current Name | New Name             |
   +==============+======================+
   | ``short``    | ``smallint``         |
   +--------------+----------------------+
   | ``long``     | ``bigint``           |
   +--------------+----------------------+
   | ``float``    | ``real``             |
   +--------------+----------------------+
   | ``double``   | ``double precision`` |
   +--------------+----------------------+
   | ``byte``     | ``char``             |
   +--------------+----------------------+
   | ``string``   | ``text``             |
   +--------------+----------------------+

  See :ref:`data-types` for more detailed information. The old data type names
  are registered as aliases for backward comparability.

- Removed the deprecated ``license.enterprise`` setting. To use CrateDB without
  any enterprise features one should use the Community Edition instead.

- Removed the HTTP pipelining functionality. We are not aware of any client
  using this functionality.

- Changed the ordering of columns to be based on their position in the
  :ref:`CREATE TABLE <ref-create-table>` statement. This was done to improve
  compatibility with PostgreSQL and will affect queries like ``SELECT * FROM``
  or ``INSERT INTO <table> VALUES (...)``

- Changed the default :ref:`column_policy` on tables from ``dynamic`` to
  ``strict``. Columns of type object still default to ``dynamic``.

- Removed the deprecated ``license.ident`` setting.

- Removed the deprecated ``USR2`` signal handling. Use :ref:`ALTER CLUSTER
  DECOMISSION <alter_cluster_decommission>` instead. Be aware that the
  behavior of sending ``USR2`` signals to a CrateDB process is now undefined
  and up to the JVM. In some cases it may still terminate the instance but
  without clean shutdown.

- Renamed ``information_schema.columns.user_defined_type_*`` columns to
  ``information_schema_columns.udt_*`` for SQL standard compatibility.

- Changed type of column ``information_schema.columns.is_generated`` to ``STRING``
  with value ``NEVER`` or ``ALWAYS`` for SQL standard compatibility.

- Removed the deprecated average duration and query frequency JMX metrics. The
  total counts and sum of durations as documented in :ref:`query_stats_mbean`
  should be used instead.

- Removed the deprecated setting ``cluster.graceful_stop.reallocate``.

- Removed the deprecated ``ON DUPLICATE KEY`` syntax of :ref:`ref-insert`
  statements.

- Dropped support for Java versions < 11

- The Elasticsearch REST API has been removed.

- Changed the layout of the ``version`` column in the
  ``information_schema.tables`` and ``information_schema.table_partitions``
  tables. The version is now displayed directly under ``created`` and
  ``upgraded``. The ``cratedb`` and ``elasticsearch`` sub-category has been
  removed.

- Removed the ``index`` thread-pool and the ``bulk`` alias for the ``write``
  thread-pool. The JMX ``getBulk`` property of the ``ThreadPools`` bean has
  been renamed too ``getWrite``.

- Removed the deprecated ``http.enabled`` setting. ``HTTP`` is now always
  enabled and can no longer be disabled.

- Removed the deprecated ``ingest`` framework, including the ``MQTT`` endpoint.


Deprecations
============

- Marked SynonymFilter tokenizer as deprecated.

- Marked LowerCase tokenizer as deprecated.

- The query frequency and average duration :ref:`query_stats_mbean` metrics
  have been deprecated in favour of the new total count and sum of durations
  metrics.

- Marked the ``cluster.graceful_stop.reallocate`` setting as deprecated.
  This setting was already being ignored, setting the value to `false` has
  no effect.

- Marked ``CREATE INGEST RULE`` and ``DROP INGEST RULE`` as deprecated.
  Given that the only implementation (MQTT) was deprecated and will be removed,
  the framework itself will also be removed.

Changes
=======

- Added support for the ``PARTITION BY`` clause in :ref:`window-functions`.

- Upgraded to Lucene 8.0.0, and as part of this the BM25 scoring has changed.
  The order of the scores remain the same, but the values of the scores differ.
  Fulltext queries including ``_score`` filters may behave slightly different.

- Added :ref:`quote_ident <scalar-quote-ident>` scalar string function that
  quotes a string if it is needed.

- Added missing Postgresql type mapping for the ``array(ip)`` collection type.

- Added a new ``_docid`` :ref:`system column
  <sql_administration_system_columns>`.

- Added :ref:`trim <scalar-trim>` scalar string function that trims
  the (leading, trailing or both) set of characters from an input string.

- Added :ref:`string_to_array <scalar-string-to-array>` scalar array function
  that splits an input string into an array of string elements using a
  separator and a null-string.

- Added support for subscript expressions on an object column of a sub-relation.
  Examples: ``select a['b'] from (select a from t1)`` or ``select a['b'] from
  my_view`` where ``my_view`` is defined as ``select a from t1``.

- Changed the trial license introduced in 3.2 to no longer have an expiration
  date, but instead be limited to 3 nodes. See :ref:`enterprise_features`.

- The :ref:`usage_data_collector` now includes information about the available
  number of processors.

- Added support for :ref:`sql_escape_string_literals`.

- Expose the sum of durations, total, and failed count metrics under the
  :ref:`query_stats_mbean` for ``QUERY``, ``INSERT``, ``UPDATE``, ``DELETE``,
  ``MANAGEMENT``, ``DDL`` and ``COPY`` statement types.

- Expose the sum of statement durations, total, and failed count classified by
  statement type under the sum_of_durations, total_count and failed_count
  columns, respectively, in the :ref:`sys-jobs-metrics` table.

- Added a node check that checks the JVM version under which CrateDB is
  running. We recommend users to upgrade to JVM 11 as support for older
  versions will be dropped in the future.

- Added ``ALTER CLUSTER DECOMMISSION <nodeId | nodeName>`` statement that
  triggers the existing node decommission functionality.

- Added ``pg_type`` columns: ``typlen``, ``typarray``, ``typnotnull``
  and ``typnamespace`` for improved postgresql compatibility.

- Added ``current_schemas(boolean)`` scalar function which will return the
  names of schemas in the ``search_path``.

- Added support for the ``first_value``, ``last_value`` and ``nth_value``
  window functions as enterprise features.

- Implemented the ``DROP ANALYZER`` statement to support removal of custom
  analyzer definitions from the cluster.

- Output the custom analyzer/tokenizer/token_filter/char_filter definition inside
  the ``information_schema.routines.routine_definition`` column.

- Added a ``pg_description`` table to the ``pg_catalog`` schema for improved
  postgresql compatibility.

- Added support for window function ``row_number()``.

- Added support to use any expression in the operand of a ``CASE`` clause.

- Buffer the file output of ``COPY TO`` operations to improve performance by not
  writing to disk on every row.

Fixes
=====

- Fix quoting of identifiers that contain leading digits or spaces when
  printing relation or column names.

- Fixed function resolution for postgresql functions ``pg_backend_pid``,
  ``pg_get_expr`` and ``current_database`` when the schema prefix
  ``pg_catalog`` is included.
