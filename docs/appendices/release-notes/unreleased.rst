
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

- Added columns ``prosupport``, ``prokind``, ``prosqlbody`` and removed columns
  ``protransform``, ``proisagg`` and ``proiswindow`` from ``pg_proc`` table to
  be in sync with PostgreSQL version ``14``.

- Added column ``relrewrite`` and removed columns ``relhasoids`` and
  ``relhaspkey``from ``pg_class`` table to be in sync with PostgreSQL version
  ``14``.

- Added columns ``atthasmissing`` and ``attmissingval`` to ``pg_attribute`` table
  to be in sync with PostgreSQL version ``14``.

- Added column ``conparentid`` and removed column ``consrc`` from
  ``pg_constraint`` table to be in sync with PostgreSQL version ``14``.

- Added column ``indnkeyatts`` to ``pg_index`` table to be in sync with
  PostgreSQL version ``14``.

- Added columns ``typacl``, ``typalign``, ``typanalyze``, ``typdefaultbin``,
  ``typmodin``, ``typmodout``, ``typstorage``, ``typsubscript`` to ``pg_type``
  table to be in sync with PostgreSQL version ``14``.

- Changed ``pg_constraint.conbin`` column type from ``OBJECT`` to ``STRING`` and
  ``pg_proc.proargdefaults`` column type from ``OBJECT[]`` to ``STRING`` to be
  in sync with other similar columns, e.g.: ``pg_index.indexprs``.

- Changed ``pg_attribute.spcacl``, ``pg_class.relacl`` and
  ``pg_namespace.nspacl`` columns type from ``OBJECT[]`` to ``STRING[]`` to be
  in sync with other similar columns, e.g.: ``pg_database.datacl``.

- Raise an exception if duplicate columns are detected on
  :ref:`named index column definition <named-index-column>` instead of
  silently ignoring them.

- Adjusted allowed array index range to be from ``Integer.MIN_VALUE`` to
  ``Integer.MAX_VALUE``. The behavior is now also consistent between subscripts
  on array literals and on columns, and between index literals and index
  expressions. That means something like ``tags[-1]`` will now return ``NULL``
  just like ``ARRAY['AUT', 'GER'][-1]`` or ``ARRAY['AUT', 'GER'][1 - 5]`` did.


Deprecations
============

None


Changes
=======

SQL Statements
--------------

- Extended the :ref:`EXPLAIN <ref-explain>` statement output to include the
  estimated row count in the output of the execution plan. The statement also
  has now options for `ANALYZE` and `COSTS` to have better control on
  the generated output plan.

SQL Standard and PostgreSQL Compatibility
-----------------------------------------

- Bumped the version of PostgreSQL wire protocol to ``14`` since ``10`` has been
  deprecated.

- Added ``any_value`` as an alias to the ``arbitrary`` aggregation function, for
  compliance with the SQL2023 standard. Extended the aggregations to support any
  type.

- Changed literal :ref:`INTERVAL data type <type-interval>` to do normalization
  up to day units, and comply with PostgreSQL behavior, e.g.::

    cr> SELECT INTERVAL '1 month 42 days 126 hours 512 mins 7123 secs';
    +------------------------------+
    | 'P1M47DT16H30M43S'::interval |
    +------------------------------+
    | 1 mon 47 days 16:30:43       |
    +------------------------------+

- Added ``attgenerated`` column to ``pg_catalog.pg_attribute`` table which
  returns ``''`` (empty string) for normal columns and ``'s'`` for
  :ref:`generated columns <ddl-generated-columns>`.

- Added the ``pg_catalog.pg_cursors`` table to expose open cursors.

- Added the
  :ref:`standard_conforming_strings <conf-session-standard_conforming_strings>`
  read-only session setting for improved compatibility with PostgreSQL clients.

- Allow casts in both forms: ``CAST(<literal or parameter> AS <datatype>)`` and
  ``<literal or parameter>::<datatype>`` for ``LIMIT`` and ``OFFSET`` clauses,

  e.g.::

    SELECT * FROM test OFFSET CAST(? AS long) LIMIT '20'::int


- Added support for ``ORDER BY``, ``MAX``, ``MIN`` and comparison operators on
  expressions of type ``INTERVAL``.

- Added support for setting session settings via a ``"options"`` property in the
  startup message for PostgreSQL wire protocol clients.

  An example for JDBC::

    Properties props = new Properties();
    props.setProperty("options", "-c statement_timeout=90000");
    Connection conn = DriverManager.getConnection(url, props);

- Added support for underscores in numeric literals. Example::

    SELECT 1_000_000;

- Added support for updating arrays by elements, e.g.::

    UPDATE t SET a[1] = 2 WHERE id = 1;

- Array comparisons like ``= ANY`` will now automatically unnest the array
  argument to the required dimensions.

  An example::

    cr> SELECT 1 = ANY([ [1, 2], [3, 4] ]);   -- automatic unnesting
    True

    cr> SELECT [1] = ANY([ [1, 2], [3, 4] ]); -- no unnesting
    False

Scalar and Aggregation Functions
--------------------------------

- Added support for :ref:`AVG() aggregation <aggregation-avg>` on
  :ref:`INTERVAL data type <type-interval>`.

- Added a :ref:`array_unnest <scalar-array_unnest>` scalar function.

- Added a :ref:`btrim <scalar-btrim>` scalar function.

- Added :ref:`array_set <scalar-array_set>` scalar function.

Performance and Resilience Improvements
---------------------------------------

- Improved the partition filtering logic to also narrow partitions if the
  partition is based on a generated column using the :ref:`date_bin <date-bin>`
  scalar.

- Improved ``COPY FROM`` retry logic to retry with a delay which increases
  exponentially on temporary network timeout and general network errors.

Data Types
----------

- Added support to disable :ref:`column storage <ddl-storage-columnstore>` for
  :ref:`numeric data types <data-types-numeric>`,
  :ref:`timestamp <type-timestamp>` and
  :ref:`timestamp with timezone`<type-timestamp-with-tz>`.

Administration and Operations
-----------------------------

- Added optimizer rules for reordering of joins for hash and nested-loop joins.
  This allows now to control the join-reordering and disable it, if desired, with
  session settings::

    SET optimizer_reorder_hash_join = false
    SET optimizer_reorder_nested_loop_join = false

  Note that these settings are experimental, and may change in the future.

- Added a :ref:`statement_timeout <conf-session-statement-timeout>` session
  setting and :ref:`cluster setting <statement_timeout>` that allows to set a
  timeout for queries.

- The severity of the node checks on the metadata gateway recovery settings
  has been lowered from `HIGH` to `MEDIUM` as leaving these to default
  or suboptimal values does not translate into data corruption or loss.

- Added the ability to set a
  :ref:`storage_class <sql-create-repo-s3-storage_class>` for S3 repositories.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue introduced with CrateDB ``5.3.0`` resulting in failing writes,
  broken replica shards, or even un-recoverable tables on tables using a
  column definition with a ``IP`` data type and an explicit ``INDEX OFF``.
  Any table that was created with ``INDEX OFF`` on a ``IP`` column and already
  written to with CrateDB version >= ``5.3.0`` should be recreated using e.g.
  :ref:`INSERT INTO new_table SELECT * FROM old_table<dml-inserting-by-query>`
  (followed by swap table
  :ref:`ALTER CLUSTER SWAP TABLE new_table TO old_table<alter_cluster_swap_table>`)
  or :ref:`restored from a backup<sql-restore-snapshot>`.

- Improved error message to be user-friendly, for definition of
  :ref:`CHECK <check_constraint>` at column level for object sub-columns,
  instead of a ``ConversionException``.

- Added validation to prevent creation of invalid nested array columns via
  ``INSERT INTO`` and dynamic column policy.

- Fixed parsing of ``ARRAY`` literals in PostgreSQL ``simple`` query mode.

- Fixed value of ``sys.jobs_log.stmt`` for various statements when issued via
  the PostgreSQL ``simple`` query mode by using the original query string
  instead of the statements string representation.

- Fixed an issue that could cause errors for queries with aggregations,
  ``UNION`` and ``LIMIT``, e.g. ::

    SELECT a, avg(c), b FROM t1 GROUP BY 1, 3
    UNION
    SELECT x, avg(z), y FROM t2 GROUP BY 1, 3
    UNION
    SELECT i, avg(k), j FROM t3 GROUP BY 1, 3
    LIMIT 10

- Fixed an issue which prevented ``INSERT INTO ... SELECT ...`` from inserting
  any records if the target table had a partitioned column of a non-string
  type, used in any expressions of ``GENERATED`` or ``CHECK`` definitions.

- Fixed an issue which caused ``INSERT INTO ... SELECT ...`` statements to
  skip ``NULL`` checks of ``CLUSTERED BY`` column values.

- Fixed an issue that resulted in enabled indexing for columns defined as
  the `BIT` data type even when explicitly turning it of using ``INDEX OFF``.

- Fixed an issue resulting in an exception when writing data into a column of
  type ``Boolean`` with disabled indexing using ``INDEX OFF``.

- Fixed an issue that caused an exception to be thrown when inserting a
  non-array value into a column that is dynamically created by inserting an
  empty array, ultimately modifying the type of the column and then selecting
  this column by the row's primary key, for example::

    CREATE TABLE t (id int primary key, o OBJECT(dynamic));
    INSERT INTO t VALUES (1, {x=[]});
    INSERT INTO t VALUES (2, {x={}});  /* this is the culprit statement, inserting an object onto an array typed column */

    SELECT * FROM t WHERE id=1;
    SQLParseException[Cannot cast object element `x` with value `[]` to type `object`]

  after the fix::

    SELECT * FROM t WHERE id=1;
    +----+-------------+
    | id | o           |
    +----+-------------+
    |  1 | {"x": null} |
    +----+-------------+
