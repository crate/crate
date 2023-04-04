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

- Removed support for ``SET LICENSE``. The statement had no effect since CrateDB
  4.5 and was only kept for backward compatibility.


Deprecations
============

None


Changes
=======

SQL Standard And PostgreSQL Schema Compatibility
------------------------------------------------

- Changed the behavior of ``SHOW search_path`` to omit the implicit
  ``pg_catalog`` schema, unless the user set it explicitly. This matches the
  PostgreSQL behavior.

- Allowed schema and table names to contain upper case letters. This can be
  achieved by quoting the names. Unquoted names with upper case letters are
  converted to lower cases which has been the existing behaviour.

- Allowed schema and table names to start with ``_``.

- Added the :ref:`col_description(integer, integer) <scalar-col_description>` scalar
  function for improved PostgreSQL compatibility. CrateDB does not support
  comments for columns, so this function always returns ``NULL``.

Performance Improvements
------------------------

- Optimized the evaluation of ``CASE`` expressions to prevent stack overflows
  for very large expressions.

- Improved the performance of queries using a correlated sub-query inside the
  ``WHERE`` clause in conjunction with a non-correlated filter clause.

- Improved performance of statements that create multiple partitions at once,
  which can occur during ``COPY FROM`` or INSERTS with multi-values into
  partitioned tables.

- Improved ingestion performance by up to 30%.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that prevented PostgreSQL wire protocol clients from being able
  to ``describe`` the query of a cursor created using ``DECLARE``. An example of
  a client that uses the functionality is ``psycopg3``. A snippet like the
  following failed::

    import psycopg
    conn = psycopg.connect("host=localhost port=5432 user=crate")
    cur = conn.cursor(name="foo")
    cur.execute("select 1")
    for row in cur.fetchall():
        print(row)

- Fixed an issue in the PostgreSQL wire protocol implementation that could
  prevent protocol level fetch from working correctly with some clients. An
  example client is `pg-cursor <https://www.npmjs.com/package/pg-cursor>`_.

- Fixed an issue that a wrong HTTP response was sent, when trying to ``POST`` to
  an invalid URL, causing the HTTP client to stall.

- Fixed response for HTTP ``GET`` request to not expose internal paths when the
  requested URL doesn't exist.

- Fixed a performance regression for queries that used a scalar sub-query in the
  ``WHERE`` which itself also filtered on columns in a ``WHERE`` without
  selecting those columns. An example::

    SELECT name FROM users
      WHERE id IN (SELECT user_id FROM hits WHERE ts > '2023-01-01')

- Fixed default behaviour for :ref:`CURSOR <sql-declare>`'s
  :ref:`SCROLL <sql-declare-scroll>`. When neither ``SCROLL`` nor ``NO SCROLL``
  is provided in the statement, ``NO SCROLL`` is now assumed.

- Fixed a race condition that could lead to a ``ShardNotFoundException`` when
  executing ``UPDATE`` statements.

- Fixed an issue that caused a subset of a ``WHERE`` clause to be lost from a
  ``JOIN`` statement. The first trigger condition was using a column by itself
  as a boolean expression. For example from ``WHERE NOT b AND c`` the column
  ``c`` representing a boolean expression ``c = TRUE`` overrode ``NOT b``. The
  second trigger condition was :ref:`MATCH predicate <predicates_match>`
  which also overrode preceding ``WHERE`` conditions.

- Fixed an issue that caused a ``ColumnUnknownException`` when creating a table
  with a ``generated column`` involving a subscript expression with a root
  column name containing upper cases.
  An example::

    CREATE TABLE t ("OBJ" OBJECT AS (intarray int[]), firstElement AS "OBJ"['intarray'][1]);
    ColumnUnknownException[Column obj['intarray'] unknown]

- Fixed a ``NullPointerException`` which occurs when using NULL as a setting value.

- Fixed a resource leak that could happen when inserting data which causes
  constraints violation or parsing errors.

