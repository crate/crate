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

- Added a :ref:`btrim <scalar-btrim>` scalar function.

- Added support for underscores in numeric literals. Example::

    SELECT 1_000_000;

- Added a :ref:`statement_timeout <conf-session-statement-timeout>` session
  setting that allows to set a timeout for queries.

- Added ``any_value`` as an alias to the ``arbitrary`` aggregation function, for
  compliance with the SQL2023 standard. Extended the aggregations to support any
  type.

- Added support for ``ORDER BY``, ``MAX``, ``MIN`` and comparison operators on
  expressions of type ``INTERVAL``.

- Bumped the version of PostgreSQL wire protocol to ``14`` since ``10`` has been
  deprecated.

- Allow casts in both forms: ``CAST(<literal or parameter> AS <datatype>)`` and
  ``<literal or parameter>::<datatype>`` for ``LIMIT`` and ``OFFSET`` clauses,

  e.g.::

    SELECT * FROM test OFFSET CAST(? AS long) LIMIT '20'::int

- Added the ``pg_catalog.pg_cursors`` table to expose open cursors.

- Added the
  :ref:`standard_conforming_strings <conf-session-standard_conforming_strings>`
  read-only session setting for improved compatibility with PostgreSQL clients.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Added a :ref:`null_or_empty <scalar-null-or-empty>` scalar function that can
  be used as a faster alternative to `IS NULL` if it's acceptable to match on
  empty objects. This makes it possible to mitigate a performance regression
  introduced in 5.0.3 and 5.1.1

- Fixed an issue that led to ``NullPointerException`` when trying to query an
  ``OBJECT`` field with no values, using the ``NOT`` operator, e.g.::

     CREATE TABLE test (obj OBJECT(DYNAMIC)); -- no data
     SELECT myobj FROM test WHERE (obj::TEXT) NOT LIKE '%value%';

- Fixed an issue in the PostgreSQL wire protocol implementation that could
  lead to ``ClientInterrupted`` errors with some clients. An
  example client is `pg-cursor <https://www.npmjs.com/package/pg-cursor>`_.

- Fixed an issue that allowed creating columns with names conflicting with
  subscript pattern, such as ``"a[1]"``, a subscript expression enclosed in
  double quotes.

- Fixed an issue that caused ``SQLParseException`` when quoted subscript
  expressions contained quotes. An example would be querying an array with the
  name containing quotes like ``SELECT "arr""[1]";``.

- Fixed an issue that caused ``ALTER TABLE ADD COLUMN`` statement to assign
  ``PRIMARY KEY`` to wrong columns, when adding multiple primary key columns,
  having none-primary columns in-between.

- Fixed an issue that caused ``ALTER TABLE ADD COLUMN`` statement to assign a
  wrong type to ``ARRAY(TEXT)`` column and create a ``TEXT`` column instead if
  column has a ``FULLTEXT`` index.
