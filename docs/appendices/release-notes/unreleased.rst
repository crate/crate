
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

None


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue introduced in 5.2.0 which led ``INNER JOIN`` queries to produce
  0 or wrong results when filtering with a constant value is used in the join
  condition, e.g.::

    SELECT * FROM t1 INNER JOIN t2 ON t1.a = 10 AND t1.x = t2.y

- Fixed a performance regression introduced in 5.2.3 which led to filters on
  object columns resulting in a table scan if used with views or virtual tables.
  See `#14015 <https://github.com/crate/crate/issues/14015>`_ for details.

- Fixed an issue that caused ``geo_shape_array IS NULL`` expressions to fail
  with an ``IllegalStateException``.

- Fixed an issue that caused the actual cast/type conversion error to be hidden
  when it failed for a sub-column of an object column, when using a client
  statement with parameters i.e (python).::

    CREATE TABLE a (b OBJECT(DYNAMIC) AS (c REAL));
    # create a connection and a cursor and then:
    cursor.execute("INSERT INTO a VALUES (?)", [({"c": True},)])

- Fixed a regression that caused the ``-h`` option in ``bin/crate`` to fail with
  an ``Error parsing arguments!`` error.

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
  wrong type to ``ARRAY(GEO_SHAPE)`` column and create a ``GEO_SHAPE`` column
  instead.

- Fixed an issue that caused ``ALTER TABLE ADD COLUMN`` statement to assign a
  wrong type to ``ARRAY(TEXT)`` column and create a ``TEXT`` column instead if
  column has a ``FULLTEXT`` index.

- Fixed an issue that prevented assigning default expression to ``ARRAY``
  columns.
