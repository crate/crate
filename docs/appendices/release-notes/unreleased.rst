
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

- Fixed an issue that could lead to queries to become stuck instead of failing
  with a circuit breaker error if a node is under memory pressure.

- Improved an optimization rule to enable index lookups instead of table scans
  in more cases. This is a follow up to a fix in 5.2.7 which fixed a regression
  introduced in 5.2.3.

- Fixed an issue that caused ``DROP TABLE IF EXISTS`` to wrongly return ``1``
  row affected or ``SQLParseException`` (depending on user privileges), when
  called on an existent schema, a non-existent table and with the ``crate``
  catalog prefix, e.g.::

    DROP TABLE IF EXISTS crate.doc.non_existent_table

- Improved output representation of timestamp subtraction, by normalizing to
  bigger units, but no further than days, to be consistent with PostgreSQL
  behavior. e.g::

    SELECT '2022-12-05T11:22:33.123456789+05:30'::timestamp - '2022-12-03T11:22:33.123456789-02:15'::timestamp

  previously would return: ``PT40H15M`` and now returns: ``P1DT16H15M``.

- Improved error message for :ref:`date_bin <date-bin>` scalar function when the
  first argument of :ref:`INTERVAL data type <type-interval>` contains month
  and/or year units.

- Added a workaround for an issue that allowed inserting a non-array value onto
  a column that is dynamically created by inserting an empty array, ultimately
  modifying the type of the column. The empty arrays will be convert to
  ``nulls`` when queried. For example::

    CREATE TABLE t (o OBJECT);
    INSERT INTO t VALUES ({x=[]});
    INSERT INTO t VALUES ({x={}});  /* this is the culprit statement, inserting an object onto an array typed column */
    SHOW CREATE TABLE t;
    +-----------------------------------------------------+
    | SHOW CREATE TABLE doc.t                             |
    +-----------------------------------------------------+
    | CREATE TABLE IF NOT EXISTS "doc"."t" (              |
    |    "o" OBJECT(DYNAMIC) AS (                         |
    |       "x" OBJECT(DYNAMIC)  /* an array type modified to an object type */
    SELECT * FROM t;
    +-------------+
    | o           |
    +-------------+
    | {"x": {}}   |
    | {"x": null} |  /* an empty array converted to null */
    +-------------+

- Fixed an issue that caused ``AssertionError`` to be thrown when referencing
  previous relations, not explicitly joined, in an join condition, e.g.::

    SELECT * FROM t1
    CROSS JOIN t2
    INNER JOIN t3 ON t3.x = t1.x AND t3.y = t2

