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

- Fixed an issue that prevented ``DEFAULT`` clauses from being evaluated per
  record in ``INSERT`` statements with multiple source values. This resulted in
  the same values being inserted when using nondeterministic functions like
  ``gen_random_text_uuid`` as default expression.

- Fixed an issue that prevented aggregations or grouping operations on virtual
  tables to run parallel on shard level, even if the inner query would support
  it.

- Fixed an issue that prevented ``INSERT INTO`` statements where the source is
  a query that selects an object column which contains a different set of
  columns than the target object column.

- Fixed an issue that could lead to errors when using ``DISTINCT`` or ``GROUP
  BY`` with duplicate columns.

- Fixed an issue that could cause ``GROUP BY`` queries with a ``LIMIT`` clause
  and aliased columns to fail.

- Fixed an issue that prevented ``LIKE`` operators from using the index if the
  left operand was a varchar column with length limit, and the right operand a
  literal.

- Fixed an issue that resulted in more data being snapshot than expected if
  only concrete tables were snapshot by the
  ``CREATE SNAPSHOT ... TABLE [table, ...]``. Instead of just the concrete
  tables, also the metadata of partitioned table, views, users, etc. were
  falsely stored.

- Fixed an issue that resulted in a non-executable plan if a windows function
  result from a sub-select is used inside a query filter. An example::

      SELECT * FROM (
        SELECT ROW_NUMBER() OVER(PARTITION by col1) as row_num
        FROM (VALUES('x')) t1
      ) t2
      WHERE row_num = 2;

- Fixed an issue that caused valid values for ``number_of_routing_shards`` in
  ``CREATE TABLE`` statements to be rejected because the validation always used
  a fixed value of ``5`` instead of the actual number of shards declared within
  the ``CREATE TABLE`` statement.

- Fixed an issue that caused incorrect classification for DELETE and UPDATE
  queries with sub-select. Statement type for those queries was always SELECT.

- Fixed an issue that threw an exception when ``ORDER BY`` clauses contain
  the output column position or the alias name of an aliased column.
