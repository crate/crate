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

- Changed the behavior of ``SHOW search_path`` to omit the implicit
  ``pg_catalog`` schema, unless the user set it explicitly. This matches the
  PostgreSQL behavior.

- Optimized the evaluation of ``CASE`` expressions to prevent stack overflows
  for very large expressions.

- Allowed schema and table names to contain upper case letters. This can be
  achieved by quoting the names. Unquoted names with upper case letters are
  converted to lower cases which has been the existing behaviour.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed behaviour of :ref:`FETCH RELATIVE <sql-fetch>`, which previously behaved
  identically to `FETCH FORWARD` and `FETCH BACKWARD`, whereas it should behave
  similarly to `FETCH ABSOLUTE`, with the difference that the position of the 1
  row to return is calculated relatively to the current cursor position.

- Fixed an issue that caused accounted memory not to be released when using
  :ref:`cursors <sql-fetch>`, even if the ``CURSOR`` was explicitly or
  automatically (session terminated) closed.

- Fixed an issue that caused the returned column names to be missing the
  subscripts when querying sub-columns of nested object arrays.

- Fixed an issue that caused ``ClassCastException`` when accessing a sub-column
  of a nested object array where the sub-column resolves to a nested array.
  An example ::

    CREATE TABLE test (
      "a" ARRAY(OBJECT AS (
        "b" ARRAY(OBJECT AS (
          "s" STRING
        )))));
    INSERT INTO test (a) VALUES ([{b=[{s='1'}, {s='2'}, {s='3'}]}]);
    SELECT a['b'] FROM test; // a['b'] is type of array(array(object))

- Added validation to reject inner column names containing whitespace characters
  to avoid invalid schema definitions.

- Fixed an issue that caused ``IndexOutOfBoundsException`` to be thrown when
  trying to :ref:`FETCH <sql-fetch>` backwards from a ``CURSOR``, past the 1st
  row.

- Fixed an issue that caused wrong rows to be returned when scrolling backwards
  and then forwards through a :ref:`CURSOR <sql-fetch>`.

- Improved error message when :ref:`fetching <sql-fetch>` using ``ABSOLUTE``,
  past the last row returned by the cursor query.

- Fixed and issue that caused wrong or 0 rows to be returned when trying to
  fetch all rows backwards from a ``CURSOR`` using: ``FETCH BACKWARDS ALL``.

- Fixed an issue that caused :ref:`swap table <alter_cluster_swap_table>` to
  consume invalid table names provided in a double-quoted string format
  containing ``.`` such as ``"table.t"`` by mis-interpreting it as
  ``"table"."t"``, which is a two double-quoted strings joined by a ``.``.
  This caused metadata corruptions leading to ``StartupExceptions`` and data
  losses. Corrupted metadata recovery is in place to prevent the exceptions
  but not all data can be recovered.

- Fixed an issue in the PostgreSQL wire protocol that would cause
  de-serialization of arrays to fail if they contained unquoted strings
  consisting of more than 2 words.
