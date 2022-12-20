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

- Fixed an issue that allowed users without the related privileges to check
  other users' privileges by calling
  :ref:`has_schema_privilege <scalar-has-schema-priv>` function.

- Fixed an issue that prevented :ref:`UDFs <user-defined-functions>` from
  accessing nested objects.

- Fixed an issue that caused ``SELECT *`` statements to fail if a table has an
  object with inner null object and a sibling column with the same name with
  one of the sub-columns. An example::

    CREATE TABLE IF NOT EXISTS "t" (
      "obj1" OBJECT(DYNAMIC) AS (
       "target" text,
       "obj2" OBJECT(DYNAMIC) AS (
          "target" REAL
       )
      )
    );
    INSERT INTO t VALUES ('{"obj2": null, "target": "Sensor"}');
    SELECT * FROM t;

- Fixed an issue that caused :ref:`swap table <alter_cluster_swap_table>` to
  consume invalid table names provided in a double-quoted string format
  containing ``.`` such as ``"table.t"`` by mis-interpreting it as
  ``"table"."t"``, which is a two double-quoted strings joined by a ``.``.
