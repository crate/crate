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

- Fixed an issue that declared the rule optimizer settings as global. The
  settings are now session local.

- Fixed an issue that prevented the ``MATCH`` predicate from working in mixed
  clusters running 4.1.8 and 4.2.

- Fixed an issue that prevented user defined functions in a custom schema from
  working if used in a generated column expression.

- Fixed an issue that allowed users to use a function in a generated column
  that didn't fully match the given arguments, leading to a subsequent runtime
  failure when trying to access tables.

- Fixed exposure of the full qualified name of a sub-script column in
  `information_schema.tables.partitioned_by` and
  `pg_catalog.pg_attribute.attname` to use the CrateDB SQL compatible identifier.

- Fixed an issue that led to a ``Message not fully read`` error when trying to
  decommission a node using ``ALTER CLUSTER DECOMMISSION``.

- Fixed an issue that resulted in incorrect results when querying the
  ``sys.nodes`` table. Predicates used in the ``WHERE`` clause on columns that
  were absent in the select-list never matched.
