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

- The ``array_unique`` scalar function use a common element type based on the
  type precedence if given arrays have different element types instead of always
  casting to the element type of the first array argument.

- Remap CrateDB :ref:`object_data_type` array data type from the PostgreSQL
  JSON to JSON array type. That might effect some drivers that use the
  PostgreSQL wire protocol to insert data into tables with object array typed
  columns. For instance,  when using the ``Npgsql`` driver, it is not longer
  possible to insert an array of objects into a column of the object array
  data type by using the parameter of a SQL statement that has the JSON data
  type and an array of CLR as its value. Instead, use a string array with JSON
  strings that represent the objects. See the ``Npgsql`` documentation for
  more details.

- Bulk ``INSERT INTO ... VALUES (...)`` statements do not throw an exception
  any longer when one of the bulk operations fails. The result of the
  execution is only available via the ``results`` array represented by a
  row count for each bulk operation.

Deprecations
============

None

Changes
=======

- Added the scalar function ``CURRENT_TIME``, which returns the time portion
  of ``CURRENT_TIMESTAMP`` in absolute milliseconds, see :ref:`current_time`.

- Added the ``varchar`` and ``character varying`` types. Currently as aliases
  for ``text``. Length limitations are not supported.

- Add the :ref:`CHECK <check_constraint>` constraint syntax, which specifies
  that the values of certain columns must satisfy a boolean expression on
  insert and update.

- Optimized `<column> IS NOT NULL` queries.

- Include the bundled version of ``OpenJDK`` (13.0.2+8) into the ``CrateDB``
  built. It means that ``CrateDB`` doesn't rely the ``JAVA_HOME`` of the host
  system any longer.

- Removed a node check that checks the JVM version under which CrateDB is
  running.

- Added the `pg_catalog.pg_proc <postgres_pg_catalog>`_ table.

- Added :ref:`length <scalar-length>` and :ref:`repeat <scalar-repeat>`
  scalar functions.

- Added the :ref:`server_version_num <conf-session-server_version_num>` and
  :ref:`server_version <conf-session-server_version>` read-only session
  settings.

- Added the :ref:`array_agg <array_agg>` aggregation function.

- Added entries for primary keys to ``pg_class`` and ``pg_index`` table.

- Added support for ``GROUP BY`` operations on analysed columns of type
  ``text``.

- Added the :ref:`trunc <scalar-trunc>` scalar function.

- Added the :ref:`now <now>` scalar function.

- Introduced new optional ``RETURNING`` clause for :ref:`Insert <ref-insert>` to
  return specified values from each row inserted.

- Added a ``mod`` alias for the :ref:`modulus <scalar-modulus>` function for
  improved PostgreSQL compatibility.

- Added the :ref:`atan2 <scalar-atan2>` trigonometric scalar function.

- Added the :ref:`exp <scalar-exp>` scalar function.

- Added the :ref:`degrees <scalar-degrees>` and :ref:`radians <scalar-radians>`
  scalar functions.

- Added support for using :ref:`table functions <ref-table-functions>` with
  more than one column within the select list part of a SELECT statement.

- Added :ref:`postgres_pg_type` columns: ``typbyval``, ``typcategory``,
  ``typowner``, ``typisdefined``, ``typrelid``, ``typndims``,
  ``typcollation``, ``typinput``, ``typoutput``, and ``typndefault`` for improved
  PostgreSQL compatibility.

- Replaced the ``Nashorn`` JavaScript engine with ``GraalVM`` for JavaScript
  :ref:`user-defined functions <sql_administration_udf>`. This change upgrades
  ``ECMAScript`` support from ``5.1`` to ``10.0``.

- Added support for ``JOIN USING``, e.g. ``SELECT * FROM t1 JOIN t2 USING (col)``,
  an alternative to ``JOIN ON``, when the column name(s) are the same in both relations.

- Added the :ref:`cot <scalar-cot>` trigonometric scalar function.

- Added support for :ref:`record subscript <record-subscript>` syntax as
  alternative to the existing :ref:`object subscript <object-subscript>`
  syntax.

- Added the :ref:`pi <scalar-pi>` scalar function.

- Added a ``ceiling`` alias for the :ref:`ceil <scalar-ceil>` function for
  improved PostgreSQL compatibility.

- Added the :ref:`encode(bytea, format) <scalar-encode>` and :ref:`decode(text,
  format) <scalar-decode>` string functions.

- Added the :ref:`ascii <scalar_ascii>` scalar function.

- Introduced new optional ``RETURNING`` clause for :ref:`Update <ref-update>` to
  return specified values from each row updated.

- Added the :ref:`obj_description(integer, text) <obj_description>` scalar
  function for improved PostgreSQL compatibility.

- Added support for using columns of type ``long`` inside subscript expressions
  (e.g., ``array_expr[column]``).

- Made :ref:`generate_series <table-functions-generate-series>` addressable by
  specifying the ``pg_catalog`` schema explicitly. So, for example, both
  ``generate_series(1, 2)`` and ``pg_catalog.generate_series(1, 2)`` are valid.

- Added the :ref:`version() <version>` system information function.

- Added support for the PostgreSQL notation to refer to array types. For
  example, it is now possible to use ``text[]`` instead of ``array(test)``.

Fixes
=====

None
