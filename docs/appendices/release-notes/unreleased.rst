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
