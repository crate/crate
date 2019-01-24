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

.. rubric:: Table of Contents

.. contents::
   :local:

Breaking Changes
================

None

Changes
=======

- Added the ``pg_type.typlen`` column for improved postgresql compatibility.

- Marked ``CREATE INGEST RULE`` and ``DROP INGEST RULE`` as deprecated. Given
  that the only implementation (MQTT) was deprecated and will be removed, the
  framework itself will also be removed.

- Added ``current_schemas(boolean)`` scalar function which will return the
  names of schemas in the ``search_path``.

- Added support for the ``first_value``, ``last_value`` and ``nth_value``
  window functions as enterprise features.

- Implemented the ``DROP ANALYZER`` statement to support removal of custom
  analyzer definitions from the cluster.

- Output the custom analyzer/tokenizer/token_filter/char_filter definition inside
  the ``information_schema.routines.routine_definition`` column.


- Added a ``pg_description`` table to the ``pg_catalog`` schema for improved
  postgresql compatibility.

- Added support for window function ``row_number()``.

- Added support to use any expression in the operand of a ``CASE`` clause.

- Buffer the file output of ``COPY TO`` operations to improve performance by not
  writing to disk on every row.

Fixes
=====
