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

None


Deprecations
============

None

Changes
=======

None

Fixes
=====

- Fixed an issue that could cause browsers to prompt for a client certificate
  if ``SSL`` is enabled on the server side, even if no cert authentication
  method is configured.

- Fixed a regression introduced Crate ``4.1`` resulting in duplicated recovery
  file chunk responses sent.
  This causes log entries of `Transport handler not found ...`.

- Fixed an issue that resulted in records in ``pg_catalog.pg_proc`` which
  wouldn't be joined with ``pg_catalog.pg_type``. Clients like ``npgsql`` use
  this information and without it the users received an error like ``The CLR
  array type System.Int32[] isn't supported by Npgsql or your PostgreSQL`` if
  using array types.

- Fixed an issue that could lead to stuck ``INSERT INTO .. RETURNING`` queries.

- Fixed a regression introduced in CrateDB >= ``4.3`` which prevents using
  ``regexp_matches()`` wrapped inside a subscript expression from being used
  as a ``GROUP BY`` expression.
  This fixed the broken AdminUI->Montoring tab as it uses such an statement.

- Fixed validation of ``GROUP BY`` expressions if an alias is used. The
  validation was by passed and resulted in an execution exception instead of
  an user friendly validation exception.

- Fixed an issue that caused ``IS NULL`` and ``IS NOT NULL`` operators on
  columns of type ``OBJECT`` with the column policy ``IGNORED`` to match
  incorrect records.

- Fixed an issue that led to an error like ``UnsupportedOperationException:
  Can't handle Symbol [ParameterSymbol: $1]`` if using ``INSERT INTO`` with a
  query that contains parameter place holders and a ``LIMIT`` clause.

- Fixed an issue that led to an error if a user nested multiple table
  functions.
