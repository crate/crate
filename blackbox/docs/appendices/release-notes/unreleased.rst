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

Changes
=======

- Added support for ``SHOW parameter_name`` to retrieve current session
  parameter values.

- Added support for ``initcap(string)`` scalar expression.

- Improved the compatibility with postgres clients that use the ``text`` type
  for parameter encoding.

-  Upgraded the Admin UI to 1.11.2 which includes the following changes:

   Changed the license information (ident) to be taken from the
   `sys.cluster.licence` attribute instead of the `license.ident` setting
   which is `@deprecated`.

   Addition of French language files and menu options.

   Various Admin UI improvements.

- Changed PostgreSQL wire interface to emulate version ``10.5``.

-  Added support for CrateDB license management
   enabling users to trial the enterprise features,
   set a production enterprise license or continue
   using the community edition.

   New ``set license`` statement added for license registration.

   Setting ``license.ident`` becomes ``@deprecated``.

- The ``mqtt`` endpoint has been deprecated and will be removed in a future
  version.

- Upgraded to Elasticsearch 6.4.2

- Deprecated the ``http.enabled`` setting.

- Added a ``remove_duplicates`` token filter.

- Added a ``char_group`` tokenizer.

- Renamed the ``delimited_payload_filter`` token filter to
  ``delimited_payload``. The old name can still be used, but is deprecated.

- Added the ``generate_series(start, stop [, step ])`` table function.

- Added ``pg_class``, ``pg_namespace``, ``pg_attribute``, ``pg_attrdef``,
  ``pg_index`` and ``pg_constraint`` tables to the ``pg_catalog`` schema for
  improved compatibility with postgresql.

- Added support for using table functions in the select list of a query.

- Implemented the ``array_upper``, ``array_length`` and ``array_lower`` scalars
  that return the upper and respectively lower bound of a given array
  dimension.

- Added some type aliases for improved compatibility with postgresql.

- Expand the ``search_path`` setting to accept a list of schemas that will be
  searched when a relation (table, view or user defined function) is referenced
  without specifying a schema. The system ``pg_catalog`` schema is implicitly
  included as the first one in the path.

- Improved the handling of function expressions inside subscripts used on
  object columns. This allows expressions like ``obj['x' || 'x']`` to be used.

- The ``= ANY`` operator now also supports operations on object arrays or
  nested arrays. This enables queries like ``WHERE ['foo', 'bar'] =
  ANY(object_array(string_array))``.

- Added support for the ``array(subquery)`` expression.

- ``<object_column> = <object_literal>`` comparisons now try to utilize the
  index for the objects contents and can therefore run much faster.

- Values of byte-size and time based configuration setting do not require a unit
  suffix anymore. Without a unit time values are treat as milliseconds since
  epoch and byte size values are treat as bytes.

- Added support of using units inside byte-size or time bases statement
  parameters values. E.g. '1mb' for 1 MegaByte or '1s' for 1 Second.

Fixes
=====
