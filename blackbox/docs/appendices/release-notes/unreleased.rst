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

- Changed how columns of type :ref:`geo_point_data_type` are being communicated
  to PostgreSQL clients: Before clients were told that those columns are double
  arrays, now they're correctly mapped to the PostgreSQL ``point`` type. This
  means that applications using clients like ``JDBC`` will have to be adapted
  to use ``PgPoint``. (See `Geometric DataTypes in JDBC
  <https://jdbc.postgresql.org/documentation/head/geometric.html>`_)


Changes
=======

- Added :ref:`TIMEZONE <scalar-timezone>` scalar function.

- Added support for the filter clause in the
  :ref:`aggregate expressions <aggregate-expressions>`.

- Added support for `offset PRECEDING` and `offset FOLLOWING`
  :ref:`window definitions <window-definition>`.

- Added support for using :ref:`ref-values` as top-level relation.

- Added an optimization that allows to run `WHERE` clauses on top of
  derived tables containing :ref:`table functions <ref-table-functions>`
  more efficiently in some cases.

- Statements containing limits, filters, window functions or table functions
  will now be labelled accordingly in :ref:`sys-jobs-metrics`.

- Added support for the :ref:`named window definition <named-windows>`.
  It allows a user to define a list of window definitions in the
  :ref:`sql_reference_window` clause that can be referenced in :ref:`over`
  clauses.

- Add support for ``ROWS`` frame definitions in the context of window functions
  :ref:`window definitions <window-definition>`.

- The ``node`` argument of the :ref:`REROUTE <alter_table_reroute>` commands of
  :ref:`ref-alter-table` can now either be the id or the name of a node.

- Added a :ref:`PROMOTE REPLICA <alter_table_reroute>` sub command to
  :ref:`ref-alter-table`.

- Added support for the :ref:`lag <window-function-lag>` and
  :ref:`lead <window-function-lead>` window functions as enterprise features.

- Changed the default for :ref:`sql_ref_write_wait_for_active_shards` from
  ``ALL`` to ``1``. This will improve the out of box experience as it allows a
  subset of nodes to become unavailable without blocking write operations. See
  the documentation for more details about the implications.

- Added left and right scalar functions.

Fixes
=====

- Fixed an issue with using the same column under a different name/alias inside
  more complex queries. Example:
  ``SELECT count(*), t.x, t.x AS tx FROM t GROUP BY t.x``

- Fixed an issue with the ``collect_set`` function. It could compute an
  incorrect result if used as a window function with shrinking window frames.

- Fixed an issue with the version payload returned by HTTP, which resulted in
  falsely displaying CrateDB's version as a ``-SNAPSHOT`` version at the AdminUI.

- Fixed parsing of timestamps with a time zone offset of `+0000`.

- Fixed an issue that could cause startup to fail with early access builds of
  Java.
