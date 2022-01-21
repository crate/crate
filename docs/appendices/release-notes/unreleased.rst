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

- Deprecated support for HDFS snapshot repositories.

- The :ref:`gateway.expected_nodes <gateway.expected_nodes>` cluster setting
  has been marked as deprecated and will be removed in CrateDB 5.0.
  The :ref:`gateway.expected_data_nodes <gateway.expected_data_nodes>` must be
  used instead.

- The :ref:`gateway.recover_after_nodes <gateway.recover_after_nodes>` cluster
  setting has been marked as deprecated and will be removed in CrateDB 5.0.
  The :ref:`gateway.recover_after_data_nodes <gateway.recover_after_data_nodes>`
  must be used instead.

- The :ref:`ssl.transport.mode <ssl.transport.mode>` LEGACY mode has been
  deprecated for removal in CrateDB 5.0.

Changes
=======


SQL Statements and Compatibility
--------------------------------

- Added support for the :ref:`END <ref-end>` statement for improved PostgreSQL
  compatibility.

- Added support to use an aggregation in an order-by clause without having
  them in the select list like ``select x from tbl group by x order by count(y)``

- Changed the type precedence rules for ``INSERT FROM VALUES`` statements. The
  target column types now take higher precedence to avoid errors in statements
  like ``INSERT INTO tbl (text_column) VALUES ('a'), (3)``. Here ``3``
  (``INTEGER``) used to take precedence, leading to a cast error because ``a``
  cannot be converted to an ``INTEGER``.

  This doesn't change the behavior of standalone ``VALUES`` statements.
  ``VALUES ('a'), (3)`` as a standalone statement will still fail.

- Introduced ``RESPECT NULLS`` and ``IGNORE NULLS`` flags to window function
  calls. The following window functions can now utilize the flags: ``LEAD``,
  ``LAG``, ``NTH_VALUE``, ``FIRST_VALUE``, and ``LAST_VALUE``.

- Added ``FAIL_FAST`` option to ``COPY FROM`` statement that when it is set to
  true, any errors observed while processing the statement will trigger an
  exception and the on-going executions will terminate in best effort.



Scalar and Aggregation Functions
--------------------------------

- Added the scalar function :ref:`array_append
  <scalar-array_append>` which adds a value at the end of an array

- Registered the scalar function :ref:`array_to_string
  <scalar-array_to_string>` under the `pg_catalog` schema.

- Added the scalar function :ref:`pg_encoding_to_char
  <scalar-pg_encoding_to_char>` which converts an PostgreSQL encoding's internal
  identifier to a human-readable name.

- Added the scalar function :ref:`age <scalar-pg-age>` which returns
  :ref:`interval <type-interval>` between 2 timestamps.

- Added the :ref:`date_bin <date-bin>` scalar function that truncates timestamp
  into specified interval aligned with specified origin.

- Added the :ref:`scalar-array_slice` scalar function.

- Added support for the array slice access expression ``anyarray[from:to]``.

- Added support of ``numeric`` type to the ``avg`` aggregation function.

- Added the :ref:`scalar-area` scalar function that calculates the area for a
  ``GEO_SHAPE``.

- Enabled the setting of most prototype methods for JavaScript Objects (e.g.
  Array.prototype, Object.prototype) in :ref:`user-defined functions <user-defined-functions>`


New Tables and Schema Extensions
--------------------------------

- Added an empty ``pg_catalog.pg_locks`` table for improved PostgreSQL
  compatibility.

- Added an empty ``pg_catalog.pg_indexes`` table for compatibility with
  PostgreSQL.

- Added a new ``table_partitions`` column to the :ref:`sys.snapshots
  <sys-snapshots>` table.

- Added the `column_details` column to the `information_schema.columns` table
  including the top level column name and path information of object elements.


Administration and Operations
-----------------------------

- Added a :ref:`sys node check for max shards per node
  <sys-node_checks_max_shards_per_node>` to verify that the amount of shards on the
  current node is less than 90 % of  :ref:`cluster.max_shards_per_node
  <cluster.max_shards_per_node>`. The check is exposed via :ref:`sys.node_checks
  <sys-node-checks>`.

- Added ``error_on_unknown_object_key`` session setting. This will either allow
  or suppress an error when unknown object keys are queried from dynamic
  objects.

- Enabled HTTP connections to preserve :ref:`session settings <conf-session>`
  across the requests as long as the connection is re-used.

  Note that connections are established on an individual node to node basis. If
  a client sends requests to different nodes, those won't share the same
  session settings, unless the client sets the session settings on each node
  individually.

- Improved the visual layout of the administration console: Remove dedicated
  "Monitoring" page and move its contents to the "Overview" page.

- Added ``switch_to_plaintext`` :ref:`Host-Based Authentication <admin_hba>`
  option for enabling plain text connection for intra-zone communications.


New Types
---------

- Added ``float4`` type as alias to ``real`` and ``float8`` type as alias to
  ``double precision``

- Added the :ref:`JSON type <data-type-json>`.


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that caused truncation of milliseconds in ``timezone`` scalar
  function.

None
