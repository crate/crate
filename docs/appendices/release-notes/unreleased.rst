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

- Added ``attributes`` column to :ref:`sys.nodes <sys-nodes>` table to expose
  :ref:`custom node settings <conf-node-attributes>`.

- Added support for ``SCROLL`` and backward movement to cursors. See
  :ref:`DECLARE <sql-declare>` and :ref:`FETCH <sql-fetch>`.

- Added the :ref:`MAX_BY <aggregation-max_by>` and :ref:`MIN_BY
  <aggregation-min_by>` aggregation functions


Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed an issue that could lead to serialization errors when using the ``bit``
  type in objects.

- Fixed an issue that could lead to ``IllegalIndexShardStateException`` errors
  when running a ``SELECT count(*) FROM tbl`` on partitioned tables.

- Fixed an issue which caused ``PRIMARY KEY`` columns to be required on insert
  even if they are generated and their source columns are default not-null,
  i.e.::

    CREATE TABLE test (
      id INT NOT NULL PRIMARY KEY,
      created TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp NOT NULL,
      month TIMESTAMP GENERATED ALWAYS AS date_trunc('month', created) PRIMARY KEY
    );

    INSERT INTO test(id) VALUES(1);

- Fixed an issue that could cause ``COPY FROM``, ``INSERT INTO``,
  ``UPDATE`` and ``DELETE`` operations to get stuck if under memory pressure.

- Fixed an issue that didn't allow queries with a greater than ``0`` ``OFFSET``
  but without ``LIMIT`` to be executed successfully, i.e.::

    SELECT * FROM test OFFSET 10
    SELECT * FROM test LIMIT null OFFSET 10
    SELECT * FROM test LIMIT ALL OFFSET 10

- Fixed an issue that caused ``col IS NULL`` to match empty objects.

- Fixed an issue that caused ``ARRAY_COL = []`` to throw an exception on
  ``OBJECT``, ``GEO_SHAPE``, ``IP`` or ``BIT`` array element types.

- Fixed an issue that caused queries reading values of type ``BIT`` to return a
  wrong result if the query contains a ``WHERE`` clause ``pk_col = ?``
  condition.
