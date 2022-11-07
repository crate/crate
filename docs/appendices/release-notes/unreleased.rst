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

- Removed support for the ``CRATE_INCLUDE`` environment variable from the
  ``bin/crate`` start script.
  Configuration of CrateDB should happen via the ``crate.yml``, the
  ``CRATE_HEAP_SIZE`` environment variable and optionally ``CRATE_JAVA_OPTS``.

- Removed support for the ``-d`` and ``-p`` options from the ``bin/crate`` start
  script. It's recommended to run CrateDB either via a container runtime like
  Docker, or via a service manager like ``systemd`` where these options are not
  required.

Deprecations
============

None


Changes
=======

- Exposed the ``require``, ``include`` and ``exclude`` ``routing.allocation``
  settings per partition within ``information_schema.table_partitions``.

- ``cancel`` messages sent from a client via the PostgreSQL wire protocol are
  now internally forwarded to other nodes to support setups with load-balancers.

- Extended the syntax for ``CREATE VIEW`` to allow parenthesis surrounding the
  query.

- Added ``attributes`` column to :ref:`sys.nodes <sys-nodes>` table to expose
  :ref:`custom node settings <conf-node-attributes>`.

- Added support for ``SCROLL`` and backward movement to cursors. See
  :ref:`DECLARE <sql-declare>` and :ref:`FETCH <sql-fetch>`.

- Added the :ref:`MAX_BY <aggregation-max_by>` and :ref:`MIN_BY
  <aggregation-min_by>` aggregation functions

- Added support for :ref:`bit operators <bit-operators>` on integral and
  ``BIT`` types.

- Added a :ref:`WITH clause <sql-copy-from-with>` option :ref:`SKIP
  <sql-copy-from-skip>` for :ref:`COPY FROM <sql-copy-from>` which allows
  skipping rows from the beginning while copying data.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

- Fixed a race condition that could lead to a ``NullPointerException`` when
  using ``IS NULL`` on an object that was just added to a table.

- Fixed an issue that caused the generated expressions on columns of type
  ``GEO_SHAPE`` not being evaluated on writes and such being ignored.

- Fixed an issue that could generate duplicate data on ``COPY FROM``  while
  some internal retries were happening due to I/O errors e.g. socket timeouts.
