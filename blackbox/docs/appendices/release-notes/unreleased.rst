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

Changes
=======

None

Fixes
=====

- Fixed an issue that could prevent accounted memory from being properly
  de-accounted on queries using ``hyperloglog_distinct``, leading clients to
  eventually receive ``CircuitBreakingException`` error messages and also
  breaking internal recovery operations.

- Fixed an issue that caused the users list in the privileges tab to not
  displayed when the CrateDB Admin UI is not served from ``/``.

- Fixed various issues in the CrateDB Admin UI console.

- Fixed an issue that caused the Twitter tutorial to not start automatically
  after the login redirect in the CrateDB Admin UI.

- Fixed an issue that prevented subqueries from being used in select item
  expressions that also contain a reference accessed via a relation alias.
  For example: ``SELECT t.y IN (SELECT x FROM t2) FROM t1 t``

- Fail the storage engine if indexing on a replica shard fails after it was
  successfully done on a primary shard. It prevents replica and primary shards
  from going out of sync.

- Fixed bug in the disk threshold decider logic that would ignore to account
  new relocating shard (``STARTED`` to ``RELOCATING``) when deciding how to
  allocate or relocate shards with respect to
  :ref:`cluster.routing.allocation.disk.watermark.low
  <cluster.routing.allocation.disk.watermark.low>` and
  :ref:`cluster.routing.allocation.disk.watermark.high
  <cluster.routing.allocation.disk.watermark.high>` settings.

- Fixed regression that prevented shards from reallocation when a node passes
  over :ref:`cluster.routing.allocation.disk.watermark.high
  <cluster.routing.allocation.disk.watermark.high>`.

- Removed a case where a ``NullPointerException`` was logged if a HTTP client
  disconnected before a pending response could be sent to the client.
