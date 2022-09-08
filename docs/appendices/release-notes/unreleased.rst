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

- Removed the ``indices.breaker.fielddata.limit`` setting and the ``*.overhead``
  settings for all circuit breakers. They were deprecated in 4.3.0 and had no
  effect since then.

- Removed the deprecated ``discovery.zen.publish_timeout``,
  ``discovery.zen.commit_timeout``, ``discovery.zen.no_master_block``,
  ``discovery.zen.publish_diff.enable`` settings.
  They had no effect since 4.0.0 and have been deprecated in 4.4.0.

- Removed the deprecated azure discovery functionality.

- Fields referencing ``catalog`` in :ref:`information_schema <information_schema>`
  tables now return ``'crate'`` (the only catalog in CrateDB) instead of the
  table ``schema``.

Deprecations
============

- Deprecated the ``upgrade_segments`` option of the ``OPTIMIZE TABLE``
  statement. The option will now longer have any effect and will be removed in
  the future.


Changes
=======

- Added support for ``SET TIME ZONE`` to improve PostgreSQL Compatibility.
  Timezone will be ignored on the server side.

- Added support for the ``EXISTS`` expression.

- Added support for ``'YES'``, ``'ON'`` and ``'1'`` as alternative way to
  specify a ``TRUE`` boolean constant and ``'NO'``, ``'OFF'`` and ``'0'`` as
  alternative way to specify ``FALSE`` boolean constant improving compatibility
  with PostgreSQL.

- Changed the ``interval`` parameter of ``date_trunc`` to be case insensitive.

- Added support for correlated scalar sub-queries within the select list of a
  query. See :ref:`Scalar subquery <sql-scalar-subquery>`.

- Improve performance of queries on ``sys.snapshots``.

- Added a ``application_name`` session setting that can be used to identify
  clients or applications which connect to a CrateDB node.

- Added support for ``catalog`` in fully qualified table and column names,
  i.e.::

    SELECT * FROM crate.doc.t1;
    SELECT crate.doc.t1.a, crate.doc.t1.b FROM crate.doc.t1;

- Added support of ``GROUP BY`` on ``ARRAY`` typed columns.

- Added support for casting ``TIMESTAMP`` and ``TIMESTAMP WITHOUT TIME ZONE``
  values to the ``DATE`` data type and vice versa.

- Made the commas between successive ``transaction_modes`` of the ``BEGIN`` and
  its SQL equivalent ``START TRANSACTION`` statement optional to support
  compatibility with clients and tools using an older (< 8.0) PostgreSQL syntax.

Fixes
=====

.. If you add an entry here, the fix needs to be backported to the latest
.. stable branch. You can add a version label (`v/X.Y`) to the pull request for
.. an automated mergify backport.

None
