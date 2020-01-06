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

- Fixed an NPE which occurred when using the ``current_timestamp`` inside the
  ``WHERE`` clause on a **view** relation.

- Fixed the data type of the ``sys.jobs_metrics.classification['labels']``
  column, should be ``text_array`` instead of an ``undefined`` type.

- Fixed an issue that caused a type cast error in ``INSERT`` statements if the
  target table contained a ``array(object() as (...)`` column where a child of
  the object array contained a ``NOT NULL`` constraint.

- Fixed a ``NullPointerException`` that could prevent a node from starting up.
  This could occur if the node crashed or disconnected while a user deleted a
  table.

- Improved the memory accounting for values of type ``geo_shape``, ``object``
  or ``undefined``. Previously an arbitrary fixed value was used for memory
  accounting. If the actual payloads are large, this could have led to out of
  memory errors as the memory usage was under-estimated.

- Fixed the type information of the ``fs['data']`` and ``fs['disks']`` column
  in the ``sys.nodes`` table. Querying those columns could have resulted in
  serialization errors.

- Fixed the support for the ``readonly`` property in ``CREATE REPOSITORY``.

- Fixed an issue that may cause a ``SELECT`` query to hang on multiple nodes
  cluster if a resource error like a ``CircuitBreakingException`` occurs.

- Fixed an issue that caused a ``INSERT INTO ... (SELECT ... FROM ..)``
  statement to fail if not all columns of a ``PARTITIONED BY`` clause
  appeared in the target list of the ``INSERT INTO`` statement.
