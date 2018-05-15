.. highlight:: psql

===========
``EXPLAIN``
===========

Explain and analyze the plan for a given statement.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    EXPLAIN [ ANALYZE ] statement

Description
===========

The ``EXPLAIN`` command displays the execution plan that the planner generates
for the supplied statement. The plan is returned as a nested object containing
the plan tree.

When issuing ``EXPLAIN ANALYZE`` the plan of the statement is also executed. In
this case, timings of the different phases of the plan are returned instead of
the plan itself.

.. NOTE::

   The content of the returned plan tree as well as the level of detail of the
   timings of the different phases should be considered experimental and are
   subject to change in future versions. Also not all plan nodes provide
   in-depth details.


Parameters
==========

:statement:
  The statement for which a plan or plan analysis should be returned.

  Currently only ``SELECT`` and ``COPY FROM`` statements are supported.
