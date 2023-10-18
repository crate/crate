.. highlight:: psql

.. _ref-explain:

===========
``EXPLAIN``
===========

Explain or analyze the plan for a given statement.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    EXPLAIN [ ANALYZE ] statement
    EXPLAIN [ ( option [, ...] ) ] statement

    where option is:

        ANALYZE [ boolean ]
        COSTS [ boolean ]

Description
===========

The ``EXPLAIN`` command displays the execution plan that the planner generates
for the supplied statement. The plan is returned as a nested object containing
the plan tree.

When issuing ``EXPLAIN ANALYZE`` or ``EXPLAIN (ANALYZE TRUE)`` the plan of the
statement is executed and timings of the different phases of the plan are returned.

The ``COSTS`` option is by default enabled and can be disabled by issuing
``EXPLAIN (COSTS FALSE)``. The output of the execution plan does then exclude
the costs for each logical plan.

.. NOTE::

   The content of the returned plan tree as well as the level of detail of the
   timings of the different phases should be considered experimental and are
   subject to change in future versions. Also not all plan nodes provide
   in-depth details.


The output of ``EXPLAIN ANALYZE`` also includes a break down of the query
execution if the statement being explained involves queries which are executed
using Lucene.

.. NOTE::

   When a query involves an empty partitioned table you will see no breakdown
   concerning that table until at least one partition is created by inserting
   a record.


The output includes verbose low level information per queried shard. Since SQL
query :ref:`expressions <gloss-expression>` do not always have a direct 1:1
mapping to Lucene queries, the output may be more complex but in most cases it
should still be possible to identify the most expensive parts of a query
expression.  Some familiarity with Lucene helps in interpreting the output.

A short excerpt of a query breakdown looks like this::

    {
      "QueryName": "PointRangeQuery",
      "QueryDescription": "x:[1 TO 1]",
      "Time": 0.004096,
      "BreakDown": {
        "score": 0,
        "match_count": 0,
        "build_scorer_count": 0,
        "create_weight": 0.004095,
        "next_doc": 0,
        "match": 0,
        "score_count": 0,
        "next_doc_count": 0,
        "create_weight_count": 1,
        "build_scorer": 0,
        "advance_count": 0,
        "advance": 0
      }
    }

The time values are in milliseconds. Fields suffixed with ``_count`` indicate
how often an operation was invoked.

+-----------------------------------+-----------------------------------+
| field                             | description                       |
+===================================+===================================+
| ``create_weight``                 | A ``Weight`` object is created    |
|                                   | for a query and acts as a         |
|                                   | temporary object containing       |
|                                   | state. This metric shows how long |
|                                   | this process took.                |
+-----------------------------------+-----------------------------------+
| ``build_scorer``                  | A ``Scorer`` object is used to    |
|                                   | iterate over documents matching   |
|                                   | the query and generate scores for |
|                                   | them. Note that this includes     |
|                                   | only the time to create the       |
|                                   | scorer, not that actual time      |
|                                   | spent on the iteration.           |
+-----------------------------------+-----------------------------------+
| ``score``                         | Shows the time it takes to score  |
|                                   | a particular document via its     |
|                                   | ``Scorer``.                       |
+-----------------------------------+-----------------------------------+
| ``next_doc``                      | Shows the time it takes to        |
|                                   | determine which document is the   |
|                                   | next match.                       |
+-----------------------------------+-----------------------------------+
| ``advance``                       | A lower level version of          |
|                                   | ``next_doc``.                     |
+-----------------------------------+-----------------------------------+
| ``match``                         | Some queries use a two-phase      |
|                                   | execution, doing an               |
|                                   | ``approximation`` first, and then |
|                                   | a second more expensive phase.    |
|                                   | This metric measures the second   |
|                                   | phase.                            |
+-----------------------------------+-----------------------------------+

.. NOTE::

   Individual timings of the different phases and queries that are profiled do
   not sum up to the ``Total``. This is because there is usually additional
   initialization that is not measured. Also, certain phases do overlap during
   their execution.

Parameters
==========

:statement:
  The statement for which a plan or plan analysis should be returned.

  Currently only ``SELECT`` and ``COPY FROM`` statements are supported.
