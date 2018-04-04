=====
Views
=====

.. rubric:: Table of Contents

.. contents::
    :local:

Introduction: Creating Views
============================

Views are stored named queries which can be used in place of table names.
They're resolved at runtime and can be used to simplify common queries.

Views are created using the :ref:`CREATE VIEW statement <ref-create-view>`

For example, a common use case is to create a view which queries a table with a
pre-defined filter::

    cr> CREATE VIEW big_mountains AS
    ... SELECT * FROM sys.summits WHERE height > 2000;
    CREATE OK, 1 row affected (... sec)


Querying Views
==============

Once created, views can be used instead of a table in a statement::

    cr> SELECT mountain, height FROM big_mountains ORDER BY 1 LIMIT 3;
    +--------------+--------+
    | mountain     | height |
    +--------------+--------+
    | Acherkogel   |   3008 |
    | Ackerlspitze |   2329 |
    | Adamello     |   3539 |
    +--------------+--------+
    SELECT 3 rows in set (... sec)


Privileges (Enterprise only)
----------------------------

In order to be able to query data from a view, a user needs to have ``DQL``
privileges on a view.
This is the case if the user has ``DQL`` privileges on a cluster level, or on
the schema in which the view is contained.

In addition to that, the **owner**, that is the user that created the view,
needs to have ``DQL`` privileges on all relations occurring within the views
query definition.

A common use case for this is to give users access to a subset of a table
without exposing the table itself as well.

This means if the owner looses the permissions on the underlying relations, a
user that has access to the view will no longer be able to query it.


Dropping Views
==============

Views can be dropped using the :ref:`DROP VIEW statement <ref-drop-view>`::

    cr> DROP VIEW big_mountains;
    DROP OK, 1 row affected (... sec)
