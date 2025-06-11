.. _ddl-views:

=====
Views
=====


.. _views-create:

Creating views
==============

Views are stored named queries which can be used in place of table names.
They're resolved at runtime and can be used to simplify common queries.

Views are created using the :ref:`CREATE VIEW statement <sql-create-view>`

For example, a common use case is to create a view which queries a table with a
pre-defined filter::

    cr> CREATE VIEW big_mountains AS
    ... SELECT * FROM sys.summits WHERE height > 2000;
    CREATE OK, 1 row affected (... sec)


.. _views-query:

Querying views
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


.. _views-privileges:

Privileges
----------

In order to be able to query data from a view, a user needs to have ``DQL``
privileges on a view. DQL privileges can be granted on a cluster level, on the
schema in which the view is contained, or the view itself. Privileges on
relations accessed by the view are not necessary.

However, it is required, at all times, that the *owner* (the user who created
the view), has ``DQL`` privileges on all relations occurring within the view's
query definition.

A common use case for this is to give users access to a subset of a table
without exposing the table itself as well. If the owner ``DQL`` permissions
on the underlying relations, a user who has access to the view will no longer
be able to query it.

.. SEEALSO::

    :ref:`Administration: Privileges <administration-privileges>`


.. _views-drop:

Dropping views
==============

Views can be dropped using the :ref:`DROP VIEW statement <sql-drop-view>`::

    cr> DROP VIEW big_mountains;
    DROP OK, 1 row affected (... sec)
