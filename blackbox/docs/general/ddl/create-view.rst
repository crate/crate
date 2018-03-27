==============
Creating Views
==============

.. rubric:: Table of Contents

.. contents::
    :local:

Introduction
============

Views are stored named queries which can be used in place of table names.
They're resolved at runtime and can be used to simplify common queries.

Views are created using the :ref:`CREATE VIEW statement <ref-create-view>`

For example, a common use case is to create a view which queries a table with a
pre-defined filter::

    cr> CREATE VIEW big_mountains AS
    ... SELECT * FROM sys.summits WHERE height > 2000;
    CREATE OK, 1 row affected (... sec)

Once created, this view can then be used instead of a table in a statement::

    cr> SELECT mountain, height FROM big_mountains ORDER BY 1 LIMIT 3;
    +--------------+--------+
    | mountain     | height |
    +--------------+--------+
    | Acherkogel   |   3008 |
    | Ackerlspitze |   2329 |
    | Adamello     |   3539 |
    +--------------+--------+
    SELECT 3 rows in set (... sec)


.. hide: DROP VIEW::

    cr> DROP VIEW big_mountains;
    DROP OK, 1 row affected (... sec)
