.. highlight:: psql
.. _administration-privileges:

==========
Privileges
==========

The superuser is allowed to execute any statement without any privilege checks.

The superuser uses ``GRANT``, ``DENY`` and ``REVOKE`` statements to control
access to the resource. The privileges are either applied on the whole cluster
or on instances of objects such as schema, or table.

Currently, only ``DQL``, ``DML`` and ``DDL`` privileges can be granted.

Any statements which are not allowed with those privileges, such as ``GRANT``,
``DENY`` and ``REVOKE``, can only be issued by a superuser.

.. NOTE::

   Privileges are an :ref:`enterprise feature <enterprise_features>`.

   When the CrateDB Enterprise Edition is disabled, there will be no user
   privilege checks, and every statement will be executed without the
   validation of privileges.

.. rubric:: Table of Contents

.. contents::
   :local:

Privilege Types
===============

``DQL``
.......

Granting ``Data Query Language (DQL)`` privilege to a user, indicates that this
user is allowed to execute ``SELECT``, ``SHOW``, ``REFRESH``, ``COPY TO``,
and ``SET SESSION`` statements, as well as using the available user defined
functions, on the object for which the privilege applies.

``DML``
.......

Granting ``Data Manipulation Language (DML)`` privilege to a user, indicates
that this user is allowed to execute ``INSERT``, ``COPY FROM``, ``UPDATE``
and ``DELETE`` statements, on the object for which the privilege applies.

``DDL``
.......

Granting ``Data Definition Language (DDL)`` privilege to a user, indicates that
this user is allowed to execute ``CREATE TABLE``, ``DROP TABLE``,
``CREATE VIEW``, ``DROP VIEW``,``CREATE FUNCTION``, ``DROP FUNCTION``,
``CREATE REPOSITORY``, ``DROP REPOSITORY``, ``CREATE SNAPSHOT``,
``DROP SNAPSHOT``, ``RESTORE SNAPSHOT``, and ``ALTER`` statements, on the
object for which the privilege applies.

.. _hierarchical_privileges_inheritance:

Hierarchical Inheritance of Privileges
======================================
.. hide:

    cr> CREATE user riley;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE user kala;
    CREATE OK, 1 row affected (... sec)

    cr> create table if not exists doc.accounting (
    ...   id integer primary key,
    ...   name string,
    ...   joined timestamp
    ... ) clustered by (id);
    CREATE OK, 1 row affected (... sec)

    cr> INSERT INTO doc.accounting
    ...   (id, name, joined)
    ...   VALUES (1, 'Jon', 0);
    INSERT OK, 1 row affected (... sec)

    cr> REFRESH table doc.accounting
    REFRESH OK, 1 row affected (... sec)

Privileges can be managed on three different levels, namely: ``CLUSTER``,
``SCHEMA``, and ``TABLE``/``VIEW``.

When a privilege is assigned on a certain level, the privilege will propagate
down the hierarchy. Privileges defined on a lower level will always override
those from a higher level:

.. code-block:: none

    cluster
      ||
    schema
     /  \
  table view

This statement will grant ``DQL`` privilege to user riley on all the tables
and functions of the ``doc`` schema::

    cr> GRANT DQL ON SCHEMA doc TO riley;
    GRANT OK, 1 row affected (... sec)

This statement will deny ``DQL`` privilege to user riley on the ``doc`` schema
table ``doc.accounting``. However, user riley, will still have ``DQL``
privilege on all the other tables of the ``doc`` schema::

    cr> DENY DQL ON TABLE doc.accounting TO riley;
    DENY OK, 1 row affected (... sec)

.. NOTE::

    In CrateDB, schemas are just namespaces that are created and dropped
    implicitly. Therefore, when ``GRANT``, ``DENY`` or ```REVOKE`` are invoked
    on a schema level, CrateDB takes the schema name provided without further
    validation.

    Privileges can be managed on all schemas and tables of the cluster,
    except the ``information_schema``.

Views are on the same hierarchy with tables, i.e. a privilege on a view
is gained through a ``GRANT`` on either the view itself, the schema the view
belongs to, or a cluster-wide privilege. Privileges on relations which are
referenced in the view do not grant any privileges on the view itself. On the
contrary, even if the user does not have any privileges on a view's referenced
relations but on the view itself, the user can still access the relations
through the view. For example::

    cr> CREATE VIEW first_customer as SELECT * from doc.accounting ORDER BY id LIMIT 1
    CREATE OK, 1 row affected (... sec)

Previously we had issued a ``DENY`` for user ``riley`` on ``doc.accounting``
but we can still access it through the view because we have access to it
through the ``doc`` schema::

    cr> SELECT id from first_customer;
    +----+
    | id |
    +----+
    |  1 |
    +----+
    SELECT 1 row in set (... sec)

For more information regarding views, please see the
:ref:`views page <views_enterprise>`.

Behavior of ``GRANT``, ``DENY`` and ``REVOKE``
==============================================

.. CAUTION::

    Stale permissions might be introduced if ``DDL`` statements were invoked
    while the Enterprise Edition is temporarily disabled. To allow clients
    to remove such stale permissions even, if the table does not exist anymore,
    the ``REVOKE`` statement does not perform any validation checks on the
    table ident.

``GRANT``
.........

.. hide:

    cr> CREATE user wolfgang;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE user will;
    CREATE OK, 1 row affected (... sec)

    cr> create table if not exists doc.books (
    ...   first_column integer primary key,
    ...   second_column string,
    ...   third_column timestamp,
    ...   fourth_column object(strict) as (
    ...     key string,
    ...     value string
    ...   )
    ... ) clustered by (first_column) into 5 shards;
    CREATE OK, 1 row affected (... sec)

To grant a privilege to an existing user on the whole cluster,
we use the :ref:`ref-grant` SQL statement, for example::

    cr> GRANT DML TO wolfgang;
    GRANT OK, 1 row affected (... sec)

``DQL`` privilege can be granted on the sys schema to user wolfgang,
like this::

    cr> GRANT DQL ON SCHEMA sys TO wolfgang;
    GRANT OK, 1 row affected (... sec)

The following statement will grant all privileges on table doc.books to user
wolfgang::

    cr> GRANT ALL PRIVILEGES ON TABLE doc.books TO wolfgang;
    GRANT OK, 3 rows affected (... sec)

Using "ALL PRIVILEGES" is a shortcut to grant all the currently grantable
privileges to a user, namely ``DQL``, ``DML`` and ``DDL``.

.. NOTE::

    If no schema is specified in the table ``ident``, the table will be
    looked up in the current schema.

If a user with the username specified in the SQL statement does not exist the
statement returns an error::

    cr> GRANT DQL TO layla;
    SQLActionException[UserUnknownException: User 'layla' does not exist]

To grant ``ALL PRIVILEGES`` to user will on the cluster, we can use the
following syntax::

    cr> GRANT ALL PRIVILEGES TO will;
    GRANT OK, 3 rows affected (... sec)

Using ``ALL PRIVILEGES`` is a shortcut to grant all the currently grantable
privileges to a user, namely ``DQL``, ``DML`` and ``DDL``.

Privileges can be granted to multiple users in the same statement, like so::

    cr> GRANT DDL ON TABLE doc.books TO wolfgang, will;
    GRANT OK, 1 row affected (... sec)

``DENY``
........

To deny a privilege to an existing user on the whole cluster, use the
:ref:`ref-deny` SQL statement, for example::

    cr> DENY DDL TO will;
    DENY OK, 1 row affected (... sec)

``DQL`` privilege can be denied on the sys schema to user wolfgang,
like this::

    cr> DENY DQL ON SCHEMA sys TO wolfgang;
    DENY OK, 1 row affected (... sec)

The following statement will deny ``DQL`` privilege on table doc.books to user
wolfgang::

    cr> DENY DQL ON TABLE doc.books TO wolfgang;
    DENY OK, 1 row affected (... sec)

``DENY ALL`` or ``DENY ALL PRIVILEGES`` will deny all privileges to a user,
on the cluster it can be used like this::

    cr> DENY ALL TO will;
    DENY OK, 2 rows affected (... sec)

``REVOKE``
..........

To revoke a privilege that was previously granted or denied to a user use the
:ref:`ref-revoke` SQL statement, for example the ``DQL`` privilege that was
previously denied to user wolfgang on the *sys* schema, can be revoked like
this::

    cr> REVOKE DQL ON SCHEMA sys FROM wolfgang;
    REVOKE OK, 1 row affected (... sec)

The privileges that were granted and denied to user wolfgang on doc.books
can be revoked like this::

    cr> REVOKE ALL ON TABLE doc.books FROM wolfgang;
    REVOKE OK, 3 rows affected (... sec)

The privileges that were granted to user will on the cluster can be revoked
like this::

    cr> REVOKE ALL FROM will;
    REVOKE OK, 3 rows affected (... sec)

.. NOTE::

    The ``REVOKE`` statement can remove only privileges that have been granted
    or denied through the ``GRANT`` or ``DENY`` statements. If the privilege
    on a specific object was not explicitly granted, the ``REVOKE`` statement
    has no effect. The effect of the ``REVOKE`` statement will be reflected
    in the row count.

List Privileges
===============

CrateDB exposes privileges ``sys.privileges`` system table.

By querying the ``sys.privileges`` table you can get all
information regarding the existing privileges. E.g.::

    cr> SELECT * FROM sys.privileges order by grantee, class, ident;
    +---------+----------+---------+----------------+-------+------+
    | class   | grantee  | grantor | ident          | state | type |
    +---------+----------+---------+----------------+-------+------+
    | SCHEMA  | riley    | crate   | doc            | GRANT | DQL  |
    | TABLE   | riley    | crate   | doc.accounting | DENY  | DQL  |
    | TABLE   | will     | crate   | doc.books      | GRANT | DDL  |
    | CLUSTER | wolfgang | crate   | NULL           | GRANT | DML  |
    +---------+----------+---------+----------------+-------+------+
    SELECT 4 rows in set (... sec)

The column ``grantor`` shows the user who granted or denied the privilege,
the column ``grantee`` shows the user for whom the privilege was granted
or denied. The column ``class`` identifies on which type of context the
privilege applies. ``ident`` stands for the ident of the object that the
privilege is set on and finally ``type`` stands for the type of privileges that
was granted or denied.


.. _Enterprise Edition: https://crate.io/enterprise/

.. hide:

    cr> DROP user riley;
    DROP OK, 1 row affected (... sec)

    cr> DROP user kala;
    DROP OK, 1 row affected (... sec)

    cr> DROP TABLE IF EXISTS doc.accounting;
    DROP OK, 1 row affected (... sec)

    cr> DROP user wolfgang;
    DROP OK, 1 row affected (... sec)

    cr> DROP user will;
    DROP OK, 1 row affected (... sec)

    cr> DROP TABLE IF EXISTS doc.books;
    DROP OK, 1 row affected (... sec)

    cr> DROP VIEW first_customer;
    DROP OK, 1 row affected (... sec)
