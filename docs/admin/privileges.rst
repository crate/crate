.. highlight:: psql
.. _administration-privileges:

==========
Privileges
==========

To execute statements, a user needs to have the required privileges.

.. rubric:: Table of contents

.. contents::
   :local:


.. _privileges-intro:

Introduction
============

CrateDB has a superuser (``crate``) which has the privilege to do anything. The
privileges of other users and roles have to be managed using the ``GRANT``,
``DENY`` or ``REVOKE`` statements.

The privileges that can be granted, denied or revoked are:

- ``DQL``
- ``DML``
- ``DDL``
- ``AL``

Skip to :ref:`privilege_types` for details.

.. _privileges-classes:

Privilege Classes
=================

The privileges can be granted on different classes:

- ``CLUSTER``
- ``SCHEMA``
- ``TABLE`` and ``VIEW``

Skip to :ref:`hierarchical_privileges_inheritance` for details.

A user with ``AL`` on level ``CLUSTER`` can grant privileges they have
themselves to other users or roles as well.


.. _privilege_types:

Privilege types
===============

``DQL``
.......

Granting ``Data Query Language (DQL)`` privilege to a user or role, indicates
that this user/role is allowed to execute ``SELECT``, ``SHOW``, ``REFRESH`` and
``COPY TO`` statements, as well as using the available
:ref:`user-defined functions <user-defined-functions>`, on the object for which
the privilege applies.


``DML``
.......

Granting ``Data Manipulation Language (DML)`` privilege to a user or role,
indicates that this user/role is allowed to execute ``INSERT``, ``COPY FROM``,
``UPDATE`` and ``DELETE`` statements, on the object for which the privilege
applies.

``DDL``
.......

Granting ``Data Definition Language (DDL)`` privilege to a user or role,
indicates that this user/role is allowed to execute the following statements on
objects for which the privilege applies:

- ``CREATE TABLE``
- ``DROP TABLE``
- ``CREATE VIEW``
- ``DROP VIEW``
- ``CREATE FUNCTION``
- ``DROP FUNCTION``
- ``CREATE REPOSITORY``
- ``DROP REPOSITORY``
- ``CREATE SNAPSHOT``
- ``DROP SNAPSHOT``
- ``RESTORE SNAPSHOT``
- ``ALTER TABLE``

``AL``
......

Granting ``Administration Language (AL)`` privilege to a user or role, enables
the user/role to execute the following statements:

- ``CREATE USER/ROLE``
- ``DROP USER/ROLE``
- ``SET GLOBAL``

All statements enabled via the ``AL`` privilege operate on a cluster level. So
granting this on a schema or table level will have no effect.


.. _hierarchical_privileges_inheritance:

Hierarchical inheritance of privileges
======================================

.. vale off
.. hide:

    cr> CREATE USER riley;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE USER kala;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE TABLE IF NOT EXISTS doc.accounting (
    ...   id integer primary key,
    ...   name text,
    ...   joined timestamp with time zone
    ... ) clustered by (id);
    CREATE OK, 1 row affected (... sec)

    cr> INSERT INTO doc.accounting
    ...   (id, name, joined)
    ...   VALUES (1, 'Jon', 0);
    INSERT OK, 1 row affected (... sec)

    cr> REFRESH TABLE doc.accounting
    REFRESH OK, 1 row affected (... sec)

.. vale on

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

This statement will grant ``DQL`` privilege to user ``riley`` on all the tables
and :ref:`functions <gloss-function>` of the ``doc`` schema::

    cr> GRANT DQL ON SCHEMA doc TO riley;
    GRANT OK, 1 row affected (... sec)

This statement will deny ``DQL`` privilege to user ``riley`` on the ``doc``
schema table ``doc.accounting``. However, ``riley`` will still have ``DQL``
privilege on all the other tables of the ``doc`` schema::

    cr> DENY DQL ON TABLE doc.accounting TO riley;
    DENY OK, 1 row affected (... sec)

.. NOTE::

    In CrateDB, schemas are just namespaces that are created and dropped
    implicitly. Therefore, when ``GRANT``, ``DENY`` or ``REVOKE`` are invoked
    on a schema level, CrateDB takes the schema name provided without further
    validation.

    Privileges can be managed on all schemas and tables of the cluster,
    except the ``information_schema``.

Views are on the same hierarchy with tables, i.e. a privilege on a view
is gained through a ``GRANT`` on either the view itself, the schema the view
belongs to, or a cluster-wide privilege. Privileges on relations which are
referenced in the view do not grant any privileges on the view itself. On the
contrary, even if the user/role does not have any privileges on a view's
referenced relations but on the view itself, the user/role can still access the
relations through the view. For example::

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

.. SEEALSO::

    :ref:`Views: Privileges <views-privileges>`


Behavior of ``GRANT``, ``DENY`` and ``REVOKE``
==============================================

.. NOTE::

    You can only grant, deny, or revoke privileges for an existing user or role.
    You must first :ref:`create a user/role <administration_user_management>`
    and then configure privileges.

``GRANT``
.........

.. hide:

    cr> CREATE USER wolfgang;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE USER will;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE TABLE IF NOT EXISTS doc.books (
    ...   first_column integer primary key,
    ...   second_column text);
    CREATE OK, 1 row affected (... sec)

To grant a privilege to an existing user or role on the whole cluster,
we use the :ref:`ref-grant` SQL statement, for example::

    cr> GRANT DML TO wolfgang;
    GRANT OK, 1 row affected (... sec)

``DQL`` privilege can be granted on the ``sys`` schema to user ``wolfgang``,
like this::

    cr> GRANT DQL ON SCHEMA sys TO wolfgang;
    GRANT OK, 1 row affected (... sec)

The following statement will grant all privileges on table doc.books to user
``wolfgang``::

    cr> GRANT ALL PRIVILEGES ON TABLE doc.books TO wolfgang;
    GRANT OK, 4 rows affected (... sec)

Using "ALL PRIVILEGES" is a shortcut to grant all the :ref:`currently grantable
privileges <privilege_types>` to a user or role.

.. NOTE::

    If no schema is specified in the table ``ident``, the table will be
    looked up in the current schema.

If a user/role with the name specified in the SQL statement does not exist the
statement returns an error::

    cr> GRANT DQL TO layla;
    RoleUnknownException[Role 'layla' does not exist]

To grant ``ALL PRIVILEGES`` to user will on the cluster, we can use the
following syntax::

    cr> GRANT ALL PRIVILEGES TO will;
    GRANT OK, 4 rows affected (... sec)

Using ``ALL PRIVILEGES`` is a shortcut to grant all the currently grantable
privileges to a user or role, namely ``DQL``, ``DML`` and ``DDL``.

Privileges can be granted to multiple users/roles in the same statement, like
so::

    cr> GRANT DDL ON TABLE doc.books TO wolfgang, will;
    GRANT OK, 1 row affected (... sec)

``DENY``
........

To deny a privilege to an existing user or role on the whole cluster, use the
:ref:`ref-deny` SQL statement, for example::

    cr> DENY DDL TO will;
    DENY OK, 1 row affected (... sec)

``DQL`` privilege can be denied on the ``sys`` schema to user ``wolfgang`` like
this::

    cr> DENY DQL ON SCHEMA sys TO wolfgang;
    DENY OK, 1 row affected (... sec)

The following statement will deny ``DQL`` privilege on table doc.books to user
``wolfgang``::

    cr> DENY DQL ON TABLE doc.books TO wolfgang;
    DENY OK, 1 row affected (... sec)

``DENY ALL`` or ``DENY ALL PRIVILEGES`` will deny all privileges to a user or
role, on the cluster it can be used like this::

    cr> DENY ALL TO will;
    DENY OK, 3 rows affected (... sec)

``REVOKE``
..........

To revoke a privilege that was previously granted or denied to a user or role
use the :ref:`ref-revoke` SQL statement, for example the ``DQL`` privilege that
was previously denied to user ``wolfgang`` on the ``sys`` schema, can be revoked
like this::

    cr> REVOKE DQL ON SCHEMA sys FROM wolfgang;
    REVOKE OK, 1 row affected (... sec)

The privileges that were granted and denied to user ``wolfgang`` on doc.books
can be revoked like this::

    cr> REVOKE ALL ON TABLE doc.books FROM wolfgang;
    REVOKE OK, 4 rows affected (... sec)

The privileges that were granted to user ``will`` on the cluster can be revoked
like this::

    cr> REVOKE ALL FROM will;
    REVOKE OK, 4 rows affected (... sec)

.. NOTE::

    The ``REVOKE`` statement can remove only privileges that have been granted
    or denied through the ``GRANT`` or ``DENY`` statements. If the privilege
    on a specific object was not explicitly granted, the ``REVOKE`` statement
    has no effect. The effect of the ``REVOKE`` statement will be reflected
    in the row count.

.. NOTE::

    When a privilege is revoked from a user or role, it can still be active for
    that user/role, if the user/role :ref:`inherits <roles_inheritance>` it,
    from another role.

List privileges
===============

CrateDB exposes the privileges of users and roles of the database through the
:ref:`sys.privileges <sys-privileges>` system table.

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


.. _roles_inheritance:

Roles inheritance
=================

.. hide:

    cr> CREATE USER john;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_a;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_b;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_c;
    CREATE OK, 1 row affected (... sec)


Introduction
............

You can grant, or revoke roles for an existing user or role. This allows to
group granted or denied privileges and inherit them to other users or roles.

You must first :ref:`create usesr and roles <administration_user_management>`
and then grant roles to other roles or users. You can configure the privileges
of each role before or after granting roles to other roles or users.

.. NOTE::

    Roles can be granted to other roles or users, but users (roles which can
    also login to the database) cannot be granted to other roles or users.

.. NOTE::

    Superuser ``crate`` cannot be granted to other users or roles, and roles
    cannot be granted to it.

Inheritance
...........

The inheritance can span multiple levels, so you can have ``role_a`` which is
granted to ``role_b``, which in turn is granted to ``role_c``, and so on. Each
role can be granted to multiple other roles and each role or user can be granted
multiple other roles. Cycles cannot be created, for example::

    cr> GRANT role_a TO role_b;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT role_b TO role_c;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT role_c TO role_a;
    SQLParseException[Cannot grant role role_c to role_a, role_a is a parent role of role_c and a cycle will be created]


.. hide:

    cr> REVOKE role_b FROM role_c;
    REVOKE OK, 1 row affected (... sec)
    cr> REVOKE role_a FROM role_b;
    REVOKE OK, 1 row affected (... sec)


Privileges resolution
.....................

When a user executes a statement, the privileges mechanism will check first if
the user has been granted the required privileges, if not, it will check if the
roles which this user has been granted have those privileges and if not, it will
continue checking the roles granted to those parent roles of the user and so on.
For example::

    cr> GRANT role_a TO role_b;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT role_b TO role_c;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT DQL ON TABLE sys.users TO role_a;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT role_c TO john;
    GRANT OK, 1 row affected (... sec)

User ``john`` is able to query ``sys.users``, as even though he lacks ``DQL``
privilege on the table, he is granted ``role_c`` which in turn is granted
``role_b`` which is granted ``role_a``, and ``role`` has the ``DQL`` privilege
on ``sys.users``.


.. hide:

    cr> REVOKE role_c FROM john;
    REVOKE OK, 1 row affected (... sec)
    cr> REVOKE role_b FROM role_c;
    REVOKE OK, 1 row affected (... sec)
    cr> REVOKE role_a FROM role_b;
    REVOKE OK, 1 row affected (... sec)
    cr> REVOKE DQL ON TABLE sys.users FROM role_a;
    REVOKE OK, 1 row affected (... sec)

Keep in mind that ``DENY`` has precedence over ``GRANT``. If a role has been
both granted and denied a privilege (directly or through role inheritance), then
``DENY`` will take effect. For example, ``GRANT`` is inherited from a role
and ``DENY`` directly set on the user::

    cr> GRANT DQL ON TABLE sys.users TO role_a;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT role_a TO john
    GRANT OK, 1 row affected (... sec)

::

    cr> DENY DQL ON TABLE sys.users TO john
    DENY OK, 1 row affected (... sec)

User ``john`` cannot query ``sys.users``.


.. hide:

    cr> REVOKE role_a FROM john;
    REVOKE OK, 1 row affected (... sec)
    cr> REVOKE DQL ON TABLE sys.users FROM role_a;
    REVOKE OK, 1 row affected (... sec)

Another example with ``DENY`` in effect, inherited from a role::

    cr> GRANT DQL ON TABLE sys.users TO role_a;
    GRANT OK, 1 row affected (... sec)

::

    cr> DENY DQL ON TABLE sys.users TO role_b;
    DENY OK, 1 row affected (... sec)

::

    cr> GRANT role_a, role_b TO john;
    GRANT OK, 2 rows affected (... sec)

User ``john`` cannot query ``sys.users``.


.. hide:

    cr> DROP USER john;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_c;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_b;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_a;
    DROP OK, 1 row affected (... sec)

.. _granting_roles:

``GRANT``
.........

.. hide:

    cr> CREATE ROLE role_dql;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_all_on_books;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE USER wolfgang;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE USER will;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE USER layla;
    CREATE OK, 1 row affected (... sec)

    cr> CREATE TABLE IF NOT EXISTS doc.books (
    ...   first_column integer primary key,
    ...   second_column text);
    CREATE OK, 1 row affected (... sec)

To grant an existing role to an existing user or role on the whole cluster,
we use the :ref:`ref-grant` SQL statement, for example::

    cr> GRANT role_dql TO wolfgang;
    GRANT OK, 1 row affected (... sec)

``DML`` privilege can be granted on the ``sys`` schema to role ``role_dml``, so,
by inheritance, to user ``wolfgang`` as well, like this::

    cr> GRANT DQL ON SCHEMA sys TO role_dql;
    GRANT OK, 1 row affected (... sec)

The following statements will grant all privileges on table doc.books to role
``role_all_on_books``, and by inheritance to user ``wolfgang`` as well::

    cr> GRANT role_all_on_books TO wolfgang;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT ALL PRIVILEGES ON TABLE doc.books TO role_all_on_books;
    GRANT OK, 4 rows affected (... sec)


If a role with the name specified in the SQL statement does not exist the
statement returns an error::

    cr> GRANT DDL TO role_ddl;
    RoleUnknownException[Role 'role_ddl' does not exist]

Multiple roles can be granted to multiple users/roles in the same statement,
like so::

    cr> GRANT role_dql, role_all_on_books TO layla, will;
    GRANT OK, 4 rows affected (... sec)

Notice that `4 rows` affected is returned, as in total there are 2 users,
``will`` and ``layla`` and each of them is granted two roles: ``role_dql`` and
``role_all_on_books``.



``REVOKE``
..........

To revoke a role that was previously granted to a user or role use the
:ref:`ref-revoke` SQL statement. For example role ``role_dql`` which was
previously granted to users ``wolfgang``,``layla`` and ``will``, can be revoked
like this::

    cr> REVOKE role_dql FROM wolfgang, layla, will;
    REVOKE OK, 3 rows affected (... sec)

If a privilege is revoked from a role which is granted to other roles or users,
the privilege is automatically revoked also for those roles and users, for
example if we revoke privileges on table ``doc.books`` from
``role_all_on_books``::

    cr> REVOKE ALL PRIVILEGES ON TABLE doc.books FROM role_all_on_books;
    REVOKE OK, 4 rows affected (... sec)

user ``wolfgang``, who is granted the role ``role_all_on_books``, also looses
those privileges.

.. hide:

    cr> CREATE ROLE role_dml;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE john;
    CREATE OK, 1 row affected (... sec)

If a user is granted the same privilege by inheriting two different roles, when
revoking one of the roles, the user still keeps the privilege. For example if
user ``john`` gets granted ```role_dql`` and ``role_dml``::

    cr> GRANT DQL TO role_dql;
    GRANT OK, 1 row affected (... sec)

::

    cr> GRANT DQL, DML TO role_dml;
    GRANT OK, 2 rows affected (... sec)

::

    cr> GRANT role_dql, role_dml TO john;
    GRANT OK, 2 rows affected (... sec)

and then we revoke ``role_dql`` from ``john``::

    cr> REVOKE role_dql FROM john;
    REVOKE OK, 1 row affected (... sec)

``john`` still has ``DQL`` privilege since it inherits it from ``role_dml``
which is still granted to him.


.. hide:

    cr> DROP USER wolfgang;
    DROP OK, 1 row affected (... sec)
    cr> DROP USER will;
    DROP OK, 1 row affected (... sec)
    cr> DROP USER layla;
    DROP OK, 1 row affected (... sec)
    cr> DROP USER john;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_dql;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_dml;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_all_on_books;
    DROP OK, 1 row affected (... sec)

    cr> DROP TABLE doc.books;
    DROP OK, 1 row affected (... sec)
