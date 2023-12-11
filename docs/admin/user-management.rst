.. _administration_user_management:

===============
User management
===============

User account information is stored in the cluster metadata of CrateDB and
supports the following statements to create, alter and drop users:

    * `CREATE USER`_
    * `ALTER USER`_
    * `DROP USER`_

These statements are database management statements that can be invoked by
superusers that already exist in the CrateDB cluster. The `CREATE USER`_ and
`DROP USER`_ statements can also be invoked by users with the ``AL`` privilege.

When CrateDB is started, the cluster contains one predefined superuser. This
user is called ``crate``. It is not possible to create any other superusers

The definition of all users and roles, including hashes of their passwords,
together with their privileges is backed up together with the cluster's metadata
when a snapshot is created, and it is restored when using the ``ALL``,
``METADATA``, or ``USERMANAGEMENT`` keywords with the
:ref:`sql-restore-snapshot` command.

.. rubric:: Table of contents

.. contents::
   :local:

``CREATE USER``
===============

To create a new user for the CrateDB database cluster use the
:ref:`ref-create-user` SQL statement::

    cr> CREATE USER user_a;
    CREATE OK, 1 row affected (... sec)

.. TIP::

    Newly created users do not have any privileges. After creating a user, you
    should :ref:`configure user privileges <administration-privileges>`.

    For example, to grant all privileges to the ``user_a`` user, run::

        cr> GRANT ALL PRIVILEGES TO user_a;
        GRANT OK, 4 rows affected (... sec)

.. hide:

    cr> REVOKE ALL PRIVILEGES FROM user_a;
    REVOKE OK, 4 rows affected (... sec)

It can be used to connect to the database cluster using available authentication
methods. You can specify the user's password in the ``WITH`` clause of the
``CREATE`` statement. This is required if you want to use the
:ref:`auth_password`::

    cr> CREATE USER user_b WITH (password = 'a_secret_password');
    CREATE OK, 1 row affected (... sec)

The username parameter of the statement follows the principles of an identifier
which means that it must be double-quoted if it contains special characters
(e.g. whitespace) or if the case needs to be maintained::

    cr> CREATE USER "Custom User";
    CREATE OK, 1 row affected (... sec)

If a user with the username specified in the SQL statement already exists the
statement returns an error::

    cr> CREATE USER "Custom User";
    RoleAlreadyExistsException[Role 'Custom User' already exists]


``ALTER USER``
==============

To alter the password for an existing user from the CrateDB database cluster use
the :ref:`ref-alter-user` SQL statement::

    cr> ALTER USER user_a SET (password = 'pass');
    ALTER OK, 1 row affected (... sec)

The password can be reset (cleared) if specified as ``NULL``::

    cr> ALTER USER user_a SET (password = NULL);
    ALTER OK, 1 row affected (... sec)

.. NOTE::

    The built-in superuser ``crate`` has no password and it is not possible to set a new password for this user.


``DROP USER``
=============

.. hide:

    cr> CREATE USER user_c;
    CREATE OK, 1 row affected (... sec)

To remove an existing user from the CrateDB database cluster use the
:ref:`ref-drop-user` SQL statement::

    cr> DROP USER user_c;
    DROP OK, 1 row affected (... sec)

If a user with the username specified in the SQL statement does not exist the
statement returns an error::

    cr> DROP USER user_c;
    RoleUnknownException[Role 'user_c' does not exist]

.. NOTE::

    It is not possible to drop the built-in superuser ``crate``.

List users
==========

CrateDB exposes database users via the read-only :ref:`sys-users` system table.
The ``sys.users`` table shows all users in the cluster which can be used for
authentication. The initial superuser ``crate`` which is available for all
CrateDB clusters is also part of that list.

To list all existing users query the table::

    cr> SELECT * FROM sys.users order by name;
    +-------------+----------+-----------+
    | name        | password | superuser |
    +-------------+----------+-----------+
    | Custom User |     NULL | FALSE     |
    | crate       |     NULL | TRUE      |
    | user_a      |     NULL | FALSE     |
    | user_b      | ******** | FALSE     |
    +-------------+----------+-----------+
    SELECT 4 rows in set (... sec)

The column ``name`` shows the unique name of the user, the column ``superuser``
shows whether the user has superuser privileges or not.

.. NOTE::

    CrateDB also supports retrieving the current connected user using the
    :ref:`system information functions <scalar-sysinfo>`: :ref:`CURRENT_USER
    <scalar-current_user>`, :ref:`USER <scalar-user>` and :ref:`SESSION_USER
    <scalar-session_user>`.

List roles
==========

.. hide:

    cr> CREATE ROLE role_a;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_b;
    CREATE OK, 1 row affected (... sec)

CrateDB exposes database roles via the read-only :ref:`sys-roles` system table.
The ``sys.roles`` table shows all roles in the cluster which can be used to
group privileges.

To list all existing roles query the table::

    cr> SELECT * FROM sys.roles order by name;
    +--------+
    | name   |
    +--------+
    | role_a |
    | role_b |
    +--------+
    SELECT 2 rows in set (... sec)

The column ``name`` shows the unique name of the role.

.. vale off
.. Drop Users & Roles
.. hide:

    cr> DROP USER "Custom User";
    DROP OK, 1 row affected (... sec)
    cr> DROP USER user_a;
    DROP OK, 1 row affected (... sec)
    cr> DROP USER user_b;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_a;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_b;
    DROP OK, 1 row affected (... sec)
