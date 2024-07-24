.. _administration_user_management:

==========================
Users and roles management
==========================

Users and roles account information is stored in the cluster metadata of CrateDB
and supports the following statements to create, alter and drop users and roles:

* `CREATE USER`_
* `CREATE ROLE`_
* `ALTER USER`_ or `ALTER ROLE`_
* `DROP USER`_ or `DROP ROLE`_

These statements are database management statements that can be invoked by
superusers that already exist in the CrateDB cluster. The `CREATE USER`_,
`CREATE ROLE`_, `DROP USER`_ and `DROP ROLE`_ statements can also be invoked by
users with the ``AL`` privilege. `ALTER USER`_ or `ALTER ROLE`_ can be invoked
by users to change their own password, without requiring any privilege.

When CrateDB is started, the cluster contains one predefined superuser. This
user is called ``crate``. It is not possible to create any other superusers.

The definition of all users and roles, including hashes of their passwords,
together with their :ref:`privileges <administration-privileges>` is backed up
together with the cluster's metadata when a snapshot is created, and it is
restored when using the ``ALL``, ``METADATA``, or ``USERMANAGEMENT`` keywords
with the:ref:`sql-restore-snapshot` command.

.. rubric:: Table of contents

.. contents::
   :local:

``ROLES``
---------

Roles are entities that are **not** allowed to login, but can be assigned
privileges and they can be granted to other roles, thus creating a role
hierarchy, or directly to users. For example, a role ``myschema_dql_role`` can
be granted with ``DQL`` privileges on schema ``myschema`` and afterwards the
role can be :ref:`granted <granting_roles>` to a user, which will automatically
:ref:`inherit <roles_inheritance>` those privileges from the
``myschema_dql_role``. A role ``myschema_dml_role`` can be granted with ``DML``
privileges on schema ``myschema`` and can also be granted the role
``myschema_dql_role``, thus gaining also ``DQL`` privileges. When
``myschema_dml_role`` is granted to a user, this user will automatically have
both ``DQL`` and ``DML`` privileges on ``myschema``.


``CREATE ROLE``
===============

To create a new role for the CrateDB database cluster use the
:ref:`ref-create-role` SQL statement::

    cr> CREATE ROLE role_a;
    CREATE OK, 1 row affected (... sec)

.. TIP::

    Newly created roles do not have any privileges. After creating a role, you
    should :ref:`configure user privileges <administration-privileges>`.

    For example, to grant all privileges to the ``role_a`` user, run::

        cr> GRANT ALL PRIVILEGES TO role_a;
        GRANT OK, 4 rows affected (... sec)

.. hide:

    cr> REVOKE ALL PRIVILEGES FROM role_a;
    REVOKE OK, 4 rows affected (... sec)


The name parameter of the statement follows the principles of an identifier
which means that it must be double-quoted if it contains special characters
(e.g. whitespace) or if the case needs to be maintained::

    cr> CREATE ROLE "Custom Role";
    CREATE OK, 1 row affected (... sec)

If a role or user with the name specified in the SQL statement already exists the
statement returns an error::

    cr> CREATE ROLE "Custom Role";
    RoleAlreadyExistsException[Role 'Custom Role' already exists]


.. hide:

    cr> DROP ROLE "Custom Role";
    DROP OK, 1 row affected (... sec)


``ALTER ROLE``
==============

:ref:`ref-alter-role` and :ref:`ref-alter-user` SQL statements are not supported
for roles, only for users.


``DROP ROLE``
=============

.. hide:

    cr> CREATE ROLE role_c;
    CREATE OK, 1 row affected (... sec)

.. hide:

    cr> CREATE ROLE role_d;
    CREATE OK, 1 row affected (... sec)

To remove an existing role from the CrateDB database cluster use the
:ref:`ref-drop-role` or :ref:`ref-drop-user` SQL statement::

    cr> DROP ROLE role_c;
    DROP OK, 1 row affected (... sec)

::

    cr> DROP USER role_d;
    DROP OK, 1 row affected (... sec)

If a role with the name specified in the SQL statement does not exist, the
statement returns an error::

    cr> DROP ROLE role_d;
    RoleUnknownException[Role 'role_d' does not exist]


List roles
==========

.. hide:

    cr> CREATE ROLE role_b;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE ROLE role_c;
    CREATE OK, 1 row affected (... sec)
    cr> GRANT role_c TO role_b;
    GRANT OK, 1 row affected (... sec)

CrateDB exposes database roles via the read-only :ref:`sys-roles` system table.
The ``sys.roles`` table shows all roles in the cluster which can be used to
group privileges.

To list all existing roles query the table::

    cr> SELECT name, granted_roles FROM sys.roles order by name;
    +--------+------------------------------------------+
    | name   | granted_roles                            |
    +--------+------------------------------------------+
    | role_a | []                                       |
    | role_b | [{"grantor": "crate", "role": "role_c"}] |
    | role_c | []                                       |
    +--------+------------------------------------------+
    SELECT 3 rows in set (... sec)


``USERS``
---------

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


.. hide:

    cr> DROP USER "Custom User";
    DROP OK, 1 row affected (... sec)

.. _administration_user_management_alter_user:

``ALTER USER``
==============

To alter the password for an existing user from the CrateDB database cluster use
the :ref:`ref-alter-role` or :ref:`ref-alter-user` SQL statements::

    cr> ALTER USER user_a SET (password = 'pass');
    ALTER OK, 1 row affected (... sec)

The password can be reset (cleared) if specified as ``NULL``::

    cr> ALTER USER user_a SET (password = NULL);
    ALTER OK, 1 row affected (... sec)

.. NOTE::

    The built-in superuser ``crate`` has no password and it is not possible to
    set a new password for this user.

To add or alter :ref:`session settings <conf-session>` use the following SQL
statement::

    cr> ALTER USER user_b SET (search_path = 'myschema', statement_timeout = '10m');
    ALTER OK, 1 row affected (... sec)

To reset a :ref:`session setting <conf-session>` to its default value use the
following SQL statement::

    cr> ALTER USER user_b RESET statement_timeout;
    ALTER OK, 1 row affected (... sec)

.. hide:

   cr> ALTER USER user_a SET (search_path = 'new_schema', statement_timeout = '1h');
    ALTER OK, 1 row affected (... sec)

To reset all modified :ref:`session setting <conf-session>` for a user to their
default values, use the following SQL statement::

    cr> ALTER USER user_a RESET ALL;
    ALTER OK, 1 row affected (... sec)


``DROP USER``
=============

.. hide:

    cr> CREATE USER user_c;
    CREATE OK, 1 row affected (... sec)
    cr> CREATE USER user_d;
    CREATE OK, 1 row affected (... sec)

To remove an existing user from the CrateDB database cluster use the
:ref:`ref-drop-role` or :ref:`ref-drop-user` SQL statements::

    cr> DROP USER user_c;
    DROP OK, 1 row affected (... sec)

::

    cr> DROP ROLE user_d;
    DROP OK, 1 row affected (... sec)

If a user with the username specified in the SQL statement does not exist the
statement returns an error::

    cr> DROP USER user_d;
    RoleUnknownException[Role 'user_d' does not exist]

.. NOTE::

    It is not possible to drop the built-in superuser ``crate``.

List users
==========

.. hide:

     cr> GRANT role_a, role_b TO user_a;
     GRANT OK, 2 rows affected (... sec)

CrateDB exposes database users via the read-only :ref:`sys-users` system table.
The ``sys.users`` table shows all users in the cluster which can be used for
authentication. The initial superuser ``crate`` which is available for all
CrateDB clusters is also part of that list.

To list all existing users query the table::

    cr> SELECT name, granted_roles, password, session_settings, superuser FROM sys.users order by name;
    +--------+----------------------------------------------------------------------------------+----------+-----------------------------+-----------+
    | name   | granted_roles                                                                    | password | session_settings            | superuser |
    +--------+----------------------------------------------------------------------------------+----------+-----------------------------+-----------+
    | crate  | []                                                                               | NULL     | {}                          | TRUE      |
    | user_a | [{"grantor": "crate", "role": "role_a"}, {"grantor": "crate", "role": "role_b"}] | NULL     | {}                          | FALSE     |
    | user_b | []                                                                               | ******** | {"search_path": "myschema"} | FALSE     |
    +--------+----------------------------------------------------------------------------------+----------+-----------------------------+-----------+
    SELECT 3 rows in set (... sec)


.. NOTE::

    CrateDB also supports retrieving the current connected user using the
    :ref:`system information functions <scalar-sysinfo>`: :ref:`CURRENT_USER
    <scalar-current_user>`, :ref:`USER <scalar-user>` and :ref:`SESSION_USER
    <scalar-session_user>`.

.. vale off
.. Drop Users & Roles
.. hide:

    cr> DROP USER user_a;
    DROP OK, 1 row affected (... sec)
    cr> DROP USER user_b;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_a;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_b;
    DROP OK, 1 row affected (... sec)
    cr> DROP ROLE role_c;
    DROP OK, 1 row affected (... sec)
