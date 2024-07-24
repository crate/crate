.. highlight:: psql
.. _ref-alter-role:

==============
``ALTER ROLE``
==============

Alter an existing database user or role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    ALTER ROLE name
        SET ( parameter = value [, ...] )
      | RESET [parameter | ALL]


Description
===========

``ALTER ROLE SET`` applies a change to an existing database user or role. Only
existing superusers or the user itself have the privilege to alter an existing
database user.

``ALTER ROLE RESET`` resets modifiable
:ref:`session settings <conf-session>` to their default value.


Arguments
=========

``name``
--------

The name by which the user or role is identified inside the database.

``SET``
-------

Changes a user parameter to a different value. The following ``parameter``
are supported to alter an existing user account:

:password:
  The password as cleartext entered as string literal.

  ``NULL`` removes the password from the user.

.. CAUTION::

    Passwords cannot be set for the ``crate`` superuser.

    For security reasons it is recommended to authenticate as ``crate`` using a
    client certificate.

:jwt:
  JWT properties map (``iss``, ``username`` and ``aud``) entered as a string literal.
  e.g.::

     ALTER USER john WITH (jwt = {"iss" = 'new_issuer', "username" = 'john.smith', "aud" = 'new_aud'})

  New JWT properties must not coincide with JWT properties of another user.

  ``NULL`` removes the JWT properties from the user.

.. NOTE::

   ``jwt = {...}`` overrides existing jwt properties. If an optional property
   is not provided, an existing value will be discarded.

.. NOTE::

   Passwords and JWT properties can be changed only for existing database
   users, but not to roles.

:session settings:

  Any of the modifiable :ref:`session settings <conf-session>`. The value set
  is used for the user when logins to the database, instead of the default
  value, thus, there is no need to use ``SET`` statements to modify the setting
  value on its user session.


.. NOTE::

    The session settings can only be set to a user and not on a role and
    are therefore are not inherited to other users.

    Changes to session settings are only applied to new sessions opened by the
    user.


``RESET``
---------

Resets modifiable :ref:`session settings <conf-session>` to their default value.

:parameter:

  Any of the modifiable :ref:`session settings <conf-session>`.

:ALL:

  Resets all the  modifiable :ref:`session settings <conf-session>` of the user
  to their default values.
