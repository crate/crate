.. highlight:: psql
.. _ref-create-user:

===============
``CREATE USER``
===============

Create a new database user.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

.. code-block:: psql

  CREATE USER username
  [ WITH ( user_parameter = value [, ...]) ] |
  [ [ WITH ] user_parameter [value] [ ... ] ]

Description
===========

``CREATE USER`` is a management statement to create a new database user in the
CrateDB cluster. The newly created user does not have any special privileges,
and those must be assigned afterwards, for details see the
:ref:`privileges documentation<administration-privileges>`.
The created user can be used to authenticate against CrateDB, see
:ref:`admin_hba`.

The statement allows to specify a password for this account. This is not
necessary if password authentication is disabled.

.. NOTE::

    ``USER`` is essentially the same as ``ROLE`` with the difference that a
    ``USER`` **can** login to the database and **can** also be assigned a
    password, but **cannot** be granted to another ``USER`` or ``ROLE``. On the
    contrary, a ``ROLE`` **cannot** login to the database, and therefore
    **cannot** be assigned a password, but it **can** be
    :ref:`granted <granting_roles>` to another ``USER`` or ``ROLE``.

For usages of the ``CREATE USER`` statement see
:ref:`administration_user_management`.

Parameters
==========

:username:
  The unique name of the database user.

  The name follows the principles of a SQL identifier (see
  :ref:`sql_lexical_keywords_identifiers`).

Clauses
=======

``WITH``
--------

The following ``user_parameter`` are supported to define a new user account:

:password:
  The password as cleartext entered as string literal. e.g.::

     CREATE USER john WITH (password='foo')

  ::

     CREATE USER john WITH password='foo'

  ::

     CREATE USER john WITH password 'foo'

  ::

     CREATE USER john password 'foo'

.. vale off

:jwt:
  JWT properties map ('iss' and 'username') entered as string literal. e.g.::

     CREATE USER john WITH (jwt = {"iss" = 'https://login.company.org/keys', "username" = 'test@company.org'})

  `iss`_ is a JWK endpoint, containing public keys.

  ``username`` is a user name in a third party app.

  Combination of ``iss`` and ``username`` must be unique.

.. vale on

.. _iss: https://www.rfc-editor.org/rfc/rfc7519#section-4.1.1
