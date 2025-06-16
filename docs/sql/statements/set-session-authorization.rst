.. highlight:: psql
.. _ref-set-session-authorization:

=======================================
``SET AND RESET SESSION AUTHORIZATION``
=======================================

Set the user of the current session.

Synopsis
========

::

    SET [ SESSION | LOCAL ] SESSION AUTHORIZATION username
    SET [ SESSION | LOCAL ] SESSION AUTHORIZATION DEFAULT
    RESET SESSION AUTHORIZATION

Description
===========

These statements set or reset the user of the session to the given or revert
the user back to the original authenticated user.

The session user can be changed only if the initial authenticated user had the
superuser privileges. Otherwise, the statement is only accepted if the specified
``username`` matches the originally authenticated user.

Using this statement, a superuser can temporarily become an unprivileged user
and later switch back to a superuser. The superuser would switch using
``SET SESSION AUTHORIZATION '<impersonating_user>'`` to drop privileges and
become ``impersonating_user``. Later the original privileges can be restored
using ``SET SESSION AUTHORIZATION <real_username>`` or
``SET SESSION AUTHORIZATION DEFAULT``.

``SET LOCAL`` does not have any effect on session. All ``SET LOCAL`` statements
will be ignored by CrateDB and logged with the ``INFO`` logging level.

The ``DEFAULT`` and ``RESET`` forms reset the session and current user to be
the originally authenticated user.

Parameters
==========

:username:
  The user name represented as an identifier or a string literal.

:DEFAULT:
  Used for resetting the session user to initial session (authenticated) user.
