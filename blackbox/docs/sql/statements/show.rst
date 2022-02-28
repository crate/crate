.. highlight:: psql
.. _ref-show:

===========================
``SHOW (session settings)``
===========================

.. include:: ../../_include/version-note.rst

The ``SHOW`` statement can display the value of either one or all session
setting variables. Some of these can also be configured via
:ref:`SET SESSION <ref-set>`.

.. note:

   The ``SHOW`` statement for session settings is unrelated to the other ``SHOW``
   statements like e.g. ``SHOW TABLES``.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    SHOW { parameter_name | ALL }


Parameters
==========

:parameter_name:
  The name of the session setting which should be printed. See :ref:`conf-session`
  for available session settings.

:ALL:
  Show the values of all settings.
