.. highlight:: psql
.. _ref-show:

===========================
``SHOW (session settings)``
===========================

The ``SHOW`` statement can display the value of one session setting
variable. Some of these can also be configured via :ref:`SET SESSION <ref-set>`.

.. note:

   The ``SHOW`` statement for session settings is unrelated to the other ``SHOW``
   statements like e.g. ``SHOW TABLES``.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    SHOW parameter_name


Parameters
==========

:parameter_name:
  The name of the session setting which should be printed. See :ref:`conf-session`
  for available session settings.
