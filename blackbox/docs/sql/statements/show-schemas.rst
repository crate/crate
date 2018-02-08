.. _ref-show-schemas:

================
``SHOW SCHEMAS``
================

Lists the table schemas of the database.

.. rubric:: Table of Contents

.. contents::
   :local:

Synopsis
========

::

    SHOW SCHEMAS [LIKE 'pattern' | WHERE expression]

Description
===========

``SHOW SCHEMAS`` can be used to retrieve defined schema names of the database
in alphabetical order.

The same list can be fetched by querying the the schema names from the
``information_schema.schemata`` table.

Clauses
=======

``LIKE``
--------

The optional ``LIKE`` clause indicates which schema names to match. It takes a
string pattern as a filter and has an equivalent behavior to
:ref:`sql_dql_like`.

``WHERE``
---------

The optional WHERE clause defines the condition to be met for a row to be
returned.
