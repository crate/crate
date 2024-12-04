.. highlight:: psql
.. _ref-create-table-as:


===================
``CREATE TABLE AS``
===================

Define a new table from existing tables.

.. rubric:: Table of contents

.. contents::
   :local:


Synopsis
========

::

    CREATE TABLE [ IF NOT EXISTS ] table_ident AS { ( query ) | query }


Description
===========

``CREATE TABLE AS`` will create a new table and insert rows based on the
specified ``query``.

Only the column names, types, and the output rows will be used from the
``query``. Default values will be assigned to the optional parameters used for
the table creation.

For further details on the default values of the optional parameters,
see :ref:`sql-create-table`.

``IF NOT EXISTS``
=================

If the optional ``IF NOT EXISTS`` clause is used, this statement won't do
anything if the table exists already, and ``0`` rows will be returned.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be created.

:query:
    A query (``SELECT`` statement) that supplies the rows to be inserted.
    Refer to the ``SELECT`` statement for a description of the syntax.
