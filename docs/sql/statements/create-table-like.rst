.. highlight:: psql
.. _ref-create-table-like:


=====================
``CREATE TABLE LIKE``
=====================

Create a new table with the same schema as an existing table.

Synopsis
========

::

    CREATE TABLE [ IF NOT EXISTS ] table_ident ( LIKE source_table [ like_option [...] ] )

where ``like_option`` is::

    { INCLUDING | EXCLUDING } { ALL | CONSTRAINTS | DEFAULTS | GENERATED | INDEXES | STORAGE }


Description
===========

``CREATE TABLE LIKE`` will create a new empty table with the same column
definitions as the source table. Unlike :ref:`ref-create-table-as`, no data is
copied.

By default (without any ``like_option``), only column names, data types, and
``NOT NULL`` constraints are copied. All other properties are excluded unless
explicitly included using ``INCLUDING`` options.

Partitioning (``PARTITIONED BY``) and clustering (``CLUSTERED BY``) settings
are always copied from the source table.

For further details on the default values of the optional parameters,
see :ref:`sql-create-table`.

``IF NOT EXISTS``
=================

If the optional ``IF NOT EXISTS`` clause is used, this statement won't do
anything if the table exists already.

Parameters
==========

:table_ident:
  The name (optionally schema-qualified) of the table to be created.

:source_table:
  The name (optionally schema-qualified) of an existing table to copy the
  schema from. Views and system tables cannot be used as a source.

:like_option:
  Controls which additional properties are copied from the source table.
  Multiple options can be specified. If the same kind of option is specified
  more than once, the last one takes effect.

  ``INCLUDING ALL``
    Copy all properties. Equivalent to specifying ``INCLUDING`` for every
    individual option.

  ``INCLUDING CONSTRAINTS``
    Copy ``CHECK`` constraints and ``PRIMARY KEY`` constraints.
    ``NOT NULL`` constraints are always copied, regardless of this option.

  ``INCLUDING DEFAULTS``
    Copy ``DEFAULT`` expressions for columns.

  ``INCLUDING GENERATED``
    Copy ``GENERATED ALWAYS AS`` expressions for generated columns. Without
    this option, generated columns are created as regular columns with their
    data type preserved.

  ``INCLUDING INDEXES``
    Copy index definitions and column-level index settings (e.g.,
    ``FULLTEXT``, ``INDEX OFF``).

  ``INCLUDING STORAGE``
    Copy column storage settings (e.g., ``columnstore``) and table-level
    ``WITH`` properties (e.g., ``number_of_replicas``, ``column_policy``).

  ``EXCLUDING``
    Can be used with any of the above to explicitly exclude a category. Useful
    in combination with ``INCLUDING ALL`` (e.g.,
    ``INCLUDING ALL EXCLUDING DEFAULTS``).
