.. highlight:: sh

.. _conf-session-settings:

==========================
Session Setting Parameters
==========================

The section lists the session setting parameters supported by CrateDB.

Parameters can be set with the ``SET/SET SESSION`` statement, see
:ref:`ref-set`.

**search_path**
  | *Default:* ``doc``

  This parameter holds the default schema for a session that should be used
  when no schema is provided in queries.

  The value of ``search_path`` can be either a string or a comma-separated
  list of strings. However, CrateDB only considers the first element when a
  list is provided.
