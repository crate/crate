.. _conf-session:

================
Session Settings
================

The section documents settings that only apply to the currently connected client
session. Currently, there is only one of these settings.

.. SEEALSO::

    Settings can be changed during a session with :ref:`ref-set`.

.. _conf-session-search-path:

``search_path``: *schema name* (default: ``doc``)
  The default schema for a session when no schema is provided.

  This setting mirrors the PostgreSQL `search_path`_ setting. However,
  while CrateDB will accept a list of schemas, for the time being, only the
  first one is used.

.. _search_path: https://www.postgresql.org/docs/8.1/static/ddl-schemas.html#DDL-SCHEMAS-PATH
