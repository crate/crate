.. _conf-session:

================
Session Settings
================

This section documents settings that only apply to the currently connected client
session. Currently, there is only one of these settings.

.. SEEALSO::

    Settings can be changed during a session with :ref:`ref-set`.

.. _conf-session-search-path:

``search_path``: *schema names* (default: ``doc``)
  The list of schemas that will be used to look for a relation that is
  referenced without a schema.

  CrateDB will try to resolve an unqualified relation name against the
  configured ``search path`` by iterating over the configured schemas in the
  order they were declared in. The first matching relation in the
  ``search path`` is used, or an error is reported if there is no match.

  In order to configure the ``search path`` we use

    .. code-block:: sql

        SET search_path TO myschema, doc;

  To retrieve the current set value, :ref:`ref-show` can be used:

    .. code-block:: sql

        SHOW search_path;

  This setting mirrors the PostgreSQL `search_path`_ setting.

  Some clients, which generally connect to CrateDB using the
  :ref:`PostgreSQL wire protocol <postgres_wire_protocol>`, require access to
  various tables in the `pg_catalog` schema, usually to extract information
  about built-in data types or functions.
  CrateDB implements the system `pg_catalog` schema and it implicitly includes
  it in the ``search_path`` *before* the configured schemas, unless it is
  explicitly declared in the ``search_path`` on any position.

.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
