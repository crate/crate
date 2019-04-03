.. _conf-session:

================
Session Settings
================

Session settings only apply to the currently connected client session. At the
moment, there is only one such setting.

.. SEEALSO::

    Settings can be changed during a session with :ref:`ref-set`.

.. _conf-session-search-path:

``search_path``: *schema names* (default: ``doc``)
  The list of schemas to be searched when a relation is referenced without a
  schema.

  CrateDB will try to resolve an unqualified relation name against the
  configured ``search_path`` by iterating over the configured schemas in the
  order they were declared. The first matching relation in the ``search_path``
  is used. CrateDB will report an error if there is no match.

  To configure the ``search_path``, use :ref:`SET <ref-set>`:

    .. code-block:: sql

        SET search_path TO myschema, doc;

  The current search path value could be retrieved using :ref:`SHOW
  <ref-show>`:

    .. code-block:: sql

        SHOW search_path;

   Besides using ``SHOW``, it is also possible to use the :ref:`current_setting
   <scalar_current_setting>` scalar function to retrieve the value of a session
   setting.

  .. NOTE::

     This setting mirrors the PostgreSQL `search_path`_ setting.

     Some :ref:`PostgreSQL clients <postgres_wire_protocol>` require access to
     various tables in the ``pg_catalog`` schema. Usually, this is to extract
     information about built-in data types or functions.

     CrateDB implements the system ``pg_catalog`` schema and it automatically
     includes it in the ``search_path`` *before* the configured schemas, unless
     it is already explicitly in the schema configuration.

.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
