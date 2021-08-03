.. _conf-session:

================
Session settings
================

.. rubric:: Table of contents

.. contents::
   :local:

Session settings only apply to the currently connected client session.


Usage
=====

To configure a modifiable session setting, use :ref:`SET <ref-set>`, for
example:

.. code-block:: sql

  SET search_path TO myschema, doc;

To retrieve the current value of a session setting, use :ref:`SHOW <ref-show>`
eg:

.. code-block:: sql

  SHOW search_path;

Besides using ``SHOW``, it is also possible to use the :ref:`current_setting
<scalar-current_setting>` :ref:`scalar function <scalar-functions>`.


Supported session settings
==========================

.. _conf-session-search-path:

**search_path**
  | *Default:* ``pg_catalog, doc``
  | *Modifiable:* ``yes``

  The list of schemas to be searched when a relation is referenced without a
  schema.

  CrateDB will try to resolve an unqualified relation name against the
  configured ``search_path`` by iterating over the configured schemas in the
  order they were declared. The first matching relation in the ``search_path``
  is used. CrateDB will report an error if there is no match.

  .. NOTE::

     This setting mirrors the PostgreSQL `search_path`_ setting.

     Some :ref:`PostgreSQL clients <interface-postgresql>` require access to
     various tables in the ``pg_catalog`` schema. Usually, this is to extract
     information about built-in data types or :ref:`functions
     <gloss-function>`.

     CrateDB implements the system ``pg_catalog`` schema and it automatically
     includes it in the ``search_path`` *before* the configured schemas, unless
     it is already explicitly in the schema configuration.

.. _conf-session-enable-hashjoin:

**enable_hashjoin**
  | *Default:* ``true``
  | *Modifiable:* ``yes``

  An :ref:`experimental <experimental-warning>` setting which enables CrateDB
  to consider whether a ``JOIN`` :ref:`operation <gloss-operator>` should be
  :ref:`evaluated <gloss-evaluation>` using the ``HashJoin`` implementation
  instead of the ``Nested-Loops`` implementation.

  .. NOTE::

     It is not always possible or efficient to use the ``HashJoin``
     implementation. Having this setting enabled, will only add the option of
     considering it, it will not guarantee it.  See also the :ref:`available
     join algorithms <available-join-algo>` for more insights on this topic.

.. _conf-session-max_index_keys:

**max_index_keys**
  | *Default:* ``32``
  | *Modifiable:* ``no``

  Shows the maximum number of index keys.

  .. NOTE::

     The session setting has no effect in CrateDB and exists for compatibility
     with ``PostgreSQL``.

.. _conf-session-server_version_num:

**server_version_num**
  | *Default:* ``100500``
  | *Modifiable:* ``no``

  Shows the emulated ``PostgreSQL`` server version.


.. _conf-session-server_version:

**server_version**
  | *Default:* ``10.5``
  | *Modifiable:* ``no``

  Shows the emulated ``PostgreSQL`` server version.

.. _conf-session-optimizer:

**optimizer**
  | *Default:* ``true``
  | *Modifiable:* ``yes``

  This setting indicates whether a query optimizer rule is activated. The name
  of the query optimizer rule has to be provided as a suffix as part of the
  setting e.g. ``SET optimizer_rewrite_collect_to_get = false``.

  .. NOTE::

   The optimizer setting is for advanced use only and can significantly impact
   the performance behavior of the queries.

.. _experimental-warning:

.. WARNING::

  Experimental session settings might be removed in the future even in minor
  feature releases.


.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
