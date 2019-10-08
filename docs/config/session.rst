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

To configure a modifiable session setting, use :ref:`SET <ref-set>`,
for example:

.. code-block:: sql

  SET search_path TO myschema, doc;

To retrieve the current value of a session setting, use :ref:`SHOW <ref-show>`
eg:

.. code-block:: sql

  SHOW search_path;

Besides using ``SHOW``, it is also possible to use the :ref:`current_setting
<scalar_current_setting>` scalar function.


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

     Some :ref:`PostgreSQL clients <postgres_wire_protocol>` require access to
     various tables in the ``pg_catalog`` schema. Usually, this is to extract
     information about built-in data types or functions.

     CrateDB implements the system ``pg_catalog`` schema and it automatically
     includes it in the ``search_path`` *before* the configured schemas, unless
     it is already explicitly in the schema configuration.

.. _conf-session-enable-hashjoin:

**enable_hashjoin**
  | *Default:* ``true``
  | *Modifiable:* ``yes``

  An :ref:`experimental <experimental-warning>` setting which enables CrateDB
  to consider whether a Join operation should be evaluated using the
  ``HashJoin`` implementation instead of the ``Nested-Loops`` implementation.

  .. NOTE::

     It is not always possible or efficient to use the ``HashJoin``
     implementation. Having this setting enabled, will only add the
     option of considering it, it will not guaranty it.
     See also the :ref:`available join algorithms
     <available-join-algo>` for more insights on this topic.

.. _conf-session-max_index_keys:

**max_index_keys**
  | *Default:* ``32``
  | *Modifiable:* ``no``

  Shows the maximum number of index keys.

  .. NOTE::

     The session setting has not effect on CrateDB and was added to enhance
     compatibility with ``PostgreSQL``.

.. _experimental-warning:

.. WARNING::

  Experimental session settings might be removed in the future
  even in minor feature releases.

.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
