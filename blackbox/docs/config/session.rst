.. _conf-session:

================
Session Settings
================

.. rubric:: Table of Contents

.. contents::
   :local:

Session settings only apply to the currently connected client session.

Usage
=====

To configure a session setting, use :ref:`SET <ref-set>` eg:

.. code-block:: sql

  SET search_path TO myschema, doc;

To retrieve the current value of a session setting, use :ref:`SHOW <ref-show>`
eg:

.. code-block:: sql

  SHOW search_path;

Besides using ``SHOW``, it is also possible to use the :ref:`current_setting
<scalar_current_setting>` scalar function.


Supported Session Settings
==========================

.. _conf-session-search-path:

``search_path``: *schema names* (default: ``doc``)
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

``enable_semijoin``: *enabled* (default: ``false``)
  An :ref:`experimental <experimental-warning>` setting which enables CrateDB
  to consider rewriting a ``SemiJoin`` query into a conventional join query,
  if possible. For instance, a query in the form of:
  ``select x from t1 where x in (select y from t2)``
  will be rewritten as:
  ``select t1.x from t1 SEMI JOIN (select y from t2) t2 on t1.x = t2.y``

  .. NOTE::

     It is not always possible to rewrite a ``SemiJoin`` as a conventional
     Join. Having this setting enabled, will only trigger the attempt of a
     rewrite but not guaranty it.

.. _conf-session-enable-hashjoin:

``enable_hashjoin``: *enabled* (default: ``true``)
  An :ref:`experimental <experimental-warning>` setting which enables CrateDB
  to consider whether a Join operation should be evaluated using the
  ``HashJoin`` implementation instead of the ``Nested-Loops`` implementation.

  .. NOTE::

     It is not always possible or efficient to use the ``HashJoin``
     implementation. Having this setting enabled, will only add the
     option of considering it, it will not guaranty it.
     See also the :ref:`available join algorithms
     <available-join-algo>` for more insights on this topic.

.. _experimental-warning:

.. WARNING::

  Experimental session settings might be removed in the future
  even in minor feature releases.

.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
