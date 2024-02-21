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
e.g:

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

.. _conf-session-application-name:

**application_name**
  | *Default:* ``null``
  | *Modifiable:* ``yes``

  An arbitrary application name that can be set to identify an application that
  connects to a CrateDB node.

  Some clients set this implicitly to their client name.


.. _conf-session-statement-timeout:

**statement_timeout**
  | *Default:* ``'0'``
  | *Modifiable:* ``yes``

  The maximum duration of any statement before it gets cancelled. If ``0`` (the
  default), queries are allowed to run infinitely and don't get cancelled
  automatically.

  The value is an ``INTERVAL`` with a maximum of ``2147483647`` milliseconds.
  That's roughly 24 days.

.. _conf-session-memory-operation-limit:

**memory.operation_limit**
   | *Default:* ``0``
   | *Modifiable:* ``yes``

This is an experimental expert setting defining the maximal amount of memory in
bytes that an individual operation can consume before triggering an error.

``0`` means unlimited. In that case only the global circuit breaker limits
apply.

There is no 1:1 mapping from SQL statement to operation. Some SQL statements
have no corresponding operation. Other SQL statements can have more than one
operation. You can use the :ref:`sys.operations <sys-operations>` view to get
some insights, but keep in mind that both, operations which are used to execute
a query, and their name could change with any release, including hotfix
releases.

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

.. _conf-session-error_on_unknown_object_key:

**error_on_unknown_object_key**
  | *Default:* ``true``
  | *Modifiable:* ``yes``

  This setting controls the behaviour of querying unknown object keys to
  dynamic objects. CrateDB will throw an error by default if any of the queried
  object keys are unknown or will return a null if the setting is set to false.

.. _conf-session-datestyle:

**datestyle**
  | *Default:* ``ISO``
  | *Modifiable:* ``yes``

  Shows the display format for date and time values. Only the ``ISO`` style is 
  supported. Optionally provided pattern conventions for the order of date 
  parts (Day, Month, Year) are ignored.

  .. NOTE::

     The session setting currently has no effect in CrateDB and exists for 
     compatibility with ``PostgreSQL``. Trying to set this to a date format 
     style other than ``ISO`` will raise an exception.

.. _conf-session-max_index_keys:

**max_index_keys**
  | *Default:* ``32``
  | *Modifiable:* ``no``

  Shows the maximum number of index keys.

  .. NOTE::

     The session setting has no effect in CrateDB and exists for compatibility
     with ``PostgreSQL``.

.. _conf-session-max_identifier_length:

**max_identifier_length**
  | *Default:* ``255``
  | *Modifiable:* ``no``

  Shows the maximum length of identifiers in bytes.

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

.. _conf-session-standard_conforming_strings:

**standard_conforming_strings**
  | *Default:* ``on``
  | *Modifiable:* ``no``

  Causes ``'...'`` strings to treat backslashes literally.

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


.. _conf-session-optimizer_eliminate_cross_join:

.. vale off

**optimizer_eliminate_cross_join**
  | *Default:* ``true``
  | *Modifiable:* ``yes``

  This setting indicates if the :ref:`cross join elimination
  <join-optim-cross-join-elimination>` rule of the optimizer rule is activated.

.. vale on

.. _experimental-warning:

.. WARNING::

  Experimental session settings might be removed in the future even in minor
  feature releases.

.. _conf-session-insert-fail-fast:

**insert_fail_fast**
  | *Default:* ``false``
  | *Modifiable:* ``yes``

   This setting enables partial failures of ``INSERT`` and ``UPDATE``
   statements. First error encountered gets propagated.

.. _search_path: https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
