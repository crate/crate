.. _enterprise_features:

===================
Enterprise Features
===================

.. rubric:: Table of Contents

.. contents::
   :local:

Feature List
============

The default distribution of CrateDB contains the following enterprise features:


- :ref:`administration_user_management`: manage multiple database users
- :ref:`administration-privileges`: configure user privileges
- :ref:`admin_auth`: manage your database with authentication, and
  more
- System information functions: :ref:`CURRENT_USER <current_user>`,
  :ref:`USER <user>`, :ref:`SESSION_USER <session_user>`
- :ref:`Support for JavaScript in UDF <udf_lang_js>`: write user-defined
  functions in JavaScript
- :ref:`jmx_monitoring`: monitor your query stats with JMX
- :ref:`ingest_mqtt`: ingest data using MQTT_ without any 3rd party tools
- :ref:`aggregation-hll-distinct`: distinct count aggregation using the
  `HyperLoglog++`_ algorithm
- :ref:`window-function-firstvalue`: ``first_value`` window function
- :ref:`window-function-lastvalue`: ``last_value`` window function
- :ref:`window-function-nthvalue`: ``nth_value`` window function
- `The CrateDB admin UI`_: `shards browser`_, `monitoring overview`_,
  `privileges browser`_


.. note::

   It is also possible to build a :ref:`community-edition` which won't contain
   these features but which can be used without licensing restrictions.

.. _enterprise_trial:

Trial
=====

You may evaluate CrateDB including all enterprise features with a trial
license. This trial license is active by default and is limited to 3 nodes. If
you require more than 3 nodes you must `request an enterprise license`_  and
configure CrateDB using the :ref:`SET LICENSE <ref-set-license>` statement.

.. CAUTION::

   If you exceed the 3 nodes limitation your cluster will stop accepting
   queries. The functionality will be limited to:

    - :ref:`SET LICENSE <ref-set-license>`

    - :ref:`SELECT <sql_reference_select>` (:ref:`information_schema
      <information_schema>` and :ref:`sys <system-information>` schemas only)

    - :ref:`alter_cluster_decommission`

If you wish to use CrateDB without an enterprise license and without the 3
nodes limitation, you can switch to the :ref:`community-edition`.

.. _enterprise license: https://crate.io/products/cratedb-editions/
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _MQTT: http://mqtt.org/
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _request an enterprise license: https://crate.io/pricing/#contactsales
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html#shards
.. _The CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
