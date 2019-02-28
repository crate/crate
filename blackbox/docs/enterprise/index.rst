.. _enterprise_features:

===================
Enterprise Features
===================

.. rubric:: Table of Contents

.. contents::
   :local:

Feature List
============

- :ref:`administration_user_management`: manage multiple database users
- :ref:`administration-privileges`: configure user privileges
- :ref:`admin_auth`: manage your database with authentication, and
  more
- System information functions: :ref:`CURRENT_USER <current_user>`,
  :ref:`USER <user>`, :ref:`SESSION_USER <session_user>`
- :ref:`Support for JavaScript in UDF <udf_lang_js>`: write user-defined
  functions in JavaScript
- :ref:`jmx_monitoring`: monitor your query stats with JMX
- :ref:`aggregation-hll-distinct`: distinct count aggregation using the
  `HyperLoglog++`_ algorithm
- :ref:`window-function-firstvalue`: ``first_value`` window function
- :ref:`window-function-lastvalue`: ``last_value`` window function
- :ref:`window-function-nthvalue`: ``nth_value`` window function
- `The CrateDB admin UI`_: `shards browser`_, `monitoring overview`_,
  `privileges browser`_

.. NOTE::

   When you first download CrateDB, the :ref:`license.enterprise
   <conf-node-enterprise-license>` setting is set to ``true`` which
   results in the enterprise features being enabled.
   However, we are currently moving away from checking for enterprise features
   based on that setting. Thus the :ref:`license.enterprise
   <conf-node-enterprise-license>` setting is now deprecated
   and will be removed in the future.

.. _enterprise_trial:

Trial
=====

You may evaluate CrateDB during a 30-day trial period, after which you must
`request an enterprise license`_ and configure CrateDB using the :ref:`SET
LICENSE <ref-set-license>` statement.

.. NOTE::

    When the trial period ends, CrateDB functionality will be limited to
    executing the following statements:

    - :ref:`SET LICENSE <ref-set-license>`

    - :ref:`SELECT <sql_reference_select>` (:ref:`information_schema
      <information_schema>` and :ref:`sys <system-information>` schemas only)

If you wish to continue using CrateDB without an enterprise license after the
trial period ends you must set :ref:`license.enterprise
<conf-node-enterprise-license>` to ``false``. This activates the `community
edition`_ of CrateDB and restores all functionality except for the enterprise
features.

.. NOTE::

   As we are currently changing the way for switching to the CrateDB
   `community edition`_, the :ref:`license.enterprise
   <conf-node-enterprise-license>` setting is deprecated
   and will be removed in the future.

.. _community Edition: https://crate.io/products/cratedb-editions/
.. _enterprise license: https://crate.io/products/cratedb-editions/
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _request an enterprise license: https://crate.io/pricing/#contactsales
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html#shards
.. _The CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
