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

.. WARNING::

   We are currently moving away from enabling enterprise features
   based on the :ref:`license.enterprise
   <conf-node-enterprise-license>` setting. Thus this setting is
   now deprecated and will be removed in the future, where users
   will be able to switch to the Community Edition by building
   their own tarball.

.. _enterprise_trial:

Trial
=====

You may evaluate CrateDB for as long as you require, there is no time limit.
However, you can do this on a three-node cluster setup. Should you require
to scale up your cluster, you can `request an enterprise license`_
and configure CrateDB using the :ref:`SET LICENSE <ref-set-license>` statement.

.. NOTE::

    If your cluster size becomes greater than the allowed threshold,
    CrateDB functionality will be limited to executing the following
    statements:

    - :ref:`SET LICENSE <ref-set-license>`,
      to register another license with different node threshold.

    - :ref:`ALTER CLUSTER DECOMMISSION <alter_cluster_decommission>`,
      to downsize your cluster to the allowed threshold

    - :ref:`SELECT <sql_reference_select>` (:ref:`information_schema
      <information_schema>` and :ref:`sys <system-information>` schemas only)

If you wish to continue using CrateDB without an enterprise license, you can
switch to the CrateDB `community edition`_, restoring all functionality
except for the enterprise features. To do so, you need to build the tarball
yourself (using the dedicated gradle task:
``$ ./gradlew communityEditionDistTar``)

.. NOTE::

   If you already have an existing 30-day trial license, please contact the
   sales department when license is due to expire.

.. _community Edition: https://crate.io/products/cratedb-editions/
.. _enterprise license: https://crate.io/products/cratedb-editions/
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _request an enterprise license: https://crate.io/pricing/#contactsales
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html#shards
.. _The CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
