.. _enterprise_features:

===================
Enterprise Features
===================

CrateDB provides an `Enterprise Edition`_. The source code remains open, but if
you want to use one of the enterprise features in production you need to
purchase a `enterprise license`_.

The `Enterprise Edition`_ is enabled by default, the
:ref:`license.enterprise <conf-node-enterprise-license>` config being true, for
a period of 30 days that will allow users to evaluate the enterprise features.

Upon the expiration of the 30 days trial period you will have to request an
enterprise license by `contacting our sales department`_.
Setting a new license is achieved using the :ref:`SET LICENSE <ref-set>`
statement.

Alternatively, users seeking to use the community edition can do so at any
point by disabling the enterprise license using the
:ref:`license.enterprise <conf-node-enterprise-license>` config.

.. WARNING::

  CrateDB will continue to run after the license expires but only the
  :ref:`SET LICENSE <ref-set>` statement and the read only system information
  statements (against the ``sys`` and ``information_schema`` schemas) will be
  allowed.

Current features:

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
- `The CrateDB admin UI`_: `shards browser`_, `monitoring overview`_,
  `privileges browser`_

.. _enterprise edition: https://crate.io/enterprise-edition/
.. _enterprise license: https://crate.io/enterprise-edition/
.. _MQTT: http://mqtt.org/
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html#shards
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _The CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
.. _contacting our sales department: https://crate.io/pricing/#contactsales
