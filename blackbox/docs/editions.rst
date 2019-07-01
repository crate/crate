.. _editions:

========
Editions
========

When you download the pre-build CrateDB packages from the Crate.io website, you
are downloading the standard edition of CrateDB. This edition comes with
*Enterprise Features*. If you want to run CrateDB on more than three nodes, you
must acquire an *Enterprise License*.

When you build CrateDB from source, you get the CrateDB Community Edition
which does not come with enterprise features and does not have any node
limitations.

.. SEEALSO::

    For more information about the different editions of CrateDB, consult the
    overview `product comparison`_.

    For more detailed licensing information, please contact us directly or
    consult the information in the `NOTICE`_ and `LICENSE`_ files.

.. rubric:: Table of contents

.. contents::
   :local:

.. _standard-edition:

CrateDB
=======

.. _enterprise-features:

Enterprise features
-------------------

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
- :ref:`window-function-lag`: ``lag`` window function
- :ref:`window-function-lead`: ``lead`` window function
- :ref:`window-function-nthvalue`: ``nth_value`` window function
- `The CrateDB admin UI`_: `shards browser`_, `monitoring overview`_,
  `privileges browser`_

.. _node-limitations:

Node limitation
---------------

To make full use of CrateDB, you must `acquire an Enterprise License`_. Unless and
until you do that, CrateDB is limited to running on no more than three nodes.

.. NOTE::

    An Enterprise License will be provided at no cost for use by non-profit and
    educational organizations. Limitations apply. Please `contact us`_.

.. CAUTION::

    If you exceed the three nodes limitation your cluster will stop accepting
    queries and CrateDB functionality will be limited to:

     - :ref:`SET LICENSE <ref-set-license>`

     - :ref:`SELECT <sql_reference_select>` (:ref:`information_schema
       <information_schema>` and :ref:`sys <system-information>` schemas only)

     - :ref:`alter_cluster_decommission`

    To restore functionality, you must :ref:`SET LICENSE <ref-set-license>` or
    scale down to three or fewer nodes.

If you wish to use CrateDB without an Enterprise License and without the three
node limitation, you can switch to the :ref:`community-edition`.

.. _community-edition:

CrateDB Community Edition
=========================

The CrateDB *Community Edition* (CrateDB CE) does not include any
:ref:`enterprise-features` but can be run on as many nodes as you wish.

CrateDB CE must be built from source, like so:

.. code-block:: console

   sh$ git clone https://github.com/crate/crate
   sh$ cd crate
   sh$ git submodule update --init
   sh$ git checkout <TAG>
   sh$ ./gradlew clean communityEditionDistTar

Here, replace ``<TAG>`` with the commit hash of the Git tag that corresponds to
the `release`_ you wish to use.

When the ``gradlew`` command successfully completes, the relevant CrateDB CE
release tarball will be located in the ``app/build/distributions`` directory.

.. _acquire an enterprise license: https://crate.io/pricing/#contactsales
.. _contact us: https://crate.io/pricing/#contactsales
.. _enterprise license: https://crate.io/products/cratedb-editions/
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _LICENSE: https://github.com/crate/crate/blob/master/LICENSE
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _NOTICE: https://github.com/crate/crate/blob/master/NOTICE
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _product comparison: https://crate.io/products/cratedb-editions/
.. _release: https://github.com/crate/crate/releases
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html#shards
.. _The CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
