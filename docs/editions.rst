.. _editions:

========
Editions
========

When you download the pre-built CrateDB packages from the Crate.io website, you
are downloading the edition that comes with *Enterprise Features*. If you want
to run CrateDB on more than three nodes, you must acquire an *Enterprise License*.

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

Take note of the following items when using this edition of CrateDB.


.. _enterprise-features:

Enterprise features
-------------------

- :ref:`administration_user_management` for multiple database users
- Configure user :ref:`privileges <administration-privileges>`
- Manage your database with :ref:`authentication <admin_auth>` and more
- Write :ref:`user-defined functions in JavaScript <udf_lang_js>`
- Take advantage of these features in the `CrateDB admin UI`_:

  - `shards browser`_

  - `monitoring overview`_

  - `privileges browser`_

- Unlock the following :ref:`system information functions <scalar-sysinfo>`:

  - :ref:`CURRENT_USER <current_user>`

  - :ref:`USER <user>`

  - :ref:`SESSION_USER <session_user>`

- :ref:`Monitor <jmx_monitoring>` your query stats with JMX
- Access to the :ref:`aggregation-hll-distinct` :ref:`aggregate function
  <aggregate-functions>`
- Access to the following :ref:`window functions <window-functions>`:

  - :ref:`window-function-firstvalue`

  - :ref:`window-function-lastvalue`

  - :ref:`window-function-lag`

  - :ref:`window-function-lead`

  - :ref:`window-function-nthvalue`

  - :ref:`window-function-rank`

  - :ref:`window-function-dense_rank`


.. _node-limitations:

Node limitation
---------------

To make full use of CrateDB, you must `acquire an Enterprise License`_. Unless
and until you do that, CrateDB is limited to running on no more than three nodes.

.. NOTE::

    An Enterprise License will be provided at no cost for use by non-profit and
    educational organizations. Limitations apply. Please `contact us`_.

.. CAUTION::

    If you exceed the three-node limitation, your cluster will stop accepting
    queries and CrateDB functionality will be limited to the following SQL
    statements:

     - :ref:`SET LICENSE <ref-set-license>`

     - :ref:`SELECT <sql_reference_select>` (:ref:`information_schema
       <information_schema>` and :ref:`sys <system-information>` schemas only)

     - :ref:`alter_cluster_decommission`

    To restore functionality, you must :ref:`SET LICENSE <ref-set-license>` or
    scale down to three or fewer nodes.

If you wish to use CrateDB without an Enterprise License and without the
three-node limitation, you can switch to the :ref:`community-edition`.


.. _community-edition:

CrateDB Community Edition
=========================

The CrateDB *Community Edition* (CrateDB CE) does not include any
:ref:`enterprise-features` but can be run on as many nodes as you wish.

.. NOTE::

   CrateDB requires a `Java virtual machine`_ to run.

   Starting with CrateDB 4.2, a JVM is bundled with the tarball and no
   extra installation is necessary.

   Versions starting from 3.0 to 4.1 require a `Java 11`_ installation. We
   recommend using `Oracle's Java`_ on macOS and OpenJDK_ on Linux Systems.

   Earlier versions required Java 8.

CrateDB CE must be built from source:

.. code-block:: console

   sh$ git clone https://github.com/crate/crate
   sh$ cd crate
   sh$ git checkout <TAG>
   sh$ ./gradlew clean communityEditionDistTar

The steps above:

- clone the CrateDB Git repository and navigate into the directory

- replace ``<TAG>`` with the Git tag that corresponds to
  the `release`_ you wish to use or just the version number of the release

- execute the `Gradle Wrapper`_ script included in the repository to clean up
  any old build files and to invoke a `distribution plugin`_ that will build
  the Community Edition tar archive

When the ``gradlew`` command completes successfully, the relevant CrateDB CE
release tarball will be located in the ``app/build/distributions`` directory.

You can refer to our guide on running `CrateDB tarball installations`_.


.. _acquire an enterprise license: https://crate.io/pricing/#contactsales
.. _contact us: https://crate.io/pricing/#contactsales
.. _CrateDB admin UI: https://crate.io/docs/clients/admin-ui/en/latest/index.html
.. _CrateDB tarball installations: https://crate.io/docs/crate/tutorials/en/latest/install-run/basic.html
.. _distribution plugin: https://docs.gradle.org/current/userguide/distribution_plugin.html
.. _enterprise license: https://crate.io/products/cratedb-editions/
.. _Gradle Wrapper: https://docs.gradle.org/current/userguide/gradle_wrapper.html
.. _HyperLogLog++: https://research.google.com/pubs/pub40671.html
.. _Java 11: https://www.oracle.com/technetwork/java/javase/downloads/index.html
.. _Java virtual machine: https://en.wikipedia.org/wiki/Java_virtual_machine
.. _LICENSE: https://github.com/crate/crate/blob/master/LICENSE
.. _monitoring overview: https://crate.io/docs/clients/admin-ui/en/latest/monitoring.html
.. _NOTICE: https://github.com/crate/crate/blob/master/NOTICE
.. _OpenJDK: https://openjdk.java.net/projects/jdk/11/
.. _Oracle's Java: https://www.java.com/en/download/help/mac_install.xml
.. _privileges browser: https://crate.io/docs/clients/admin-ui/en/latest/privileges.html
.. _product comparison: https://crate.io/products/cratedb-editions/
.. _release: https://github.com/crate/crate/tags
.. _shards browser: https://crate.io/docs/clients/admin-ui/en/latest/shards.html
