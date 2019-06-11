.. _config:

=============
Configuration
=============

CrateDB ships with sensible defaults, so configuration is typically not needed
for basic, single node use.

CrateDB can be configured via configuration files. These files are located in
the ``config`` directory inside the :ref:`CRATE_HOME <conf-env-crate-home>`
directory.

The configuration directory can changed via the ``path.conf`` setting, like
so:

.. code-block:: sh

    sh$ ./bin/crate -Cpath.conf=<CUSTOM_CONFIG_DIR>

Here, replace ``<CUSTOM_CONFIG_DIR>`` with the path to your custom
configuration directory.

The primary configuration file is named ``crate.yml``. The default version of
this file has a commented out listing of every available setting. (Some
features, such as :ref:`logging <conf-logging>`, use feature-specific files.)

Settings can be configured via the config file or via the ``-C`` option at
startup. So, for example, you can set the cluster name at startup, like so:

.. code-block:: sh

    sh$ ./bin/crate -Ccluster.name=cluster

Settings passed at startup use the same name as the settings in the
configuration file. So the equivalent setting in the configuration file would
be:

.. code-block:: yaml

    cluster.name = cluster

Settings are applied in the following order:

 1. Default values
 2. Configuration file
 3. Command-line options

Each setting value overwrites any previous value. So, for example, command line
settings will override configuration file settings.

.. TIP::

   Cluster settings can be :ref:`changed at runtime
   <administration-runtime-config>`.

.. NOTE::

   If you're just getting started with a particular part of CrateDB, we
   recommend you consult the appropriate top-level section of the
   documentation. The rest of this configuration documentation assumes a basic
   familiarity with the relevant parts of CrateDB.

.. rubric:: Table of contents

.. toctree::
   :maxdepth: 2

   node
   cluster
   session
   logging
   environment
