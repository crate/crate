.. _config:

=============
Configuration
=============

CrateDB ships with sensible defaults, so no configuration is needed for basic,
single node use.

CrateDB is typically configured via a configuration file located at
``config/crate.yml`` within the install directory.

The location of the config directory can be specified at startup like so:

.. code-block:: sh

    sh$ ./bin/crate -Cpath.conf=/path/to/config/dir

The configuration file distributed with CrateDB has a commented out listing of
every available setting.

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

.. NOTE::

   Cluster settings can be :ref:`changed at runtime
   <administration-runtime-config>`.

.. NOTE::

   If you're just getting started with a particular part of CrateDB, we
   recommend you consult the appropriate top-level section of the
   documentation. The configuration documentation assumes a basic familiarity
   with the relevant parts of CrateDB.

.. rubric:: Table of Contents

.. toctree::
   :maxdepth: 2

   node
   cluster
   session
   logging
   environment
   system
   enterprise


