========
Crate DB
========

Crate is a shared nothing, fully searchable, document oriented
cluster database.

To use crate take a look at the documentation found under the ``docs``
directory.

Developers might have a look at the ``DEVELOP.rst`` document to get a
working development sandbox.

Configuration
=============

The settings file is located at config/crate.yml
The used default options are documented in this file.
To load a settings file located somewhere else in the system
pass the option -Des.config=/path/to/config.yml to the start script.

Any option can be configured either by the config file or as system
property. If using system properties the required prefix 'es.' will
be ignored.

For example, configuring the cluster name by using system properties
will work this way::

 $ crate -Des.cluster.name=cluster

Which is exactly the same like setting the cluster name by the config
file::

 cluster.name = cluster

Settings will get applied in the following order where the latter one
will overwrite the prior one:

 1. internal defaults
 2. system properties
 3. options from config file
