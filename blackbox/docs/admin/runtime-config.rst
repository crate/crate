.. _administration-runtime-config:

=====================
Runtime Configuration
=====================

The CrateDB cluster can be configured at runtime using the :ref:`SET <ref-set>`
and :ref:`RESET <ref-set>` statement. See the :ref:`Cluster Settings
<conf-cluster-settings>` configuration section for details about the supported
settings.

If :ref:`SET <ref-set>` is used with ``PERSISTENT`` the change will survive a
cluster restart, if used with ``TRANSIENT`` the value will be restored to
default or config file value on a restart::

    cr> SET GLOBAL PERSISTENT stats.enabled = false;
    SET OK, 1 row affected (... sec)

::

    cr> select sys.cluster.settings['stats']['enabled'] from sys.cluster;
    +------------------------------+
    | settings['stats']['enabled'] |
    +------------------------------+
    | FALSE                        |
    +------------------------------+
    SELECT 1 row in set (... sec)

You can change multiple values at once::

    cr> SET GLOBAL TRANSIENT stats.enabled = true,
    ... stats.jobs_log_size = 1024, stats.operations_log_size = 4096;
    SET OK, 1 row affected (... sec)

::

    cr> select settings['stats']['enabled'],
    ...   settings['stats']['jobs_log_size'],
    ...   settings['stats']['operations_log_size']
    ... from sys.cluster;
    +-...------------+-...------------------+-...------------------------+
    | ...['enabled'] | ...['jobs_log_size'] | ...['operations_log_size'] |
    +-...------------+-...------------------+-...------------------------+
    | TRUE           |                 1024 |                       4096 |
    +-...------------+-...------------------+-...------------------------+
    SELECT 1 row in set (... sec)

Its also possible to save a complete nested object of
settings::

    cr> SET GLOBAL TRANSIENT stats = {
    ...   jobs_log_size = 2048,
    ...   operations_log_size = 8192
    ... };
    SET OK, 1 row affected (... sec)

::

    cr> select settings['stats'] from sys.cluster;
    +--...--------------------------------------------------------------------------------------------------------------------------------------...--+
    | settings['stats']                                                                                                                              |
    +--...--------------------------------------------------------------------------------------------------------------------------------------...--+
    | {... "enabled": true, "jobs_log_expiration": "0s", "jobs_log_size": 2048, "operations_log_expiration": "0s", "operations_log_size": 8192, ...} |
    +--...--------------------------------------------------------------------------------------------------------------------------------------...--+
    SELECT 1 row in set (... sec)

Using the ``RESET`` statement, a setting will be reset to either on node
startup defined configuration file value or to its default value::

    cr> RESET GLOBAL stats.enabled, stats.operations_log_size;
    RESET OK, 1 row affected (... sec)

::

    cr> select settings['stats'] from sys.cluster;
    +--...----------------------------------------------------------------------------------------------------------------------------------------...--+
    | settings['stats']                                                                                                                                |
    +--...----------------------------------------------------------------------------------------------------------------------------------------...--+
    | {... "enabled": false, "jobs_log_expiration": "0s", "jobs_log_size": 2048, "operations_log_expiration": "0s", "operations_log_size": 10000, ...} |
    +--...----------------------------------------------------------------------------------------------------------------------------------------...--+
    SELECT 1 row in set (... sec)

``RESET`` can also be done on objects::

    cr> RESET GLOBAL stats;
    RESET OK, 1 row affected (... sec)

::

    cr> select settings['stats'] from sys.cluster;
    +--...-----------------------------------------------------------------------------------------------------------------------------------------...--+
    | settings['stats']                                                                                                                                 |
    +--...-----------------------------------------------------------------------------------------------------------------------------------------...--+
    | {... "enabled": false, "jobs_log_expiration": "0s", "jobs_log_size": 10000, "operations_log_expiration": "0s", "operations_log_size": 10000, ...} |
    +--...-----------------------------------------------------------------------------------------------------------------------------------------...--+
    SELECT 1 row in set (... sec)
