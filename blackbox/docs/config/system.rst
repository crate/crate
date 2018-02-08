.. highlight:: sh

.. _conf-system:

==============================
Operating System Configuration
==============================

Virtual Memory
--------------

CrateDB uses a hybrid MMap/NIO file system storage directory to store its
indices. Out-of-memory exceptions and failing bootstrap checks may be caused by
the operating system's default limit on ``max_map_count`` (the maximum number
of memory map areas a process may have) being too low.

On Linux, you can increase the limit by running the following command::

    sh$ sudo sysctl -w vm.max_map_count=262144

To set this value permanently, update the ``vm.max_map_count`` setting in
``/etc/sysctl.conf``. You can then verify this after rebooting by running::

    sh$ sysctl vm.max_map_count
