.. highlight:: psql
.. _kill_job:

===============
Jobs Management
===============

Each executed sql statement results in a corresponding job. Jobs that are
currently executing are logged in the system table ``sys.jobs`` (see
:ref:`jobs_operations_logs`).

To obtain the `UUID`_ of a job, stats needs to be enabled (see
:ref:`conf_collecting_stats`). Job logging can be disabled by
setting the queue size to zero.

Killing an active job forces CrateDB to stop its execution on the cluster
immediately. There are two different SQL commands available for killing jobs.

The ``KILL ALL`` statement stops every single job on each node that is running.
It returns the total number of contexts of all jobs that have been killed. A
job can have contexts on multiple nodes.

::

    cr> kill all;
    KILL OK, ... rows affected (... sec)

``KILL job_id`` kills one single job with the specified ``job_id``. Like ``KILL
ALL`` it returns the total number of contexts of that job killed on all nodes.

::

    cr> kill '175011ce-9bbc-45f2-a86a-5b7f993a93a6';
    KILL OK, ... rows affected (... sec)

See :ref:`sql_kill` for detailed syntax information on ``KILL`` statements.

.. _`UUID`: http://en.wikipedia.org/wiki/Universally_unique_identifier
