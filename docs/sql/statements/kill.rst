.. highlight:: psql
.. _sql_kill:

========
``KILL``
========

Kills active jobs in the CrateDB cluster.

.. NOTE::

    This statement is only available for all users on clusters running CrateDB
    versions :ref:`4.3 <version_4.3.0>` and above. Prior version 4.3, the
    ``KILL`` statement can only be run by the ``crate`` superuser.

Synopsis
========

::

    KILL (ALL | job_id)


Description
===========

The ``KILL ALL`` statement kills all active jobs within the CrateDB cluster
which are owned by the current user.

The statement ``KILL job_id`` kills the job with a specified ``job_id`` if the
job was started by the current user.

An exception to this is the ``CRATE`` super-user, which can also kill
statements of other users.


Be aware that CrateDB doesn't have transactions. If an operation which modifies
data is killed, it won't rollback. For example if a update operation is killed
it is likely that it updated some documents before being killed. This might
leave the data in an inconsistent state. So take care when using ``KILL``.


Certain fast running operations have a small time frame in which they can be
killed. For example if you delete a single document by ID the document could
be deleted before the ``KILL`` command is processed, but the client might
receive an error that the operation has been killed because the ``KILL``
command processed before the final result is sent to the client.

``KILL ALL`` and ``KILL job_id`` return the number of contexts killed per node.
For example if the only active query was ``select * from t`` and that query is
being executed on 3 nodes, then ``KILL ALL`` will return 3.

Parameters
==========

:job_id:
  The `UUID`_ of the currently active job that needs to be killed given
  as a string literal.


.. _CrateDB Cloud: https://crate.io/products/cratedb-cloud/
.. _UUID: https://en.wikipedia.org/wiki/Universally_unique_identifier
