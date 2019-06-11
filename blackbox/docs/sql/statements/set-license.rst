.. highlight:: psql
.. _ref-set-license:

===============
``SET LICENSE``
===============

Sets the license Key.

.. rubric:: Table of contents

.. contents::
    :local:

Synopsis
========

::

    SET LICENSE license_key

Description
===========

``SET LICENSE`` registers a license key that will enable
CrateDB to operate in `Enterprise Edition`_  until the license expiry date.

.. NOTE::

    Should the license registration fail, the cluster will continue to
    operate in the existing mode e.g. `Enterprise Edition`_ as long as
    the current license is still valid (i.e. not expired).

    When the existing licence expires, the cluster will run in a *degraded mode*,
    allowing only read only system information operations and the
    ``SET LICENSE`` statement. Switching to the `Community Edition`_
    is an alternative. For more details on that see :ref:`enterprise-features`.


Parameters
==========

License Key
-----------

The ``license_key`` parameter is the encoded representation of
a CrateDB license that CrateIO will provide.

.. SEEALSO::
    You can view the current license by querying the ``sys.cluster`` table.
    See :ref:`sys-cluster-license`.

.. _enterprise edition: https://crate.io/enterprise-edition/
.. _community edition: https://crate.io/enterprise-edition/
