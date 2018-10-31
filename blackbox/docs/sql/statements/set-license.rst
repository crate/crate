.. highlight:: psql
.. _ref-set-license:

===============
``SET LICENSE``
===============

Sets the license Key.

.. rubric:: Table of Contents

.. contents::
    :local:

Synopsis
========

::

    SET LICENSE license_key

Description
===========

``SET LICENSE`` registers a CrateDB license key to the cluster.
If the license registration is successful, CrateDB will continue to operate
in `Enterprise Edition`_ until the license expiry date.

.. NOTE::

    Should the license registration fail, the cluster will continue to
    operate in the existing mode e.g. ``Enterprise`` as long as
    the current license is still valid (i.e. not expired).

    When the existing licence expires, the cluster will run in a *degraded mode*,
    allowing only read only system information operations and the
    ``SET LICENSE`` statement.

.. SEEALSO::
    - :ref:`enterprise_features`

Parameters
==========

License Key
-----------

The ``license_key`` parameter is the encrypted representation of a CrateDB license.
A CrateDB license is comprised of the following information:

    - ``expirationDateInMs``: The :ref:`data-type-timestamp` on which the license expires.
    - ``issuedTo``: The organisation for which the license is issued.

.. SEEALSO::
    You can retrieve registered license information from the system information tables.
    See :ref:`sys-cluster-license`.


.. _enterprise edition: https://crate.io/enterprise-edition/
.. _community edition: https://crate.io/enterprise-edition/
