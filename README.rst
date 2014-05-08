==========
Crate Data
==========

``"Crate Data is a shared nothing, fully searchable, document oriented
cluster datastore."``

Crate Data...

- is a document oriented data store.

- has the ability to scale horizontally extremely well.

- forms a cluster and handles replicas for you.

- offers SQL to query and manage documents.

- lets you specify a schema

  - to define tables

  - and data types.

- offers support to manage BLOBs.


Getting Started
===============

Installation
------------

- Download_ the latest release and unzip the archive.

- Run ``bin/crate`` on unix or ``bin/crate.bat`` on Windows.

- Start the crate shell by invoking ``bin/crash``.

- Start some more servers to form a cluster and relax.


Get Crate Data
==============

Crate Data is not only easy to use, it's also easy to get. Use the one method which
works best for you.

Binary Archive
---------------

- Download_ the latest release and unzip the archive.

Debian Package
--------------

Crate Launchpad_ Page. A package is available for the Ubuntu versions Lucid
Lync, Saucy Salamander and Precise Pangolin.

::

    sudo apt-get install python-software-properties
    sudo add-apt-repository ppa:crate/stable
    sudo apt-get update
    sudo apt-get install crate


RPM: CentOS, Scientific Linux
-----------------------------

All Crate Data packages are signed with GPG. To verify the packages the public
key must be installed to the system. After that, you can install the .rpm
containing the YUM repository definition on your system.

::

    sudo rpm --import https://cdn.crate.io/downloads/yum/RPM-GPG-KEY-crate
    sudo rpm -Uvh https://cdn.crate.io/downloads/yum/6/x86_64/crate-release-6.5-1.noarch.rpm

Then install Crate Data:

``yum install crate``

The Crate Data Testing repository is disabled by default. It contains development builds and is
frequently updated. If you want to enable the Testing repo on your server, set ``enabled=1`` in
``/etc/yum.repos.d/crate.repo``.

.. _Download: https://crate.io/download
.. _Launchpad: https://launchpad.net/~crate

Where to go from here?
======================

That is certainly not all Crate Data offers to you. To take a look at the
documentation visit
`https://crate.io/docs/ <https://crate.io/docs/>`_.

Managing data
-------------

There are several different ways to manage data in Crate.

- The `admin interface <http://localhost:4200/admin>`_

- For for further clients in different languages see `Crate Documentation`_.

.. _Crate Documentation: https://crate.io/docs/

Are you a Developer?
====================

You can build Crate Data on your own with the latest version hosted on GitHub.
To do so, please refer to ``DEVELOP.rst`` and ``CONTRIBUTING.rst`` for further
information.

Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to swing by our IRC channel #crate on Freenode_.
Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _Freenode: http://freenode.net
