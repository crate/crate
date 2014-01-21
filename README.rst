========
Crate DB
========

Crate is a shared nothing, fully searchable, document oriented
cluster database.

Getting Started
===============

Installation
------------

* Download_ the latest release and unzip the archive.
* Run ``bin/crate -f`` on unix or ``bin/crate.bat`` on Windows.
* Say hello to Crate ``curl -sS '127.0.0.1:4200/?pretty=1'``
* Start some more servers to form a cluster and relax.

.. _Download: https://download.crate.io/

Get Crate
=========

- Download_ the latest release and unzip the archive.

Debian Package
--------------

Crate Launchpad_ Page

- ``sudo apt-get install python-software-properties``
- ``sudo add-apt-repository ppa:crate/stable``
- ``sudo apt-get update``
- ``sudo apt-get install crate``


RPM: CentOS, Scientific Linux and yum
-------------------------------------
Repository config:

::

    crate.repo # /etc/yum.repos.d/crate.repo
    [crate]
    gpgcheck=0
    baseurl=https://yum.crate.io/release/
    name=Crate

.. _Download: https://download.crate.io/
.. _Launchpad: https://launchpad.net/~crate


Where to go from here?
----------------------

That is certainly not all Crate can do. Take a look at the documentation found
under the ``docs`` directory or visit `https://docs.crate.io/ <https://docs.crate.io/>`_.

Contact
=======

For further information visit `https://crate.io/ <https://crate.io/>`_.
You're very welcome to join our IRC channel #crate on Freenode_.

.. _Freenode: http://freenode.net

