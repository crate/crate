========
Crate DB
========

``"Crate is a shared nothing, fully searchable, document oriented
cluster database."``

Crate...

- is a document oriented data base.

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

- Run ``bin/crate -f`` on unix or ``bin/crate.bat`` on Windows.

- Say hello to Crate ``curl -sS '127.0.0.1:4200/?pretty=1'``

- Start some more servers to form a cluster and relax.

.. _Download: https://crate.io/download/

Get Crate
=========

Crate isn't only easy to use, it's also easy to get. Use the one method which
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

All CRATE packages are signed with GPG. To verify the packages the public
key must be installed to the system. After that, you can install the .rpm
containing the YUM repository definition on your system.

::

sudo rpm --import https://cdn.crate.io/downloads/yum/RPM-GPG-KEY-crate
sudo rpm -Uvh https://cdn.crate.io/downloads/yum/6/x86_64/crate-release-6.5-1.noarch.rpm

Then install Crate:

``yum install crate``

The CRATE Testing repository is disabled by default. It contains development builds and is
frequently updated. If you want to enable the Testing repo on your server, set ``enabled=1`` in
``/etc/yum.repos.d/crate.repo``.

.. _Download: https://crate.io/download/
.. _Launchpad: https://launchpad.net/~crate

Where to go from here?
======================

That is certainly not all Crate offers to you. Take a look at the
documentation found under the ``docs`` directory or visit
`https://crate.io/docs/ <https://crate.io/docs/>`_. If you already installed
Crate you can also use the documentation it was shipped with available via
`http://localhost:4200/_plugin/docs/ <http://localhost:4200/_plugin/docs/>`_.

Managing data
-------------

There are several different ways to manage data in Crate.

- The `admin interface <http://localhost:4200/admin>`_

- `Python client`_ (see: GitHub_, pypi_, get it: ``pip install crate``)

  - The python client also comes with the `crash`. crash is a command-line
    util to perform SQL queries,...

- `Java client`_

.. _Python client: https://crate.io/docs/current/clients.html#crate-python-client
.. _GitHub: https://github.com/crate/crate-python
.. _pypi: https://pypi.python.org/pypi/crate/
.. _Java client: https://crate.io/docs/current/clients.html#crate-java-client

Are you a Developer?
====================

You can build Crate on your own with the latest version hosted on GitHub.
To do so, please refer to ``DEVELOP.rst``.

Contributing
-------------

If you intend to contribute to Crate we need you to agree to our CLA_.
Once that is done, we suggest to continue as follows:

1. Fork this repository of Crate.

2. Create a feature branch (``git checkout -b my_feature``)

3. Add your contributions (``git commit -am "Added feature"``)

4. Commit and publish your feature branch to your own fork
   (``git push -u origin my_markup``).

5. Create a `pull requests <https://help.github.com/articles/using-pull-requests>`_
   specifying the Crate repository as the destination.

6. Lay back and relax while waiting for our response.

.. _CLA: https://crate.io/legal/contribute/

Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to swing by our IRC channel #crate on Freenode_.
Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _Freenode: http://freenode.net
