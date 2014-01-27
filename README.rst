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


Getting Started
===============

Installation
------------

- Download_ the latest release and unzip the archive.

- Run ``bin/crate -f`` on unix or ``bin/crate.bat`` on Windows.

- Say hello to Crate ``curl -sS '127.0.0.1:4200/?pretty=1'``

- Start some more servers to form a cluster and relax.

.. _Download: https://download.crate.io/

Get Crate
=========

Crate isn't only easy to use, it's also easy to get. Use the one method which
works best for you.

Binary Archive
---------------

- Download_ the latest release and unzip the archive.

Debian Package
--------------

Crate Launchpad_ Page. A package is available for Ubuntu versions Lucid Lync,
Saucy Salamander and Precise Pangolin.

::

    sudo apt-get install python-software-properties
    sudo add-apt-repository ppa:crate/stable
    sudo apt-get update
    sudo apt-get install crate


RPM: CentOS, Scientific Linux
-----------------------------

Add the following config for the repository to ``/etc/yum.repos.d/crate.repo``:

::

    [crate]
    gpgcheck=0
    baseurl=https://yum.crate.io/release/
    name=Crate

Or simply copy & paste the following line to your bash to create the above
repository config:

::

    echo -e '[crate]\ngpcheck=0\nbaseurl=https://yum.crate.io/release/\nname=Crate' >> /etc/yum.repos.d/crate.repo

Then install Crate:

``yum install crate``


.. _Download: https://download.crate.io/
.. _Launchpad: https://launchpad.net/~crate

Where to go from here?
======================

That is certainly not all Crate offers to you. Take a look at the
documentation found under the ``docs`` directory or visit
`https://docs.crate.io/ <https://docs.crate.io/>`_. If you already installed
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

.. _Python client: https://docs.crate.io/current/clients.html#crate-python-client
.. _GitHub: https://github.com/crate/crate-python
.. _pypi: https://pypi.python.org/pypi/crate/
.. _Java client: https://docs.crate.io/current/clients.html#crate-java-client

Are you a Developer?
====================

You can build Crate on your own with the latest version hosted on GitHub.
To do so, please refer to ``DEVELOP.rst``.

Contributing
-------------

If you intend to contribute to Crate, we suggest to do so as follows:

1. Fork this repository of Crate.

2. Create a feature branch (``git checkout -b my_feature``)

3. Add your contributions (``git commit -am "Added feature"``)

4. Commit and publish your feature branch to your own fork
   (``git push -u origin my_markup``).

5. Create a `pull requests <https://help.github.com/articles/using-pull-requests>`_
   specifying the Crate repository as the destination.

6. Lay back and relax while waiting for our response.


Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to swing by our IRC channel #crate on Freenode_.
Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _Freenode: http://freenode.net
