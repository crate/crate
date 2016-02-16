.. image:: https://cdn.crate.io/web/2.0/img/crate-logo_330x72.png
    :width: 165px
    :alt: Crate
    :target: https://crate.io

=================================
CRATE: Put data to work. Simply.
=================================


    Crate allows to query and compute data with SQL in real time by providing a
    distributed aggregation engine, native search and super simple scalability.

**https://crate.io**

.. image:: https://travis-ci.org/crate/crate.svg?branch=master
    :target: https://travis-ci.org/crate/crate

.. image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
    :target: https://crate.io/docs/en/latest/

.. image:: https://img.shields.io/badge/container-docker-green.svg
    :target: https://hub.docker.com/_/crate/

Features include
----------------

Familiar SQL syntax:

::

    select * from users;
    insert into users (name) values ('Arthur');

Semi-structured data::

    create table demo (
        name string,
        obj object (dynamic) as (
            age int
        ),
        tags array (string)
    );

::

    insert into demo (name, obj, tags) values
        ('Trillian',
         {age = 39, gender='female'}, // Note that gender wasn't defined in the schema!
         ['mathematician', 'astrophysicist']);

::

    select * from demo where obj['gender'] = 'female';

::

    select * from demo where 'mathematician'= any (tags);


High availability, resiliency, and scalability in a distributed design::

    create table t (string name)
    clustered into 5 shards with (number_of_replicas = 1); // this is actually the default!

::

    select count(*) from sys.nodes;
    select table_name, count(*) from sys.shards group by 1;

Powerful Lucene based full-text search::

    select title from wikipedia where match((title 1.5, text 1.0), 'Hitchhiker')


Getting Started
===============

Get Crate
---------

There are many ways to install Crate. The fastest way to try it out is just one command-line::

    bash -c "$(curl -L try.crate.io)"

Or with docker::

    docker pull crate && docker run -p 4200:4200 crate

Visit our `getting started`_ page to see all available download and install options.


Use Crate
---------

Crate includes an Administration UI that is available under http://localhost:4200/admin/.

It also ships with a CLI called ``crash`` that can be used to run queries in a
shell.

Next steps
----------

Start some more server to form a cluster and take a look at the documentation_
to learn more.

Especially the `crate introduction`_ is a great place to start learning more.


Are you a Developer?
====================

Clone the repository::

    git clone https://github.com/crate/crate.git
    cd crate
    git submodule update --init

And build it with gradle::

    ./gradlew compileJava

Develop in IntelliJ::

    ./gradlew idea

Run tests::

    ./gradlew test

Refer to ``DEVELOP.rst`` and ``CONTRIBUTING.rst`` for further information.

Help & Contact
==============

Do you have any questions? Or suggestions? We would be very happy
to help you. So, feel free to to contact us on Slack_.

.. _Slack: https://crate.io/docs/support/slackin/

Or for further information and official contact please
visit `https://crate.io/ <https://crate.io/>`_.

.. _documentation: https://crate.io/docs/stable/installation.html
.. _getting started: https://crate.io/docs/getting-started
.. _crate introduction: https://crate.io/docs/stable/hello.html
