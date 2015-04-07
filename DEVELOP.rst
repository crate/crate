===========
DEVELOPMENT
===========

Prerequisites
=============

Crate is written in Java_ 7, so a JDK needs to be installed. On OS X we
recommend using `Oracle's Java`_ and OpenJDK_ on Linux Systems.
We recommend installing Java 7 update 55 or later or Java 8 update 20 or later.
Previous versions of Java 7 can cause data corruption and data loss.

The documentation is built using Sphinx_ which requires Python_ 2.7 to be
installed in addition to Java_. Make sure that there is a python executable
called ``python2.7`` in the global system ``$PATH``. (Most distributions do that
by default if python is installed)

Git checkout and submodules
===========================

Clone the repository and initialize the submodule::

    $ git clone https://github.com/crate/crate.git && cd crate
    $ git submodule update --init

Gradlew - Building Crate and Documentation
==========================================

This project uses Gradle_ as build tool. It can be invoked by executing
``./gradlew``. The first time this command is executed it is bootstrapped
automatically, therefore there is no need to install gradle on the system.

Writing Documentation
=====================

The documentation is maintained under the ``docs`` directory and
written in ReStructuredText_ and processed with Sphinx_.

Normally the documentation is built by `Read the Docs`_ and isn't part of the
Crate distribution. However if you work on the documentation you can run
sphinx directly, which can be done by just running ``bin/sphinx`` in the
``docs`` directory. The output can then be found in the ``out/html`` and
``out/text`` directories.

Before you can run ``bin/sphinx`` you need to setup a development environment
by running `bootstrap.py` inside the ``docs`` directory::

    python bootstrap.py

And afterwards run buildout::

    ./bin/buildout -N

To test that all examples in the documentation execute correctly run::

    ./bin/test

Or if you want to test that a specific file executes correctly run::

    ./bin/test -1vt <filename>

There is also a gradle task called ``itest`` which will execute all of the
above steps.

.. note::

    To run the tests your network connection should be up and running, else
    some of the tests will fail, because crate needs to bind to a network interface
    that is capable of handling ip multicast requests.
    This is not possible on localhost.

Building and running Crate
--------------------------

The most convenient way to run build and run Crate during development is
directly within the IDE. See the section further down for more information.

Regardless, it is possible to compile and run crate using gradle.

To compile run::

    ./gradlew compileJava

And to run::

    ./gradlew runDebug

(And attach a remote debugger on port 5555)

Or install the distribution locally::

    ./gradlew installDist

And start crate::

    ./app/build/install/crate/bin/crate


Other common tasks are:

 - Running tests during development::

    ./gradlew --parallel :sql:test -PtestForks=2 itest

 - Run a single test::

    ./gradlew test -Dtest.single='YourTestClass'

 - Building a tarball (which will be under ``app/build/distributions``)::

    ./gradlew distTar

To get a full list of all available tasks run::

    ./gradlew tasks


Finding your way around in the Crate source code
------------------------------------------------

Getting familiar with a foreign code base is often a daunting task. Especially
if it is a distributed data store.

This little section won't do justice to explain the whole architecture. This
should only give you an idea on where to start reading.

If a SQL statement is sent to Crate the work-flow is roughly as follows:

 - HTTP Request processing
 - Parse request body and create SQLRequest (happens in ``RestSQLAction.java``)
 - Process SQLRequest (see ``doExecute`` in ``TransportBaseSQLAction.java``)
    - Statement is parsed, resulting in an abstract syntax tree 
    - AST is analyzed, basically using meta data like schema information to add
      information.
    - Some statements (mostly DDL) are executed directly
    - Planner creates plan for other statements (select, update, delete...)
    - Executor executes statement


Running Crate in your IDE
=========================

IntelliJ
--------

We recommend IntelliJ to develop Crate. Gradle can be used to generate project
files that can be opened in IntelliJ::

    ./gradlew idea

Run/Debug Configurations
------------------------

It is also possible to run Crate Data nodes directly from within IntelliJ. But
before that can be done a bit of preparation is necessary.

First create the folders for the configuration and data::

    for i in {1..2}; do mkdir -p sandbox/crate_$i/{config,data,plugins}; done

Then create the configuration files for both nodes::

    touch sandbox/crate_1/config/crate.yml
    touch sandbox/crate_2/config/crate.yml

And add the following settings::

    node.name: local1

    http.port: 19201
    transport.tcp.port: 19301
    network.host: localhost

    multicast.enabled: false
    discovery.zen.ping.unicast.hosts:
      - 127.0.0.1:19301
      - 127.0.0.1:19302

.. note::

    In the second files the port number and node name has to be changed.
    19201 to 19202 and 19301 to 19302.

In addition to the `crate.yml` file it is also recommended to create a logging
configuration file for both nodes. To do so create the files
`sandbox/crate_1/config/logging.yml` and `sandbox/crate_2/config/logging.yml`.

A minimal example for the logging configuration looks like this::

    rootLogger: INFO, console
    logger:
      # log action execution errors for easier debugging
      action: DEBUG
      crate.elasticsearch.blob: TRACE

    appender:
      console:
        type: console
        layout:
          type: consolePattern
          conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"
		  
In order for the admin interface to work please check out the crate admin repository::

	https://github.com/crate/crate-admin

After that the Run/Debug Configurations can be added within IntelliJ. Go to the
`Run/Debug Configurations` window and add a new `Application` configuration
(one for each node) with the following settings:

+--------------------------+-----------------------------------------------+
| Main class               | io.crate.bootstrap.CrateF                     |
+--------------------------+-----------------------------------------------+
| VM Options               | -Des.path.home=/full/path/to/sandbox/crate_1/ |
+--------------------------+-----------------------------------------------+
| Use classpath of module: | app                                           |
+--------------------------+-----------------------------------------------+

Test Coverage
--------------

Create test coverage reports with `jacoco`_. The HTML report will be in
``build/reports/jacoco/jacocoHtml``::

    ./gradlew jacocoReport

Findbugs
--------

Running `FindBugs`_ against our code base::

    ./gradlew findBugsMain

the findbugs check will also be executed when running::

    ./gradlew check

Benchmark
=========

A Benchmark for our SQL Interface can be run by calling::

  $ ./gradlew bench

It will output some results to stdout (read between the lines) and finally you will
receive information where more detailed benchmark-results got stored.

Preparing a new Release
=======================

Before creating a new distribution, a new version and tag should be created:

 - Update the CURRENT version in ``io.crate.Version``.

 - Add a note for the new version at the ``CHANGES.txt`` file.

 - Commit e.g. using message 'prepare release x.x.x'.

 - Push to origin

 - Create a tag using the ``create_tag.sh`` script
   (run ``./devtools/create_tag.sh``).

Now everything is ready for building a new distribution, either
manually or let Jenkins_ do the job as usual :-)

Building a release tarball is done via the ``release`` task. This task
actually only runs the ``distTar`` task but additionally checks that
the output of ``git describe --tag`` matches the current version of
Crate::

 $ ./gradlew release

The resulting tarball and zip will reside in the folder
``./app/build/distributions``.

Toubleshooting
==============

If you just pulled some new commits using git and get strange compile errors in
the SQL parser code it is probably necessary to re-generate the parser code as
the grammer changed::

    ./gradlew :sql-parser:compileJava


.. _Jenkins: http://jenkins-ci.org/

.. _Python: http://www.python.org/

.. _Sphinx: http://sphinx-doc.org/

.. _ReStructuredText: http://docutils.sourceforge.net/rst.html

.. _Gradle: http://www.gradle.org/

.. _Java: http://www.java.com/

.. _`Oracle's Java`: http://www.java.com/en/download/help/mac_install.xml

.. _OpenJDK: http://openjdk.java.net/projects/jdk7/

.. _`Read the Docs`: http://readthedocs.org

.. _`jacoco`: http://www.eclemma.org/jacoco/

.. _`FindBugs`: http://findbugs.sourceforge.net/
