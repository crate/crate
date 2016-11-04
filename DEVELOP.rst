===========
DEVELOPMENT
===========

Prerequisites
=============

Crate is written in Java_, so a JDK needs to be installed. On OS X we recommend
using `Oracle's Java`_ and OpenJDK_ on Linux Systems.

It is recommended to use a recent Java 8 version.

Git checkout and submodules
===========================

Clone the repository and initialize all contained submodules::

    $ git clone --recursive https://github.com/crate/crate.git
    $ cd crate

Gradlew - Building Crate and Documentation
==========================================

This project uses Gradle_ as build tool. It can be invoked by executing
``./gradlew``. The first time this command is executed it is bootstrapped
automatically, therefore there is no need to install gradle on the system.

Writing Documentation
=====================

The documentation is maintained under the ``docs`` directory and written as
`reStructuredText`_ (aka ``rst``) and is built using Sphinx_. The line length
shall not exceed 80 characters. Most text editors support automatic line breaks
at a certain line width if you don't want to insert line break manually ;)

Normally the documentation is built by `Read the Docs`_ and isn't part of the
Crate distribution. However if you work on the documentation you can run sphinx
directly, which can be done by just running ``bin/sphinx`` in the ``blackbox``
directory. The output can then be found in the ``docs/out/html`` and
``docs/out/text`` directories.

Sphinx_ requires Python_ 3 to be installed in addition to Java_. Make
sure that there is a python executable called ``python3`` in the global system
``$PATH`` (most distributions do that by default if Python is installed).

Before you can build the documentation, you need to setup a development
environment by running `bootstrap.sh` inside the ``blackbox`` directory::

    $ cd blackbox
    $ ./bootstrap.sh

To build the HTML and text documentation, run::

    $ ./bin/sphinx

If you're editing the docs and want live rebuilds, run::

    $ ./bin/sphinx dev

This command watches the file system for changes and rebuilds the docs, refreshing your
open browser tab, as needed.

To test that all examples in the documentation execute correctly run::

    $ ./bin/test

Or if you want to test that a specific file executes correctly run::

    $ ./bin/test -1vt <filename>

There is also a gradle task called ``itest`` which will execute all of the
above steps.

.. note::

    To run the tests your network connection should be up and running, else
    some of the tests will fail, because crate needs to bind to a network
    interface that is capable of handling ip multicast requests.
    This is not possible on localhost.

Building and running Crate
--------------------------

The most convenient way to run build and run Crate during development is
directly within the IDE. See the section further down for more information.

Regardless, it is possible to compile and run crate using gradle.

To compile Crate sources execute::

    ./gradlew compileJava

To run Crate as a Gradle task, a configuration file for logging (`log4j`_)
needs to be present::

    mkdir -pv config && touch config/log4j2.properties

You can use a :ref:`minimal logging configuration <minimim_logging_config>`.
For more information about logging, please refer to our
`docs <https://crate.io/docs/en/stable/configuration.html#logging>`_.

And to run::

    ./gradlew runDebug

.. note::

   Crate will wait for a remote debugger on port ``5005`` to fully start up!

To install the distribution locally execute::

    ./gradlew installDist

And start Crate::

    ./app/build/install/crate/bin/crate

Other common tasks are::

    ./gradlew --parallel -PtestForks=2 :sql:test

    ./gradlew itest

    ./gradlew -PtestLogging :sql:test

    ./gradlew test -Dtest.single='YourTestClass'

    ./gradlew test --tests '*ClassName.testMethodName'

    ./gradlew :sql:test -Dtests.seed=8352BE0120F826A9

    ./gradlew :sql:test -Dtests.iters=20

Use ``@TestLogging(["<packageName1>:<logLevel1>", ...])`` on your
test class or test method to enable more detailed logging.

Example::

    @TestLogging("io.crate:DEBUG","io.crate.planner.consumer.NestedLoopConsumer:TRACE")

Alternatively you could use this configuration in command line but then it's applied
to all tests that are run with the command::

    ./gradlew -PtestLogging -Dtests.loggers.levels=io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE :sql:test

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

``gradlew idea`` will have created a Run/Debug configuration called ``Crate``.
This configuration can be used to launch and debug Crate from within IntelliJ.

The ``home`` directory will be set to ``<project_root>/sandbox/crate`` and the
configuration files for it can be found in
``<project_root>/sandbox/crate/config``.

Test Coverage
--------------

Create test coverage reports with `jacoco`_. The HTML report will be in
``build/reports/jacoco/jacocoHtml``::

    ./gradlew jacocoReport

Findbugs
--------

Running `FindBugs`_ against our code base::

    ./gradlew findBugsMain

The findbugs check will also be executed when running::

    ./gradlew check

Forbidden APIs
--------------

Run `Forbidden APIs`_::

    ./gradlew forbiddenApisMain

Benchmark
=========

Benchmarks are written using `JMH`_. They can be executed using ``gradle``::

    $ ./gradlew :core:jmh
    $ ./gradlew :sql:jmh

By default this will look for benchmarks inside ``<module>/src/jmh/java`` and
execute them.

If you want to execute specific benchmarks you can use the jar::

    $ ./gradlew :sql:jmhJar
    $ java -jar sql/build/libs/crate-sql-jmh.jar <benchmarkMethodName>


Results will be generated into ``$buildDir/reports/jmh``.

If you're writing new benchmarks take a look at this `JMH introduction`_ and
those `JMH samples`_.

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

Troubleshooting
===============

If you just pulled some new commits using git and get strange compile errors in
the SQL parser code it is probably necessary to re-generate the parser code as
the grammar changed::

    ./gradlew :sql-parser:compileJava

.. _Jenkins: http://jenkins-ci.org/

.. _Python: http://www.python.org/

.. _Sphinx: http://sphinx-doc.org/

.. _reStructuredText: http://docutils.sourceforge.net/rst.html

.. _Gradle: http://www.gradle.org/

.. _Java: http://www.java.com/

.. _`Oracle's Java`: http://www.java.com/en/download/help/mac_install.xml

.. _OpenJDK: http://openjdk.java.net/projects/jdk8/

.. _`Read the Docs`: http://readthedocs.org

.. _`jacoco`: http://www.eclemma.org/jacoco/

.. _`FindBugs`: http://findbugs.sourceforge.net/

.. _`log4j`: http://logging.apache.org/log4j/2.x/

.. _`JMH`: http://openjdk.java.net/projects/code-tools/jmh/

.. _`jmh-gradle-plugin`: https://github.com/melix/jmh-gradle-plugin

.. _`JMH introduction`: http://java-performance.info/jmh/

.. _`JMH samples`: http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/

.. _`Forbidden APIs`: https://github.com/policeman-tools/forbidden-apis
