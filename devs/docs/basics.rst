======================
Develop Guide - Basics
======================

Prerequisites
=============

CrateDB is written in Java_. Some of the (testing) tooling in Python_. So to
develop on CrateDB you'll need:

 - Java_ (>= 8)
 - Python_ (>= 3.6)

On OS X, we recommend using `Oracle's Java`_. If you're using Linux, we
recommend OpenJDK_.

Set Up
======

Clone the repository like so::

    $ git clone --recursive https://github.com/crate/crate.git
    $ cd crate

Manual Build
============

This project uses Gradle_ as build tool.

The most convenient way to  build and run CrateDB while you are working on the
code is to do so directly from within your IDE. See the section on IDE
integration later in this document. However, if you want to, you can work with
Gradle directly.

Gradle can be invoked by executing ``./gradlew``. The first time this command
is executed it is bootstrapped automatically, therefore there is no need to
install gradle on the system.

To compile the CrateDB sources, run::

    $ ./gradlew compileJava

To run CrateDB as a Gradle task, you need to create configuration file for
logging::

    $ mkdir -pv config && touch config/log4j2.properties

You can use a *minimal logging configuration*. For more information, see the
`logging documentation`_.

Run CrateDB like so::

    $ ./gradlew runDebug

*Note*: If you run CrateDB like this, CrateDB will wait for a remote debugger
on port ``5005`` before fully starting up!

To install the CrateDB locally, run::

    $ ./gradlew installDist

And then start CrateDB like this::

    ./app/build/install/crate/bin/crate

Build a tarball like so::

    $ ./gradlew distTar

The tarball can then be found in the ``app/build/distributions`` directory.

To get a full list of all available tasks, run::

    $ ./gradlew tasks


Running Tests
=============

See `Tests cheatsheet <tests.rst>`_.


Using an IDE
============

We recommend that you use `IntelliJ IDEA`_ for development.

Gradle can be used to generate project files that can be opened in IntelliJ::

    $ ./gradlew idea

Run/Debug Configurations
------------------------

Running ``./gradlew idea`` creates a run/debug configuration called ``Crate``.
This configuration can be used to launch and debug CrateDB from within IntelliJ.

The ``home`` directory will be set to ``<PROJECT_ROOT>/sandbox/crate`` and the
configuration files can be found in the ``<PROJECT_ROOT>/sandbox/crate/config``
directory.

Here, ``<PROJECT_ROOT>`` is the root of your Git repository.

Test Coverage
--------------

You can create test coverage reports with `jacoco`_::

    $ ./gradlew jacocoReport

The HTML test coverage report can then be found in the
``build/reports/jacoco/jacocoHtml`` directory.

SpotBugs (formerly known as FindBugs)
-------------------------------------

You can run `SpotBugs`_ like so::

    $ ./gradlew spotbugsMain

The SpotBugs check will also be executed when running ``./gradlew check``.

Forbidden APIs
--------------

You can run the `Forbidden APIs`_ tool like so::

    $ ./gradlew forbiddenApisMain

Troubleshooting
===============

If you just pulled some new commits and you're getting strange compile errors,
try resetting everything and re-compiling::

    $ git clean -xdff
    $ ./gradlew compileTestJava

If you want to get more information for unchecked or deprecation warnings run
build like so::

    $ ./gradlew -Plint-unchecked compileTestJava
    $ ./gradlew -Plint-deprecation compileTestJava
    $ ./gradlew -Plint-unchecked -Plint-deprecation compileTestJava


.. _Java: http://www.java.com/
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle's Java: http://www.java.com/en/download/help/mac_install.xml
.. _Python: http://www.python.org/
.. _Gradle: http://www.gradle.org/
.. _logging documentation: https://crate.io/docs/en/stable/configuration.html#logging
.. _IntelliJ IDEA: https://www.jetbrains.com/idea/
.. _jacoco: http://www.eclemma.org/jacoco/
.. _SpotBugs: https://spotbugs.github.io
.. _Forbidden APIs: https://github.com/policeman-tools/forbidden-apis
