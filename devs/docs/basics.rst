======================
Develop Guide - Basics
======================

Prerequisites
=============

CrateDB is written in Java_. Some of the (testing) tooling in Python_. So to
develop on CrateDB you'll need:

 - Java_ (>= 11)
 - Python_ (>= 3.6)

On macOS, we recommend using `Oracle's Java`_. If you're using Linux, we
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

Run CrateDB like so::

    $ ./gradlew run

The ``run`` command will set the HOME to ``sandbox/crate`` and so use the
configuration files located there.


Build the CrateDB distribution tarball like so::

    $ ./gradlew distTar

The built tarball will be in::

   ./app/build/distributions/

To build and unpack the distribution in one step, run::

    $ ./gradlew installDist

And then start CrateDB like this::

    ./app/build/install/crate/bin/crate

Build a tarball of the Community Edition like so::

    $ ./gradlew communityEditionDistTar

The tarball can then be found in the ``app/build/distributions`` directory.

To get a full list of all available tasks, run::

    $ ./gradlew tasks


Running Tests
=============

See `Tests cheatsheet <tests.rst>`_.


Using an IDE
============

We recommend that you use `IntelliJ IDEA`_ for development.

Do **not** use the Gradle plugin in `IntelliJ IDEA`_ but instead use the
following Gradle task and then import the ``crate.ipr`` file within Intellij::

    $ ./gradlew idea

Run/Debug Configurations
------------------------

Running ``./gradlew idea`` creates a run/debug configuration called ``Crate``.
This configuration can be used to launch and debug CrateDB from within IntelliJ.

The ``home`` directory will be set to ``<PROJECT_ROOT>/sandbox/crate`` and the
configuration files can be found in the ``<PROJECT_ROOT>/sandbox/crate/config``
directory.

Here, ``<PROJECT_ROOT>`` is the root of your Git repository.

Checkstyle
----------

The Checkstyle plugin enforces rules defined in `<PROJECT_ROOT>/gradle/checkstyle/rules.xml`.
Among others, it indicates unused imports, inconsistent formatting, and potential
bugs. The plugin is run by Gradle after compiling the main sources. It checks the
main sources only but not the test sources.

If you use IntelliJ, there is a Checkstyle plugin available which let's you check
Checkstyle compliance from within the IDE.

Test Coverage
--------------

You can create test coverage reports with `jacoco`_::

    $ ./gradlew jacocoReport

The HTML test coverage report can then be found in the
``build/reports/jacoco/jacocoHtml`` directory.

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
.. _OpenJDK: https://openjdk.java.net/projects/jdk/11/
.. _Oracle's Java: http://www.java.com/en/download/help/mac_install.xml
.. _Python: http://www.python.org/
.. _Gradle: http://www.gradle.org/
.. _logging documentation: https://crate.io/docs/en/stable/configuration.html#logging
.. _IntelliJ IDEA: https://www.jetbrains.com/idea/
.. _jacoco: http://www.eclemma.org/jacoco/
.. _Forbidden APIs: https://github.com/policeman-tools/forbidden-apis
