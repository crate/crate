======================
Develop Guide - Basics
======================

Prerequisites
=============

CrateDB is written in Java_ and includes pre-configured bundled version of
OpenJDK_ in its build. Nevertheless, to develop CrateDB, you'd still have to
install Java_ to run the Gradle_ build tool. Some of the tools that are used
for documentation building, tests, etc. require Python_. To set up you minimal
development environment, you'll need:

 - Java_ (>= 11)
 - Python_ (>= 3.7)

Set Up
======

Clone the repository like so::

    $ git clone https://github.com/crate/crate.git
    $ cd crate

Manual Build
============

This project uses Gradle_ as build tool.

The most convenient way to build and run CrateDB while you are working on the
code is to do so directly from within your IDE. See the section on IDE
integration later in this document. However, if you want to, you can work with
Gradle directly.

Gradle can be invoked by executing ``./gradlew``. The first time this command
is executed it is bootstrapped automatically, therefore there is no need to
install gradle on the system.

To compile the CrateDB sources, run::

    $ ./gradlew compileJava

Run CrateDB like so::

    $ ./gradlew app:run

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

By default, CrateDB uses the pre-configured bundled version of OpenJDK_. It
is also possible to run, compile, and test CrateDB by configuring the target
JDK. For instance::

    $ ./gradlew distTar -Dbundled_jdk_os=linux \
                        -Dbundled_jdk_arch=aarch64 \
                        -Dbundled_jdk_vendor=adoptopenjdk \
                        -Dbundled_jdk_version=13.0.2+8

It is possible to compile the code base and run tests with the host system JDK.
To achieve that, pass the ``-DuseSystemJdk`` system parameter along with a
Gradle task. For example, to run unit tests with the host system JDK, execute
the following command::

    $ ./gradlew test -DuseSystemJdk

All the tasks related to packaging and releasing (``distTar``, ``release``) or
tasks that depend on them (``itest``) will ignore the ``-DuseSystemJdk``
parameter. This means that the compilation and test execution can be
done with the system JDK, but releasing and packaging will still use the
bundled JDK. The ``-DuseSystemJdk`` is useful for doing releases and
cross-platform builds. For instance, you can build a CrateDB package for
Windows with the corresponding to the platform bundled JDK on a Linux
machine::

    $ ./gradlew distZip \
                -Dbundled_jdk_os=windows \
                -Dbundled_jdk_arch=x64 \
                -Dbundled_jdk_vendor=adoptopenjdk \
                -Dbundled_jdk_version=13.0.2+8 \
                -DuseSystemJdk

Currently, we support the following ``JDK`` operation systems and
architectures:

    +---------+---------+---------+-----+
    |         |  linux  | windows | mac |
    +---------+---------+---------+-----+
    |   x64   |    x    |    x    |  x  |
    +---------+---------+---------+-----+
    | aarch64 |    x    |         |     |
    +---------+---------+---------+-----+

The only supported ``JDK`` vendor is ``AdoptOpenJDK``. To check the available
``JDK`` versions, please see `hosted OpenJDK archives on Crate.io CDN`_.

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
.. _hosted OpenJDK archives on Crate.io CDN: https://cdn.crate.io/downloads/openjdk/
