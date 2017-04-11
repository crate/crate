=============
Develop Guide
=============

Prerequisites
=============

CrateDB is written in Java_, so a JDK needs to be installed.

On OS X, we recommend using `Oracle's Java`_. If you're using Linux, we
recommend OpenJDK_.

We recommend you use a recent Java 8 version.

Set Up
======

Clone the repository like so::

    $ git clone https://github.com/crate/crate.git
    $ cd crate
    $ git submodule update --init

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

    $ mkdir -pv config && touch config/logging.yml

You can use a *minimal logging configuration*. For more information, see the
the `logging documentation`_.

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

Some other common build tasks::

    $ ./gradlew --parallel -PtestForks=2 :sql:test

    $ ./gradlew itest

    $ ./gradlew -PtestLogging :sql:test

    $ ./gradlew test -Dtest.single='YourTestClass'

    $ ./gradlew test --tests '*ClassName.testMethodName'

    $ ./gradlew :sql:test -Dtests.seed=8352BE0120F826A9

    $ ./gradlew :sql:test -Dtests.iters=20

Use ``@TestLogging(["<packageName1>:<logLevel1>", ...])`` on your test class or
test method to enable more detailed logging. For example::

    @TestLogging("io.crate:DEBUG","io.crate.planner.consumer.NestedLoopConsumer:TRACE")

Alternatively, you can set this configuration via the command line::

    $ ./gradlew -PtestLogging -Dtests.loggers.levels=io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE :sql:test

If you do this, the setting is applied to all tests that are run.

To get a full list of all available tasks, run::

    $ ./gradlew tasks

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

FindBugs
--------

You can run `FindBugs`_ like so::

    $ ./gradlew findBugsMain

The FindBugs check will also be executed when running ``./gradlew check``.

Forbidden APIs
--------------

You can run the `Forbidden APIs`_ tool like so::

    $ ./gradlew forbiddenApisMain

Benchmarks
==========

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

Preparing a New Release
=======================

Before creating a new distribution, a new version and tag should be created:

- Update ``CURRENT`` in ``io.crate.Version``

- Prepare the release notes

  If a technical writer is available, ask them to complete these steps.

  - Create a new file called ``docs/release_notes/X.Y.Z.txt``

  - The file header should look like this::

        .. _version_X.Y.Z:

        =============
        Version X.Y.Z
        =============

        Released on YYYY/MM/DD.

    Be sure to replace ``YYYY/MM/DD`` and both instances of ``X.Y.Z`` with the
    date of this release and the version number of this release, respectively.

  - Discuss with the engineering team:

      - What is the minimum version of CrateDB required to perform an upgrade to
        this version.
      - What is the minimum version of CrateDB required to perform a *rolling
        upgrade* to this version.

    You will need to know these versions before moving on to the next step.

  - Add the upgrade notes

    - If the patch version is zero (e.g. 1.1.0), add this note::

          .. NOTE::

             If you are upgrading a cluster, you must be running CrateDB A.B.C or higher
             before you upgrade to X.Y.Z.

             You cannot perform a :ref:`cluster_upgrade` to this version. Any upgrade to
             this version will require a full cluster restart.

      Here, replace ``A.B.C`` with the minimum upgrade version. And be sure to
      replace ``X.Y.Z`` with the version of this release.

    - Else, if the patch version is not zero (e.g. 1.1.1), add this note::

          .. NOTE::

             If you are upgrading a cluster, you must be running CrateDB A.B.C or higher
             before you upgrade to X.Y.Z.

             If you want to perform a :ref:`cluster_upgrade`, your current CrateDB
             version number must be :ref:`version_X.Y.0`. Any upgrade from a version
             prior to this will require a full cluster restart.

      Here, replace ``A.B.C`` with the minimum upgrade version. And be sure to
      replace ``X.Y.Z`` with the version of this release. The ``X.Y`` in
      ``X.Y.0`` should be replaced to match the version number of this release
      (e.g. ``1.1.0`` for the 1.1.1 release).

  - Add the backup warning::

        .. WARNING::

           Before upgrading, you should `back up your data`_.

        .. _back up your data: https://crate.io/a/backing-up-and-restoring-crate/

  - Copy over each subsection from ``CHANGES.txt`` (i.e.  everything below the
    line that says "Copy everything below this line to the release notes when
    making a release.").

  - Copy edit the changes you just moved over for consistency and style.

  - Run ``bin/sphinx dev`` and verify everything looks okay. Sometimes the
    changes that are collected in ``CHANGES.txt`` use incorrect RST syntax (for
    example, only using single backticks instead of double backticks for string
    literals).

  - Reset ``CHANGES.txt`` so that (only) the ``Breaking Changes``, ``Changes``, and
    ``Fixes`` subsection headers are present, but no changes are listed (i.e.
    this file can then be used to collect unreleased changes again).

- Commit your changes with a message like "prepare release X.Y.Z"

- Push to origin

- Create a tag by running ``./devtools/create_tag.sh``

You can build a release tarball like so::

    $ ./gradlew release

This task runs the ``distTar`` task but also checks that the output of ``git
describe --tag`` matches the current version of CrateDB.

The resulting tarball and zip file will be written to the
``./app/build/distributions`` directory.

We have a Jenkins_ job that will build the tarball for you.

Writing Documentation
=====================

The docs live under the ``blackbox/docs`` directory.

The docs are written with `reStructuredText`_ and built with Sphinx_.

Line length must not exceed 80 characters (except for literals that cannot be
wrapped). Most text editors support automatic line breaks or hard wrapping at a
certain line width if you don't want to do this by hand.

To start working on the docs locally, you will need Python_ 3 in addition to
Java_ (needed for the doctests_). Make sure that ``python3`` is on your
``$PATH``.

Before you can get started, you need to bootstrap the docs::

    $ cd blackbox
    $ ./bootstrap.sh

Once this runs, you can build the docs and start the docs web server like so::

    $ ./bin/sphinx dev

Once the web server running, you can view your local copy of the docs by
visiting http://127.0.0.1:8000 in a web browser.

This command also watches the file system and rebuilds the docs when changes
are detected. Even better, it will automatically refresh the browser tab for
you.

Many of the examples in the documentation are executable and function as
doctests_.

You can run the doctests like so::

    $ ./bin/test

If you want to test the doctests in a specific file, run this::

    $ ./bin/test -1vt <filename>

There is also a Gradle task called ``itest`` which will execute all of the
above steps.

*Note*: Your network connection should be up and running, or some of the tests
will fail.

The docs are automatically built from Git by `Read the Docs`_ and there is
nothing special you need to do to get the live docs to update.

Troubleshooting
===============

If you just pulled some new commits and you're getting strange compile errors
in the SQL parser code, try re-generating the code::

    $ ./gradlew :sql-parser:compileJava

.. _doctests: http://www.sphinx-doc.org/en/stable/ext/doctest.html
.. _FindBugs: http://findbugs.sourceforge.net/
.. _Forbidden APIs: https://github.com/policeman-tools/forbidden-apis
.. _Gradle: http://www.gradle.org/
.. _IntelliJ IDEA: https://www.jetbrains.com/idea/
.. _jacoco: http://www.eclemma.org/jacoco/
.. _Java: http://www.java.com/
.. _Jenkins: http://jenkins-ci.org/
.. _JMH introduction: http://java-performance.info/jmh/
.. _JMH samples: http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/
.. _JMH: http://openjdk.java.net/projects/code-tools/jmh/
.. _logging documentation: https://crate.io/docs/en/stable/configuration.html#logging
.. _OpenJDK: http://openjdk.java.net/projects/jdk8/
.. _Oracle's Java: http://www.java.com/en/download/help/mac_install.xml
.. _Python: http://www.python.org/
.. _Read the Docs: http://readthedocs.org
.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx-doc.org/
