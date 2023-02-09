===========
Basic setup
===========


Prerequisites
=============

CrateDB is written in Java_ and includes a pre-configured bundled version of
OpenJDK_ in its build. But to develop CrateDB, you still have to install Java_
in order to run the Gradle_ build tool. Some of the tools that are used
to build documentation and run tests require Python_.

To set up a minimal development environment, you will need:

- Java_ (>= 11)
- Python_ (>= 3.7)

Then, clone the repository and navigate into its directory::

    $ git clone https://github.com/crate/crate.git
    $ cd crate


Ignore commits in blame view
----------------------------

If you want to ignore commits (i.e. bulk code formatting) when watching blame
history please issue the following::

    $ git config blame.ignoreRevsFile .git-blame-ignore-revs

If you'd like to just ignore them for a single run of git blame::

    $ git blame --ignore-revs-file .git-blame-ignore-revs


Manual Build
============

This project uses Gradle_ as a build tool. The most convenient way to build
and run CrateDB while you are working on the code is to do so directly from
within your IDE. See the section on `IDE integration`_.

However, you can also use Gradle directly. Gradle can be invoked by executing
``./gradlew``. The first time this command is executed, it is bootstrapped
automatically and there is no need to install Gradle on the system.

To compile the CrateDB sources, run::

    $ ./gradlew compileJava

To run CrateDB::

    $ ./gradlew app:run

The ``run`` command will set CRATE_HOME to ``sandbox/crate``, so use the
configuration files located in that directory.

To build the CrateDB distribution tarball, run::

    $ ./gradlew distTar

The built tarball will be in::

   ./app/build/distributions/

To build and unpack the distribution in one step, run::

    $ ./gradlew installDist

And then start CrateDB like this::

    ./app/build/install/crate/bin/crate

If you want to attach a debugger to the instance, you'd have to start it with::

    CRATE_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" ./app/build/install/crate/bin/crate

Then create an "Attach" configuration in your IDE or editor. If using Visual
Studio Code you can use the pre-defined "Attach to CrateDB" configuration.

To get a full list of all available tasks, run::

    $ ./gradlew tasks

By default, CrateDB uses the pre-configured bundled version of OpenJDK_. It
is also possible to run, compile, and test CrateDB by configuring the target
JDK. For example::

    $ ./gradlew distTar -Dbundled_jdk_os=linux \
                        -Dbundled_jdk_arch=aarch64 \
                        -Dbundled_jdk_vendor=adoptopenjdk \
                        -Dbundled_jdk_version=13.0.2+8

It is possible to compile the code base and run tests with the host system JDK.
To do this, pass the ``-DuseSystemJdk`` system parameter along with a
Gradle task. For example, to run unit tests with the host system JDK, execute
the following command::

    $ ./gradlew test -DuseSystemJdk

All the tasks related to packaging and releasing (``distTar``, ``release``) or
tasks that depend on them (``itest``) will ignore the ``-DuseSystemJdk``
parameter. This means that the compilation and test execution can be
done with the system JDK, but releasing and packaging will still use the
bundled JDK.

The ``-DuseSystemJdk`` is useful for doing releases and cross-platform builds.
For example, you can build a CrateDB package for Windows with the
corresponding platform-bundled JDK on a Linux machine::

    $ ./gradlew distZip \
                -Dbundled_jdk_os=windows \
                -Dbundled_jdk_arch=x64 \
                -Dbundled_jdk_vendor=adoptopenjdk \
                -Dbundled_jdk_version=13.0.2+8 \
                -DuseSystemJdk

Currently, we support ``JDK`` on the following operation systems and
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

Refer to `Tests cheatsheet <tests.rst>`_.


Using an IDE
============

We recommend that you use `IntelliJ IDEA`_ for development.

Do **not** use the Gradle plugin in `IntelliJ IDEA`_. Instead, use the
following Gradle task and then import the ``crate.ipr`` file within Intellij::

    $ ./gradlew idea

This will set up the project using the pre-configured code style, code
inspection, etc. It will also create some run/debug configurations which
allows you to start Crate from the IDE.


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

If you use IntelliJ, there is a Checkstyle plugin available which lets you check
Checkstyle compliance from within the IDE.

The Checkstyle plugin enforces rules defined in `<PROJECT_ROOT>/gradle/checkstyle/checkstyle.xml`.
It checks for things such as unused imports, inconsistent formatting, and potential
bugs.

The plugin is run by Gradle after compiling the main sources. Only main sources
are analyzed and not the test sources.

After setting up code style, it can be checked by running::

    ./gradlew checkstyleMain checkstyleTest checkstyleTestFixtures

Test Coverage
--------------

You can create test coverage reports with `jacoco`_ by running::

    $ ./gradlew jacocoReport

The test coverage report (in HTML) can then be found in the
``build/reports/jacoco/jacocoHtml`` directory.


Forbidden APIs
--------------

To run the `Forbidden APIs`_ tool::

    $ ./gradlew forbiddenApisMain


Work with release branches
==========================

If you want to work on a release branch (e.g.: ``5.2``) you can use
``git worktree`` to avoid checking out the branch in the same directory you've
imported to your IDE as ``master``. This can be handy especially if the release
branch uses older Java, Lucene, etc. or if there are changes in the layout of
the projects, as you won't need to wait for your IDE to switch between the two
branches (workspaces). Inside your crate repo::

    $ git checkout -b 5.2 --track origin/5.2
    $ git checkout master
    $ git worktree add ../crate-5.2 5.2

This way you can have a ``crate-5.2`` (or whatever name you choose) in the same
directory level as your ``crate`` (master) repo. You can work on it
independently of ``master`` and import it as a separate project in your IDE. Of
course you can use the ``git worktree`` for any branch that you want to work on
independently of ``master``. If you want to remove a worktree, simply issue the
following from inside your main ``crate`` repo::

    $ git worktree remove ../crate-5.2



Troubleshooting
===============

If you pulled in some new commits and are getting strange compile errors, try
to reset everything and re-compile::

    $ git clean -xdff
    $ ./gradlew compileTestJava

If you want to get more information on unchecked or deprecation warnings, run
the build with the following command::

    $ ./gradlew -Plint-unchecked -Plint-deprecation compileTestJava


.. _Forbidden APIs: https://github.com/policeman-tools/forbidden-apis
.. _Gradle: http://www.gradle.org/
.. _hosted OpenJDK archives on Crate.io CDN: https://cdn.crate.io/downloads/openjdk/
.. _IDE integration: https://github.com/crate/crate/blob/master/devs/docs/basics.rst#using-an-ide
.. _IntelliJ IDEA: https://www.jetbrains.com/idea/
.. _jacoco: http://www.eclemma.org/jacoco/
.. _Java: http://www.java.com/
.. _logging documentation: https://crate.io/docs/en/stable/configuration.html#logging
.. _OpenJDK: https://openjdk.java.net/projects/jdk/11/
.. _Oracle's Java: http://www.java.com/en/download/help/mac_install.xml
.. _Python: http://www.python.org/
