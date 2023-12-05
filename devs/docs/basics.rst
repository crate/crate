===========
Basic setup
===========


Prerequisites
=============

CrateDB is written in Java_ and includes a pre-configured bundled version of
OpenJDK_ in its build. But to develop CrateDB, you still have to install Java_
in order to run the Maven_ build tool. Some of the tools that are used
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

This project uses Maven_ as a build tool. The most convenient way to build
and run CrateDB while you are working on the code is to do so directly from
within your IDE. See the section on `IDE integration`_.

However, you can also use Maven directly. Maven can be invoked by executing
``./mvnw``. The first time this command is executed, it is bootstrapped
automatically and there is no need to install Maven on the system.

To compile the CrateDB sources, run::

    $ ./mvnw compile

To build the CrateDB distribution tarball, run::

    $ ./mvnw package -DskipTests=true

The built tarball will be in::

   ./app/target/

And then start CrateDB like this::

    tar xvzf app/target/crate-*.tar.gz
    ./crate-*/bin/crate

If you want to attach a debugger to the instance, you'd have to enable the debug agent::

    export CRATE_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

Then create an "Attach" configuration in your IDE or editor. If using Visual
Studio Code you can use the pre-defined "Attach to CrateDB" configuration.

Using an IDE
============

To import the project into `IntelliJ IDEA`_, first build the sources outside of your IDE with::

    $ ./mvnw clean install -DskipTests=true

Then import the project into IntelliJ.  You may need to explicitly reload maven projects
to ensure that it correctly picks up generated source files from antlr.

Finally, run a `git clean` to remove pointless IntelliJ file changes to checked in config files.

Running Tests
=============

Refer to `Tests cheatsheet <tests.rst>`_.


Checkstyle
==========

If you use IntelliJ, there is a Checkstyle plugin available which lets you check
Checkstyle compliance from within the IDE.

The Checkstyle plugin enforces rules defined in `<PROJECT_ROOT>/checkstyle.xml`.
It checks for things such as unused imports, inconsistent formatting, and potential
bugs.

To run checkstyle with maven, use::

    ./mvnw compile checkstyle:checkstyle

Test Coverage
=============

You can create test coverage reports with `jacoco`_ by running::

    $ ./mvn test jacoco:report

The test coverage report (in HTML) can then be found in
``<module>/target/site/jacoco/index.html``.


Forbidden APIs
==============

To run the `Forbidden APIs`_ tool::

    $ ./mvnw compile forbiddenapis:check


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
    $ ./mvnw compile


.. _Forbidden APIs: https://github.com/policeman-tools/forbidden-apis
.. _Maven: https://maven.apache.org/
.. _hosted OpenJDK archives on Crate.io CDN: https://cdn.crate.io/downloads/openjdk/
.. _IDE integration: https://github.com/crate/crate/blob/master/devs/docs/basics.rst#using-an-ide
.. _IntelliJ IDEA: https://www.jetbrains.com/idea/
.. _IDE bug: https://youtrack.jetbrains.com/issue/IDEA-267558/Project-generated-sources-not-marked-automatically-instead-it-marks-annotations-folder-as-generated-sources.-So-I-have-to-go-to
.. _jacoco: http://www.eclemma.org/jacoco/
.. _Java: http://www.java.com/
.. _logging documentation: https://crate.io/docs/en/stable/configuration.html#logging
.. _OpenJDK: https://openjdk.java.net/projects/jdk/11/
.. _Oracle's Java: http://www.java.com/en/download/help/mac_install.xml
.. _Python: http://www.python.org/
