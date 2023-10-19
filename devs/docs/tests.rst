===============
Test cheatsheet
===============

To run all tests::

    $ ./mvnw test


To run tests in a single module, you have to install the modules once::

    $ ./mvnw install -DskipTests=true

And then run::

    $ ./mvnw test -pl <module>

For example::

    $ ./mvnw test -pl server

To run tests for `server` module and build any dependency module beforehand, but
skip running tests on those modules, you can run::

    $ ./mvnw -Pserver test

Run tests using multiple forks::

    $ ./mvnw test -DforkCount=4

This requires ``$JAVA_HOME`` to match the used toolchain version because
maven/surefire won't use the toolchain JDK to run the test forks.

Run all `doctests`_::

    $ ./blackbox/bin/test-docs

For running the documentation tests on Windows, `WSL needs to be installed`_. If
the downloaded Linux distro comes without Java, you can install it by running
the following commands::

    $ sudo add-apt-repository ppa:openjdk-r/ppa
    $ sudo apt-get update
    $ sudo apt install openjdk-11-jdk

Then install python virtual environment by running::

    $ sudo apt-get install python3-venv

To support symlinks, `enable Developer Mode`_ on the "For developers" settings
page and run::

    $ git config --global core.symlinks true

*Note*
  After enabling Developer Mode switching branches will force the recreation of missing symlinks.

After all configuration is done, launch WSL from the project directory
(by running ``wsl``) and run::

    $ ./blackbox/bin/test-docs

Run the doctests for a specific file (e.g., ``filename.rst``):

    $ ITEST_FILE_NAME_FILTER=filename.rst ./blackbox/bin/test-docs

You can also ``export`` ``ITEST_FILE_NAME_FILTER`` to your shell environment
(e.g., export ITEST_FILE_NAME_FILTER=filename.rst``) if you want to set the
value for the remainder of your terminal session.

Filter tests::

    $ ./mvnw '-Dtest=PlannerTest#testSet*' test -pl server

Extra options::

    $ ./mvnw test -pl server -Dtests.seed=8352BE0120F826A9

    $ ./mvnw test -pl server -Dtests.iters=20

    $ ./mvnw test -pl server -Dtests.nightly=true # defaults to "false"

    $ ./mvnw test -pl server -Dtests.verbose=true # log result of all invoked tests

More logging by changing code:

Use ``@TestLogging(["<packageName1>:<logLevel1>", ...])`` on your test class or
test method to enable more detailed logging. For example::

    @TestLogging("io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE")

.. _doctests: https://github.com/crate/crate/blob/master/blackbox/test_docs.py
.. _WSL needs to be installed: https://docs.microsoft.com/en-us/windows/wsl/install-win10
.. _enable Developer Mode: https://docs.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development
