===============
Test cheatsheet
===============

Run tests in a single module using multiple forks::

    $ ./gradlew --parallel -PtestForks=2 :sql:test

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

    $ ./gradlew test --tests '*ClassName.testMethodName'

Extra options::

    $ ./gradlew :server:test -Dtests.seed=8352BE0120F826A9

    $ ./gradlew :server:test -Dtests.iters=20

    $ ./gradlew :server:test -Dtests.nightly=true # defaults to "false"

    $ ./gradlew :server:test -Dtests.verbose=true # log result of all invoked tests

More logging::

    $ ./gradlew -PtestLogging -Dtests.loggers.levels=io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE :server:test

More logging by changing code:

Use ``@TestLogging(["<packageName1>:<logLevel1>", ...])`` on your test class or
test method to enable more detailed logging. For example::

    @TestLogging("io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE")

.. _doctests: https://github.com/crate/crate/blob/master/blackbox/test_docs.py
.. _WSL needs to be installed: https://docs.microsoft.com/en-us/windows/wsl/install-win10
.. _enable Developer Mode: https://docs.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development
