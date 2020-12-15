===============
Test cheatsheet
===============

Run tests in a single module using multiple forks::

    $ ./gradlew --parallel -PtestForks=2 :sql:test

Run the all of `doctests`_::

    $ ./gradlew itest

Run the doctests for a specific file (e.g., ``filename.rst``):

    $ ITEST_FILE_NAME_FILTER=filename.rst ./gradlew itest

You can also ``export`` ``ITEST_FILE_NAME_FILTER`` to your shell environment
(e.g., export ITEST_FILE_NAME_FILTER=filename.rst``) if you want to set the
value for the remainder of your terminal session.

Filter tests::

    $ ./gradlew test -Dtest.single='YourTestClass'

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
