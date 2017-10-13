===============
Test Cheatsheet
===============

Run tests in a single module using multiple forks::

    $ ./gradlew --parallel -PtestForks=2 :sql:test

Run the doc-tests::

    $ ./gradlew itest

Filter tests::

    $ ./gradlew test -Dtest.single='YourTestClass'

    $ ./gradlew test --tests '*ClassName.testMethodName'


Extra options::

    $ ./gradlew :sql:test -Dtests.seed=8352BE0120F826A9

    $ ./gradlew :sql:test -Dtests.iters=20

    $ ./gradlew :sql:test -Dtests.nightly=true # defaults to "false"

    $ ./gradlew :sql:test -Dtests.verbose=true # log result of all invoked tests


More logging::

    $ ./gradlew -PtestLogging -Dtests.loggers.levels=io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE :sql:test


Or with code changes:

Use ``@TestLogging(["<packageName1>:<logLevel1>", ...])`` on your test class or
test method to enable more detailed logging. For example::

    @TestLogging("io.crate:DEBUG,io.crate.planner.consumer.NestedLoopConsumer:TRACE")
